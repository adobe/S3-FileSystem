/*
Copyright 2021 Adobe. All rights reserved.
This file is licensed to you under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License. You may obtain a copy
of the License at http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under
the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR REPRESENTATIONS
OF ANY KIND, either express or implied. See the License for the specific language
governing permissions and limitations under the License.
*/

package com.adobe.s3fs.filesystemcheck.cmdloader;

import com.adobe.s3fs.filesystemcheck.mapreduce.data.LogicalObjectWritable;
import com.adobe.s3fs.filesystemcheck.s3.S3OperationLogFactory;
import com.adobe.s3fs.filesystemcheck.utils.FileSystemServices;
import com.adobe.s3fs.metastore.api.MetadataOperationLogExtended;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Optional;

import static com.adobe.s3fs.filesystemcheck.cmdloader.CmdLoaderCounters.OPLOG_OPERATION_FAIL;
import static com.adobe.s3fs.filesystemcheck.cmdloader.CmdLoaderCounters.OPLOG_OPERATION_SUCCESSFUL;
import static com.adobe.s3fs.filesystemcheck.mapreduce.FileSystemMRJobConfig.S3_CMD_LOADER_BUCKET;
import static com.adobe.s3fs.operationlog.S3MetadataOperationLog.INFO_SUFFIX;

public class OplogFsckCmdMapper
    extends Mapper<Text, LogicalObjectWritable, NullWritable, NullWritable> {
  private static final Logger LOG = LoggerFactory.getLogger(OplogFsckCmdMapper.class); // NOSONAR

  // Update OP LOG marker
  private static final String UPDATE_OP_LOG = "updateOpLog";
  // OpLogCommandFactory used for building
  private OpLogCommandFactory cmdFactory;
  // S3 operation log factory
  private final S3OperationLogFactory s3OplogFactory;
  // OperationLogExtended
  private MetadataOperationLogExtended opLogExtended;

  // Empty constructor needed by Hadoop Framework
  public OplogFsckCmdMapper() {
    this.s3OplogFactory = FileSystemServices::createOperationLogExtended;
  }

  @VisibleForTesting
  public OplogFsckCmdMapper(S3OperationLogFactory s3OplogFactory) {
    this.s3OplogFactory = Preconditions.checkNotNull(s3OplogFactory);
  }

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);

    Configuration config = context.getConfiguration();

    boolean dryRun = config.getBoolean("dryRun", true);
    String bucket = Preconditions.checkNotNull(config.get(S3_CMD_LOADER_BUCKET));

    this.cmdFactory = new OpLogUpdateCommandFactory(dryRun);

    this.opLogExtended = s3OplogFactory.create(config, bucket);
  }

  private Optional<ImmutablePair<String, String>> parseMarkerAndPrefix(String markerAndPrefix) {
    int markerIndex = markerAndPrefix.indexOf(':');
    if (markerIndex != -1) {
      String marker = markerAndPrefix.substring(0, markerIndex);
      String prefix = markerAndPrefix.substring(markerIndex + 1);
      return Optional.of(ImmutablePair.of(marker, prefix));
    }
    return Optional.empty();
  }

  private Optional<String> getObjectHandleIdFromPrefix(String prefix) {
    int lastIdx = prefix.lastIndexOf(INFO_SUFFIX);
    if (lastIdx != -1) {
      return Optional.of(prefix.substring(0, lastIdx));
    } else {
      return Optional.empty();
    }
  }

  private Optional<OpLogCommandFactory> getFactoryFromMarker(String marker) {
    if (UPDATE_OP_LOG.equals(marker)) {
      return Optional.of(cmdFactory);
    } else {
      return Optional.empty();
    }
  }

  @Override
  protected void map(Text key, LogicalObjectWritable value, Context context)
      throws IOException, InterruptedException {
    String markerAndPrefix = key.toString();
    Optional<ImmutablePair<String, String>> markerAndPrefixOpt =
        parseMarkerAndPrefix(markerAndPrefix);

    if (markerAndPrefixOpt.isPresent()) {
      ImmutablePair<String, String> markerAndPrefixPair = markerAndPrefixOpt.get();
      OpLogCommandFactory opLogCmdFactory =
          getFactoryFromMarker(markerAndPrefixPair.getLeft())
              .orElseThrow(
                  () -> new IllegalStateException("No factory found for key " + key.toString()));
      String objectHandleId =
          getObjectHandleIdFromPrefix(markerAndPrefixPair.getRight())
              .orElseThrow(() -> new IllegalStateException(".info suffix not present"));
      FsckCommand cmd =
          opLogCmdFactory.newOpLogCommand(opLogExtended, value, objectHandleId);
      try {
        cmd.execute();
        context.getCounter(OPLOG_OPERATION_SUCCESSFUL).increment(1L);
      } catch (Exception e) {
        LOG.warn("Tried to execute command {} but failed", cmd);
        LOG.warn("Command failed with: ", e);
        context.getCounter(OPLOG_OPERATION_FAIL).increment(1L);
      }
    }
  }

  private interface OpLogCommandFactory {
    FsckCommand newOpLogCommand(
        MetadataOperationLogExtended oplogExtended, LogicalObjectWritable parameter, String objectHandleId);
  }

  private static class OpLogUpdateCommandFactory implements OpLogCommandFactory {

    private final boolean dryRun;

    OpLogUpdateCommandFactory(boolean dryRun) {
      this.dryRun = dryRun;
    }

    @Override
    public FsckCommand newOpLogCommand(
        MetadataOperationLogExtended oplogExtended,
        LogicalObjectWritable parameter,
        String objectHandleId) {
      FsckCommand realCmd = OplogUpdateCommand.newInstance(oplogExtended, parameter,  objectHandleId);
      if (dryRun) {
        return DryrunFsckCommand.newInstance(realCmd);
      } else {
        return realCmd;
      }
    }
  }
}
