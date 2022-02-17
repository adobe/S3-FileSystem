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

package com.adobe.s3fs.filesystemcheck.mapreduce;

import com.adobe.s3fs.filesystemcheck.mapreduce.data.LogicalObjectWritable;
import com.adobe.s3fs.filesystemcheck.mapreduce.data.SourceType;
import com.adobe.s3fs.filesystemcheck.mapreduce.multioutputs.MultiOutputsFactory;
import com.adobe.s3fs.filesystemcheck.s3.S3ClientFactory;
import com.adobe.s3fs.utils.aws.s3.S3Helpers;
import com.adobe.s3fs.filesystemcheck.utils.UriMetadataPair;
import com.adobe.s3fs.operationlog.LogicalFileMetadataV2;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.services.s3.AmazonS3Client;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.net.URI;
import java.util.Optional;

import static com.adobe.s3fs.filesystemcheck.mapreduce.FileSystemMRJobConfig.S3_OUTPUT_NAME;
import static com.adobe.s3fs.filesystemcheck.mapreduce.FileSystemMRJobConfig.S3_OUTPUT_SUFFIX;
import static com.adobe.s3fs.filesystemcheck.mapreduce.FsckCounters.*;

import com.adobe.s3fs.metastore.api.OperationLogEntryState;
import com.adobe.s3fs.metastore.api.ObjectOperationType;

public class FileSystemCheckS3Mapper extends AbstractFsckS3Mapper {

  // Empty constructor needed by Hadoop Framework
  public FileSystemCheckS3Mapper() {
    super(
        (retryPolicy, maxConnections) ->
            AmazonS3Client.builder()
                .withClientConfiguration(
                    new ClientConfiguration()
                        .withRetryPolicy(retryPolicy)
                        .withMaxConnections(maxConnections))
                .build(),
        MultipleOutputs::new);
  }

  @VisibleForTesting
  public FileSystemCheckS3Mapper(S3ClientFactory s3ClientFactory, MultiOutputsFactory mosFactory) {
    super(s3ClientFactory, mosFactory);
  }

  @Override
  protected void write(UriMetadataPair uriMetadataPair) throws IOException, InterruptedException {
    URI sourceUri = uriMetadataPair.uri();
    Optional<LogicalFileMetadataV2> optional = uriMetadataPair.metadata();
    if (!optional.isPresent()) {
      return;
    }

    LogicalFileMetadataV2 logicalFileMetadata = optional.get();
    if (!logicalFileMetadata.getLogicalPath().startsWith(rootPath)) {
      return;
    }

    String bucket = S3Helpers.getBucket(sourceUri);
    String prefix = S3Helpers.getPrefix(sourceUri);

    OperationLogEntryState state = logicalFileMetadata.getState();
    ObjectOperationType opType = logicalFileMetadata.getType();

    /*
     * Having this state means that the client previously issued
     * a delete operation on it. Will delete the operation log here
     * and leave the reducer to delete the corresponding physical data
     * (if any) due to the fact the only one value is shuffled for a given key
     */
    if (opType == ObjectOperationType.DELETE && state == OperationLogEntryState.COMMITTED) {
      writeDeleteOpToS3Mos(bucket, prefix);
      context.getCounter(NUM_COMMITTED_OP_LOG_DELETED).increment(1L);
      return;
    }

    /*
     * Having this state means that physical data wasn't committed,
     * hence there wasn't a link between the metadata path and the
     * physical data registered. Will delete the operation log here.
     */
    if (opType == ObjectOperationType.CREATE && state == OperationLogEntryState.COMMITTED) {
      // Sanity check. Physical data is committed starting with UPDATE state
      Preconditions.checkState(!logicalFileMetadata.isPhysicalDataCommitted(), "Wrong state!");
      writeDeleteOpToS3Mos(bucket, prefix);
      context.getCounter(NUM_UNCOMMITTED_OP_LOG_DELETED).increment(1L);
      return;
    }

    long size = logicalFileMetadata.getSize();
    long creationTime = logicalFileMetadata.getCreationTime();
    String physicalPath = logicalFileMetadata.getPhysicalPath();
    String objectHandleId = logicalFileMetadata.getId();

    /*
     * The isPendingState flag is added in order for the reducer
     * to differentiate between the valid case when operation log is committed
     * against the case where operation log is pending.
     * Having a pending operation log means we are at an intermediary state
     * and in the context of full restore (where MetadataStore is missing)
     * we can't know what happened: if the metadata operation or
     * the commit log operation failed. Only committed operation logs allow us
     * to make assumption, while for pending state we need an extra piece of
     * information provided by MetadataStore (which is missing in FullRestore)
     */
    LogicalObjectWritable writable =
        new LogicalObjectWritable.Builder()
            .withSize(size)
            .withCreationTime(creationTime)
            .isDirectory(false)
            .isPendingState(state == OperationLogEntryState.PENDING)
            .withPhysicalPath(physicalPath)
            .withLogicalPath(logicalFileMetadata.getLogicalPath())
            .withVersion(logicalFileMetadata.getVersion())
            .withSourceType(SourceType.FROM_OPLOG)
            .build();
    context.write(new Text(objectHandleId), writable);
    context.getCounter(PARTIAL_RESTORE_S3_OPLOG_SCAN).increment(1L);
  }

  private void writeDeleteOpToS3Mos(String bucket, String prefix)
      throws IOException, InterruptedException {
    Preconditions.checkNotNull(bucket);
    Preconditions.checkNotNull(prefix);
    mosAdapter.write(
        S3_OUTPUT_NAME, DELETE_OBJECT, FsckUtils.mosS3ValueWritable(bucket, prefix), S3_OUTPUT_SUFFIX);
  }
}
