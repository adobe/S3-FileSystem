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

import com.adobe.s3fs.filesystemcheck.mapreduce.data.VersionedObjectWritable;
import com.adobe.s3fs.filesystemcheck.metastore.MetadataStoreExtendedFactory;
import com.adobe.s3fs.filesystemcheck.utils.FileSystemServices;
import com.adobe.s3fs.metastore.api.MetadataStoreExtended;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static com.adobe.s3fs.filesystemcheck.cmdloader.CmdLoaderCounters.METADATA_OPERATION_FAIL;
import static com.adobe.s3fs.filesystemcheck.cmdloader.CmdLoaderCounters.METADATA_OPERATION_SUCCESSFUL;
import static com.adobe.s3fs.filesystemcheck.mapreduce.FileSystemMRJobConfig.S3_CMD_LOADER_BUCKET;

public class MetastoreFsckCmdMapper
    extends Mapper<Text, VersionedObjectWritable, NullWritable, NullWritable> {
  private static final Logger LOG = LoggerFactory.getLogger(MetastoreFsckCmdMapper.class); // NOSONAR
  // Restore Command marker
  private static final Text RESTORE_OBJECT = new Text("restoreObject");
  // Factory for creating MetadataStoreExtended
  private final MetadataStoreExtendedFactory metaStoreClientFactory;
  //  Map of commands marker to actual MetaCommandFactory used for building
  private final Map<Text, MetaCommandFactory> metaStoreFactoryTable;
  // MetadataStoreExtended object for using the extended operation set
  private MetadataStoreExtended metadataStoreExtended;

  private boolean dryRun;

  // Empty constructor needed by Hadoop Framework
  public MetastoreFsckCmdMapper() {
    this.metaStoreClientFactory = FileSystemServices::createMetadataStore;
    this.metaStoreFactoryTable = new HashMap<>();
  }

  @VisibleForTesting
  public MetastoreFsckCmdMapper(MetadataStoreExtendedFactory factory) {
    this.metaStoreClientFactory = Preconditions.checkNotNull(factory);
    this.metaStoreFactoryTable = new HashMap<>();
  }

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);

    Configuration config = context.getConfiguration();

    this.dryRun = config.getBoolean("dryRun", true);

    String bucket = Preconditions.checkNotNull(config.get(S3_CMD_LOADER_BUCKET));

    this.metadataStoreExtended = this.metaStoreClientFactory.create(config, bucket);

    registerCommandFactories();
  }

  @Override
  protected void map(Text key, VersionedObjectWritable value, Context context)
      throws IOException, InterruptedException {
    MetaCommandFactory metaCmdFactory =
        Optional.ofNullable(metaStoreFactoryTable.get(key))
            .orElseThrow(() -> new IllegalStateException("No factory found for " + key.toString()));
    FsckCommand cmd = metaCmdFactory.newMetaStoreCommand(metadataStoreExtended, value);
    try {
      cmd.execute();
      context.getCounter(METADATA_OPERATION_SUCCESSFUL).increment(1L);
    } catch (Exception e) {
      LOG.warn("Tried to execute command {} but failed", cmd);
      LOG.warn("Command failed with: ", e);
      context.getCounter(METADATA_OPERATION_FAIL).increment(1L);
    }
  }

  private void registerCommandFactories() {
    metaStoreFactoryTable.put(RESTORE_OBJECT, new MetaRestoreCmdFactory(dryRun));
  }

  private interface MetaCommandFactory {
    FsckCommand newMetaStoreCommand(
        MetadataStoreExtended metaStore, VersionedObjectWritable parameters);
  }

  private static class MetaRestoreCmdFactory implements MetaCommandFactory {

    private final boolean dryRun;

    MetaRestoreCmdFactory(boolean dryRun) {
      this.dryRun = dryRun;
    }

    @Override
    public FsckCommand newMetaStoreCommand(
        MetadataStoreExtended metaStore, VersionedObjectWritable parameter) {
      FsckCommand realCmd = MetaRestoreCommand.newInstance(metaStore, parameter);
      if (dryRun) {
        return DryrunFsckCommand.newInstance(realCmd);
      } else {
        return realCmd;
      }
    }
  }
}
