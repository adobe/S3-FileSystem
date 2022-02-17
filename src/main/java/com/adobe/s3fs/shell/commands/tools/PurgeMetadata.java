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

package com.adobe.s3fs.shell.commands.tools;

import com.adobe.s3fs.filesystemcheck.mapreduce.MetadataStoreScanInputFormat;
import com.adobe.s3fs.filesystemcheck.utils.FileSystemServices;
import com.adobe.s3fs.metastore.api.*;
import com.adobe.s3fs.utils.exceptions.UncheckedException;
import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.restrictions.Required;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static com.adobe.s3fs.shell.CommandGroups.TOOLS;

@Command(
    name = "purgeMeta",
    description = "Purges all metadata entries starting at a particular path",
    groupNames = TOOLS)
public class PurgeMetadata implements Runnable {

  @Required
  @Option(name = "--path")
  String path;

  @Option(name = "--partitions")
  int partitions = 256;

  private final Configuration configuration = new Configuration(true);

  private static final Logger LOG = LoggerFactory.getLogger(PurgeMetadata.class);

  @Override
  public void run() {
    try {
      applyNonOverridableOptions(configuration);
      Job job = Job.getInstance(configuration, "PurgeMeta:" + path);

      job.setNumReduceTasks(0);

      job.setInputFormatClass(MetadataStoreScanInputFormat.class);
      job.setMapperClass(PurgeMetaMapper.class);
      job.setMapOutputKeyClass(NullWritable.class);
      job.setMapOutputValueClass(NullWritable.class);
      job.setOutputFormatClass(NullOutputFormat.class);
      job.getConfiguration().set(MetadataStoreScanInputFormat.ROOT_PATH, path);
      job.getConfiguration().setInt(MetadataStoreScanInputFormat.PARTITION_COUNT_PROP, partitions);

      job.setJarByClass(PurgeMetadata.class);

      job.waitForCompletion(true);
    } catch (Exception ex) {
      LOG.error("Exception thrown while executing the job", ex);
      throw new UncheckedException(ex);
    }
  }

  private static void applyNonOverridableOptions(Configuration configuration) {
    configuration.setInt("mapred.map.max.attempts", 1);
    configuration.setInt("mapreduce.map.maxattempts", 1);
    configuration.setBoolean("mapred.map.tasks.speculative.execution", false);
    configuration.setBoolean("mapreduce.map.speculative", false);
    configuration.setClass(MetastoreClasses.METADATA_OPERATION_LOG_FACTORY_CLASS.getKey(),
        DisabledMetadataOperationLogFactory.class,
        MetadataOperationLogFactory.class);
  }

  private static class PurgeMetaMapper extends Mapper<Void, VersionedObjectHandle, NullWritable, NullWritable> {

    private MetadataStoreExtended metadataStoreExtended;

    private static final Logger LOG = LoggerFactory.getLogger(PurgeMetaMapper.class);

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      super.setup(context);

      this.metadataStoreExtended = FileSystemServices.createMetadataStore(context.getConfiguration(),
          new Path(context.getConfiguration().get(MetadataStoreScanInputFormat.ROOT_PATH)));
    }

    @Override
    protected void map(Void key, VersionedObjectHandle value, Context context) throws IOException, InterruptedException {
      try {
        if (metadataStoreExtended.deleteSingleObject(value, it -> Boolean.TRUE)) {
          context.getCounter(PurgeCounters.GROUP, PurgeCounters.SUCCESSFUL).increment(1L);
        } else {
          context.getCounter(PurgeCounters.GROUP, PurgeCounters.FAILED).increment(1L);
        }
      } catch (Exception e) {
        LOG.error("Error deleting {}", value);
        context.getCounter(PurgeCounters.GROUP, PurgeCounters.FAILED).increment(1L);
      }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
      metadataStoreExtended.close();
      super.cleanup(context);
    }
  }
}
