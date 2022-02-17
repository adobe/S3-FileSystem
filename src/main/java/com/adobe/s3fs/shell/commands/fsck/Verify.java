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

package com.adobe.s3fs.shell.commands.fsck;

import com.adobe.s3fs.filesystemcheck.mapreduce.FileSystemCheckVerifyReducer;
import com.adobe.s3fs.filesystemcheck.mapreduce.FileSystemCheckPartialS3Mapper;
import com.adobe.s3fs.filesystemcheck.mapreduce.MetadataStorePartialRestoreMapper;
import com.adobe.s3fs.filesystemcheck.mapreduce.MetadataStoreScanInputFormat;
import com.adobe.s3fs.filesystemcheck.mapreduce.data.LogicalObjectWritable;
import com.adobe.s3fs.filesystemcheck.mapreduce.data.VersionedObjectWritable;
import com.adobe.s3fs.filesystemcheck.s3.RawS3ScanInputFormat;
import com.adobe.s3fs.shell.helpers.CommaSeparatedValues;
import com.adobe.s3fs.utils.aws.s3.S3Helpers;
import com.adobe.s3fs.utils.exceptions.UncheckedException;
import com.adobe.s3fs.utils.mapreduce.HadoopConfigUtils;
import com.adobe.s3fs.utils.mapreduce.TextArrayWritable;
import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.restrictions.Required;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.serializer.JavaSerialization;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.map.MultithreadedMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

import static com.adobe.s3fs.filesystemcheck.mapreduce.FileSystemMRJobConfig.*;
import static com.adobe.s3fs.shell.CommandGroups.FSCK;

@Command(name = "verify", description = "Runs a check so verify that meta store is in sync with oplog and physical data",
    groupNames = FSCK)
public class Verify implements Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(Verify.class);

  private final Configuration configuration = new Configuration(true);

  @Option(
      name = "--bucket",
      description =
          "The bucket for which we are running verify.")
  @Required
  private String bucket;

  @Option(name = "--s3-partitions", description = "Number of s3 partitions tasks to use.")
  private int s3Partitions = 256;

  @Option(name = "--us-s3-mt-mapper", description = "Flag that indicates whether to use multi threading on the maps side " +
      "to process the oplog from S3. Default is false.")
  private boolean useMtMapper = false;

  @Option(name = "--s3-threads-per-partition", description = "Number of threads used to process a single S3 partition")
  private int s3ThreadsPerPartition = 3;

  @Option(name = "--meta-partitions", description = "Number of Meta partitions tasks to use")
  private int metaPartitions = 256;

  @Option(name = "--num-reducers", description = "Number of reducer tasks to use")
  private int numReducers = 128;

  @Option(
      name = "--output-path",
      description = "Local HDFS output dir where multiple outputs will be placed")
  @Required
  private String outputPath;

  @Option(
      name = "--hadoopProperties",
      description = "Provide a comma separated list of key value pairs for Hadoop configuraiton"
  )
  @CommaSeparatedValues
  private List<String> keyValueHadoopConfigs;

  private static void initOutputPath(Job job, Configuration config, String outputPathDir)
      throws IOException {
    Path outputPath = new Path(outputPathDir);
    LOG.info("Job output path:{}", outputPath);
    FileSystem fs = outputPath.getFileSystem(config);
    if (fs.exists(outputPath)) {
      LOG.info("Output path already exists, delete it before running the job");
      fs.delete(outputPath, true);
    }
    FileOutputFormat.setOutputPath(job, outputPath);
  }

  @Override
  public void run() {
    Preconditions.checkState(!Strings.isNullOrEmpty(bucket));
    Preconditions.checkState(!Strings.isNullOrEmpty(outputPath));

    if (keyValueHadoopConfigs != null) {
      LOG.info("KeyValue Hadoop pairs are {} ", keyValueHadoopConfigs);
      for (String keyValue : keyValueHadoopConfigs) {
        int index = keyValue.indexOf('=');
        if (index != -1) {
          String key = keyValue.substring(0, index);
          String value = keyValue.substring(index+1);
          configuration.set(key, value);
        }
      }
    }

    applyNonOverridableOptions(configuration);
    configuration.set(LOGICAL_ROOT_PATH, "ks://" + bucket + "/");

    try {
      Job job = Job.getInstance(configuration, "FsckVerify");

      job.setNumReduceTasks(numReducers);

      // Set output path (root dir output path)
      initOutputPath(job, configuration, outputPath);
      // Set number of partitions for each scanning type (s3 and meta)
      job.getConfiguration().set(MetadataStoreScanInputFormat.ROOT_PATH, "ks://" + bucket + "/");
      job.getConfiguration().setInt(MetadataStoreScanInputFormat.PARTITION_COUNT_PROP, metaPartitions);
      job.getConfiguration().set(RawS3ScanInputFormat.BUCKET_PROP, bucket);
      job.getConfiguration().setInt(RawS3ScanInputFormat.PARTITION_COUNT_PROP, s3Partitions);

      // needed by MultithreadedMapper in order to clone the keys and the values from the delegated RecordReader
      // keys and values are cloned because the standard practice for a RecordReader is to re-use the same objects while
      // deserializing to avoid allocating too much memory

      // Will use MultipleInputs API to inject
      // different mapper types by providing
      // dummy input paths. These input paths are usually used
      // together with the provided input format in order
      // to set/list the first-level children of the input paths
      // and build the InputSplits. In our situation RawS3ScanInputFormat &
      // MetadataStoreScanInputFormat build InputSplits without needing
      // the input paths
      MultipleInputs.addInputPath(
          job,
          new Path("/user/keystone/s3"),
          RawS3ScanInputFormat.class,
          useMtMapper ? MultithreadedMapper.class : FileSystemCheckPartialS3Mapper.class);
      MultipleInputs.addInputPath(
          job,
          new Path("/user/keystone/meta"),
          MetadataStoreScanInputFormat.class,
          MetadataStorePartialRestoreMapper.class);
      // MultithreadedMapper does not work with MultipleInputs (i.e. having multi-threading over multiple mapper types)
      // we can use it here because we only need it for the S3 mapper
      if (useMtMapper) {
        HadoopConfigUtils.addSerialization(job.getConfiguration(), JavaSerialization.class);
        MultithreadedMapper.setMapperClass(job, FileSystemCheckPartialS3Mapper.class);
        MultithreadedMapper.setNumberOfThreads(job, s3ThreadsPerPartition);
      }

      // Mapper's output key & value
      job.setMapOutputKeyClass(Text.class);
      job.setMapOutputValueClass(LogicalObjectWritable.class);

      // Reducer class
      job.setReducerClass(FileSystemCheckVerifyReducer.class);

      LazyOutputFormat.setOutputFormatClass(job, SequenceFileOutputFormat.class);

      MultipleOutputs.addNamedOutput(
          job, S3_OUTPUT_NAME, LazyOutputFormat.class, Text.class, TextArrayWritable.class);
      MultipleOutputs.addNamedOutput(
          job, METASTORE_OUTPUT_NAME, LazyOutputFormat.class, Text.class, VersionedObjectWritable.class);
      MultipleOutputs.addNamedOutput(
          job, OPLOG_OUTPUT_NAME, LazyOutputFormat.class, Text.class, LogicalObjectWritable.class);

      job.setJarByClass(FullRestore.class);

      if (!job.waitForCompletion(true)) {
        throw new UncheckedException("Partial restore failed!");
      }
    } catch (Exception ex) {
      LOG.error("Exception thrown while executing the job", ex);
      throw new UncheckedException(ex);
    }

  }

  private void applyNonOverridableOptions(Configuration configuration) {
    configuration.setBoolean("mapred.map.tasks.speculative.execution", false);
    configuration.setBoolean("mapreduce.map.speculative", false);
    configuration.setBoolean("mapreduce.reduce.speculative", false);
    configuration.setBoolean("mapreduce.task.classpath.user.precedence", true);
    configuration.setBoolean("mapreduce.job.user.classpath.first", true);
    configuration.setInt("mapreduce.map.maxattempts", 4);
  }
}
