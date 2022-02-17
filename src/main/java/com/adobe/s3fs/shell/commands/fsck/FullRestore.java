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

import com.adobe.s3fs.filesystemcheck.mapreduce.*;
import com.adobe.s3fs.filesystemcheck.mapreduce.data.LogicalObjectWritable;
import com.adobe.s3fs.filesystemcheck.mapreduce.data.VersionedObjectWritable;
import com.adobe.s3fs.filesystemcheck.s3.RawS3ScanInputFormat;
import com.adobe.s3fs.utils.exceptions.UncheckedException;
import com.adobe.s3fs.utils.mapreduce.HadoopConfigUtils;
import com.adobe.s3fs.utils.mapreduce.TextArrayWritable;
import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.restrictions.MutuallyExclusiveWith;
import com.github.rvesse.airline.annotations.restrictions.PathKind;
import com.github.rvesse.airline.annotations.restrictions.Required;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.serializer.JavaSerialization;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.map.MultithreadedMapper;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Optional;
import java.util.Properties;

import static com.adobe.s3fs.filesystemcheck.mapreduce.FileSystemMRJobConfig.*;
import static com.adobe.s3fs.shell.CommandGroups.FSCK;

@Command(name = "fullRestore", description = "Full restore from S3 data", groupNames = FSCK)
public class FullRestore implements Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(FullRestore.class);

  private final Configuration configuration = new Configuration(true);

  @Option(
      name = "--props-files",
      description =
          "Optional hadoop key-value properties to be loaded at runtime before job submission")
  @com.github.rvesse.airline.annotations.restrictions.Path(mustExist = true, kind = PathKind.FILE)
  private String propertiesFile;

  @Option(
      name = "--bucket",
      description =
          "The bucket for which we are running full restore.")
  @Required
  private String bucket;

  @Option(name = "--partitions", description = "Number of partitions tasks to use.")
  private int partitions = 256;

  @Option(name = "--us-s3-mt-mapper", description = "Flag that indicates whether to use multi threading on the maps side " +
      "to process the oplog from S3. Default is false.")
  private boolean useMtMapper = false;

  @Option(name = "--s3-threads-per-partition", description = "Number of threads used to process a single S3 partition")
  private int s3ThreadsPerPartition = 3;

  @Option(
      name = "--output-path",
      description = "Local HDFS output dir where multiple outputs will be placed")
  @Required
  private String outputPath;

  @Option(
      name = "--num-reducers-phase-1",
      description = "Number of reducers to run for 1st phase job"
  )
  private int numReducersPhase1 = 128;

  @Option(
      name = "--num-reducers-phase-2",
      description = "Number of reducers to rung for 1st phase job"
  )
  private int numReducersPhase2 = 128;

  @MutuallyExclusiveWith(tag = "fullRestorePhase")
  @Option(name = "--phase-1", description = "Run phase 1")
  protected boolean enablePhase1 = false;

  @MutuallyExclusiveWith(tag = "fullRestorePhase")
  @Option(name = "--phase-2", description = "Run phase 2")
  protected boolean enablePhase2 = false;

  private static void initOutputPath(Job job, Configuration config, String outputPathDir, String child)
      throws IOException {
    Path outputPath = new Path(outputPathDir, child);
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

    Optional<Properties> properties = loadProperties();
    properties.ifPresent(this::consumeProperties);

    applyNonOverridableOptions(configuration);
    configuration.set(LOGICAL_ROOT_PATH,"ks://" + bucket + "/");

    if (!executeFirstPhaseJob()) {
      throw new UncheckedException("Full restore first phase failed!");
    }

    if (!executeSecondPhaseJob()) {
      throw new UncheckedException("Full restore second phase failed!");
    }
  }

  private Optional<Properties> loadProperties() {
    if (propertiesFile == null || propertiesFile.isEmpty()) {
      return Optional.empty();
    }
    Properties properties = new Properties();
    try (InputStream fileInputStream = Files.newInputStream(Paths.get(propertiesFile))) {
      properties.load(fileInputStream);
    } catch (IOException e) {
      LOG.error("Exception thrown while applying config from properties file!", e);
      throw new UncheckedIOException(e);
    }
    return Optional.of(properties);
  }

  private void consumeProperties(Properties properties) {
    for (String propertyName : properties.stringPropertyNames()) {
      configuration.set(propertyName, properties.getProperty(propertyName));
    }
  }

  private void applyNonOverridableOptions(Configuration configuration) {
    configuration.setBoolean("mapred.map.tasks.speculative.execution", false);
    configuration.setBoolean("mapreduce.map.speculative", false);
    configuration.setBoolean("mapreduce.reduce.speculative", false);
    configuration.setBoolean("mapreduce.task.classpath.user.precedence", true);
    configuration.setInt("mapreduce.map.maxattempts", 1);
  }

  private boolean executeFirstPhaseJob() {
    if (!enablePhase1) {
      LOG.info("Skipping 1st phase");
      return true;
    }

    Preconditions.checkState(!Strings.isNullOrEmpty(bucket));
    try {
      Job job = Job.getInstance(configuration, "FsckFullRestorePhase1:" + bucket);

      job.getConfiguration().set(RawS3ScanInputFormat.BUCKET_PROP, bucket);
      job.getConfiguration().setInt(RawS3ScanInputFormat.PARTITION_COUNT_PROP, partitions);

      job.setNumReduceTasks(numReducersPhase1);

      // Set output path (root dir output path)
      initOutputPath(job, configuration, outputPath, "phase1");

      job.setInputFormatClass(RawS3ScanInputFormat.class);
      if (useMtMapper) {
        HadoopConfigUtils.addSerialization(job.getConfiguration(), JavaSerialization.class);
        MultithreadedMapper.setMapperClass(job, FileSystemCheckS3Mapper.class);
        MultithreadedMapper.setNumberOfThreads(job, s3ThreadsPerPartition);
        job.setMapperClass(MultithreadedMapper.class);
      } else {
        job.setMapperClass(FileSystemCheckS3Mapper.class);
      }

      job.setMapOutputKeyClass(Text.class);
      job.setMapOutputValueClass(LogicalObjectWritable.class);

      job.setReducerClass(FileSystemCheckFullRestoreReducer.class);

      LazyOutputFormat.setOutputFormatClass(job, SequenceFileOutputFormat.class);

      MultipleOutputs.addNamedOutput(
          job, PENDING_OUTPUT_NAME, TextOutputFormat.class, Text.class, Text.class);
      MultipleOutputs.addNamedOutput(
          job, NO_ACTIVE_OUTPUT_NAME, TextOutputFormat.class, Text.class, Text.class);
      MultipleOutputs.addNamedOutput(
          job, S3_OUTPUT_NAME, LazyOutputFormat.class, Text.class, TextArrayWritable.class);
      MultipleOutputs.addNamedOutput(
          job, METASTORE_OUTPUT_NAME, LazyOutputFormat.class, Text.class, VersionedObjectWritable.class);

      job.setJarByClass(FullRestore.class);

      return job.waitForCompletion(true);
    } catch (IOException | InterruptedException | ClassNotFoundException ex) { // NOSONAR
      LOG.error("Exception throw while trying to execute the first phase of the job!", ex);
      throw new UncheckedException(ex);
    }
  }

  private boolean executeSecondPhaseJob() {
    if (!enablePhase2) {
      LOG.info("Skipping phase 2");
      return true;
    }
    try {
      Job job = Job.getInstance(configuration, "FsckFullRestorePhase2:" + bucket);

      job.setNumReduceTasks(numReducersPhase2);

      job.setInputFormatClass(MetadataStoreScanInputFormat.class);
      job.setMapperClass(MetadataStorePartitionMapper.class);
      job.getConfiguration().set(MetadataStoreScanInputFormat.ROOT_PATH, "ks://" + bucket + "/");

      job.setMapOutputKeyClass(Text.class);
      job.setMapOutputValueClass(NullWritable.class);

      job.setReducerClass(FileSystemCheckFullRestoreDirectoryReducer.class);

      initOutputPath(job, job.getConfiguration(), outputPath, "phase2");

      LazyOutputFormat.setOutputFormatClass(job, SequenceFileOutputFormat.class);

      MultipleOutputs.addNamedOutput(
          job, METASTORE_OUTPUT_NAME, LazyOutputFormat.class, Text.class, VersionedObjectWritable.class);

      job.setJarByClass(FullRestore.class);

      return job.waitForCompletion(true);
    } catch (IOException | InterruptedException | ClassNotFoundException ex) { // NOSONAR
      LOG.error("Exception throw while trying to execute the second phase of the job!", ex);
      throw new UncheckedException(ex);
    }
  }
}
