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

import com.adobe.s3fs.filesystemcheck.cmdloader.FsckCommandType;
import com.adobe.s3fs.filesystemcheck.cmdloader.MetastoreFsckCmdMapper;
import com.adobe.s3fs.filesystemcheck.cmdloader.OplogFsckCmdMapper;
import com.adobe.s3fs.filesystemcheck.cmdloader.S3FsckCmdMapper;
import com.adobe.s3fs.metastore.api.DisabledMetadataOperationLogFactory;
import com.adobe.s3fs.metastore.api.MetadataOperationLogFactory;
import com.adobe.s3fs.metastore.api.MetastoreClasses;
import com.adobe.s3fs.operationlog.S3MetadataOperationLogFactory;
import com.adobe.s3fs.shell.helpers.CommaSeparatedValues;
import com.adobe.s3fs.utils.exceptions.UncheckedException;
import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.restrictions.Required;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineSequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Arrays;
import java.util.List;
import java.util.Map.Entry;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static com.adobe.s3fs.filesystemcheck.cmdloader.FsckCommandType.METASTORE;
import static com.adobe.s3fs.filesystemcheck.cmdloader.FsckCommandType.S3;
import static com.adobe.s3fs.filesystemcheck.cmdloader.FsckCommandType.OPLOG;
import static com.adobe.s3fs.filesystemcheck.mapreduce.FileSystemMRJobConfig.S3_CMD_LOADER_BUCKET;
import static com.adobe.s3fs.shell.CommandGroups.FSCK;

@Command(
    name = "commandLoader",
    description = "Reads some sequence files from HDFS and executes the commands which were read",
    groupNames = FSCK)
public class FsckCommandLoader implements Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(FsckCommandLoader.class);

  private final Configuration config = new Configuration(true);

  @Option(
      name = "--base-dir",
      description =
          "Base Path in local HDFS where are placed directories corresponding to a Command group (e.g.: S3, DynamoDB)")
  @Required
  private String baseDir;

  @Option(name = "--bucket", description = "S3 bucket used by S3 FS")
  @Required
  private String bucket;

  @Option(
      name = "--dry-run",
      description = "Run in dry run mode (don't execute commands in S3 or DynamoDB")
  private boolean dryRun = false;


  @Option(
      name = "--hadoopProperties",
      description = "Provide a comma separated list of key value pairs for Hadoop configuraiton"
  )
  @CommaSeparatedValues
  private List<String> keyValueHadoopConfigs;

  @Override
  public void run() {
    try {
      if (!runInternalMapReduce()) {
        LOG.error("Job failed!");
        throw new UncheckedException("Fsck cmd loader failed!");
      }
    } catch (IOException e) {
      LOG.error("Exception thrown during execution", e);
      throw new UncheckedIOException(e);
    } catch (InterruptedException ie) {
      LOG.error("InterruptedException thrown while running the job!", ie);
      Thread.currentThread().interrupt();
      throw new UncheckedException(ie);
    } catch (ClassNotFoundException cnfe) {
      LOG.error("ClassNotFoundException thrown with", cnfe);
      throw new UncheckedException(cnfe);
    }
  }

  private Predicate<FileStatus> filterAcceptedDirectories() {
    return fileStatus -> {
      String lastComponent = fileStatus.getPath().getName();
      return fileStatus.isDirectory()
          && (METASTORE.getType().equals(lastComponent)
              || S3.getType().equals(lastComponent)
              || OPLOG.getType().equals(lastComponent));
    };
  }

  private boolean runInternalMapReduce()
      throws IOException, ClassNotFoundException, InterruptedException {
    Preconditions.checkState(!Strings.isNullOrEmpty(baseDir));
    Preconditions.checkState(!Strings.isNullOrEmpty(bucket));

    // DryRun flag
    if (dryRun) {
      LOG.info("Running in dryRun mode!");
    }
    this.config.setBoolean("dryRun", dryRun);

    if (keyValueHadoopConfigs != null) {
      LOG.info("KeyValue Hadoop pairs are {} ", keyValueHadoopConfigs);
      for (String keyValue : keyValueHadoopConfigs) {
        int index = keyValue.indexOf('=');
        if (index != -1) {
          String key = keyValue.substring(0, index);
          String value = keyValue.substring(index+1);
          config.set(key, value);
        }
      }
    }

    applyNonOverridableOptions(config);

    config.set(S3_CMD_LOADER_BUCKET, bucket);

    // Path object
    Path commandsDir = new Path(baseDir);
    // Underlying FS
    FileSystem fs = commandsDir.getFileSystem(config);
    if (!fs.exists(commandsDir)) {
      throw new FileNotFoundException(baseDir + " not found. Please check the given path!");
    }

    /*
     * Directories corresponding to each Command group (e.g.: S3 or metastore).
     * baseDir has the following format /parent1/.../parentN/some_path_in_HDFS/
     * and has the following first level children:
     * /parent1/.../parentN/some_path_in_HDFS/s3 & /parent1/.../parentN/some_path_in_HDFS/metastore
     * and each of the first level children have the files to be read
     */
    List<Entry<FsckCommandType, FileStatus>> parentDirsWithType =
        Arrays.stream(fs.listStatus(commandsDir))
            .filter(filterAcceptedDirectories())
            .map(
                fileStatus ->
                    new SimpleImmutableEntry<>(
                        FsckCommandType.fromString(fileStatus.getPath().getName()), fileStatus))
            .collect(Collectors.toList());
    LOG.info("Command group directories {}", parentDirsWithType);

    if (parentDirsWithType.isEmpty()) {
      LOG.info("No input found. Early return");
      return true;
    }

    Job job = Job.getInstance(config, "FsckCmdLoader");

    job.setNumReduceTasks(0); // No reduce phase

    job.setInputFormatClass(SequenceFileInputFormat.class);

    job.setOutputFormatClass(NullOutputFormat.class);

    job.setMapOutputKeyClass(NullWritable.class);
    job.setMapOutputValueClass(NullWritable.class);

    for (Entry<FsckCommandType, FileStatus> entry : parentDirsWithType) {
      Path path = entry.getValue().getPath();
      if (LOG.isDebugEnabled()) {
        LOG.debug("Adding {} to multiple inputs", path);
      }
      addToMultipleInputs(job, entry.getKey(), path);
    }

    job.setJarByClass(FsckCommandLoader.class);
    LOG.info("Starting job...");
    return job.waitForCompletion(true);
  }

  private void addToMultipleInputs(Job job, FsckCommandType type, Path path) {
    switch (type) {
      case S3:
        MultipleInputs.addInputPath(
            job, path, CombineSequenceFileInputFormat.class, S3FsckCmdMapper.class);
        break;
      case METASTORE:
        MultipleInputs.addInputPath(
            job, path, SequenceFileInputFormat.class, MetastoreFsckCmdMapper.class);
        break;
      case OPLOG:
        MultipleInputs.addInputPath(
            job, path, SequenceFileInputFormat.class, OplogFsckCmdMapper.class);
        break;
      default:
        LOG.warn("No registered type mapper found for path {}", path);
        break;
    }
  }

  private void applyNonOverridableOptions(Configuration configuration) {
    configuration.setBoolean("mapred.map.tasks.speculative.execution", false);
    configuration.setBoolean("mapreduce.map.speculative", false);
    configuration.setBoolean("mapreduce.task.classpath.user.precedence", true);
    configuration.setBoolean("mapreduce.job.user.classpath.first", true);
    configuration.setBoolean("mapreduce.user.classpath.first", true);
    configuration.setInt("mapreduce.map.maxattempts", 3);
    // If interacting with meta then disable any oplog updates
    configuration.setClass(MetastoreClasses.METADATA_OPERATION_LOG_FACTORY_CLASS.getKey(),
        DisabledMetadataOperationLogFactory.class,
        MetadataOperationLogFactory.class);
    // Use only for partial restore loading
    configuration.setClass(MetastoreClasses.METADATA_OPERATION_LOG_EXTENDED_FACTORY_CLASS.getKey(),
        S3MetadataOperationLogFactory.class,
        MetadataOperationLogFactory.class);
  }
}
