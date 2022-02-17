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

import com.adobe.s3fs.filesystemcheck.s3.S3ClientFactory;
import com.adobe.s3fs.utils.aws.LoggingBackoffStrategy;
import com.adobe.s3fs.utils.mapreduce.TextArrayWritable;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.retry.PredefinedBackoffStrategies;
import com.amazonaws.retry.PredefinedRetryPolicies;
import com.amazonaws.retry.RetryPolicy;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
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

import static com.adobe.s3fs.filesystemcheck.cmdloader.CmdLoaderCounters.S3_OPERATION_FAIL;
import static com.adobe.s3fs.filesystemcheck.cmdloader.CmdLoaderCounters.S3_OPERATION_SUCCESSFUL;
import static com.adobe.s3fs.filesystemcheck.mapreduce.FileSystemMRJobConfig.*;

public class S3FsckCmdMapper extends Mapper<Text, TextArrayWritable, NullWritable, NullWritable> {
  private static final Logger LOG = LoggerFactory.getLogger(S3FsckCmdMapper.class); // NOSONAR
  // Delete object command marker
  private static final Text DELETE_OBJECT = new Text("deleteObject");
  // S3 client factory
  private final S3ClientFactory s3ClientFactory;
  // AmazonS3 client
  private AmazonS3 s3Client;
  // Map of commands marker to actual S3CommandFactory used for building
  private final Map<Text, S3CommandFactory> cmdFactoryTable;

  private boolean dryRun;

  // Empty constructor needed by Hadoop Framework
  public S3FsckCmdMapper() {
    this.s3ClientFactory =
        (retryPolicy, maxConnections) ->
            AmazonS3Client.builder()
                .withClientConfiguration(
                    new ClientConfiguration()
                        .withRetryPolicy(retryPolicy)
                        .withMaxConnections(maxConnections))
                .build();
    this.cmdFactoryTable = new HashMap<>();
  }

  @VisibleForTesting
  public S3FsckCmdMapper(S3ClientFactory s3ClientFactory) {
    this.s3ClientFactory = Preconditions.checkNotNull(s3ClientFactory);
    this.cmdFactoryTable = new HashMap<>();
  }

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);

    Configuration config = context.getConfiguration();

    this.dryRun = config.getBoolean("dryRun", true);

    final int baseDelay = config.getInt(S3_BACKOFF_BASE_DELAY, 5);
    final int maxDelay = config.getInt(S3_BACKOFF_MAX_DELAY, 1000);
    final int retries = config.getInt(S3_RETRIES, 5);
    final int maxConnections = config.getInt(S3_MAX_CONNECTIONS, 1000);

    RetryPolicy.BackoffStrategy backoffStrategy =
        new LoggingBackoffStrategy(
            new PredefinedBackoffStrategies.FullJitterBackoffStrategy(baseDelay, maxDelay));
    RetryPolicy retryPolicy =
        new RetryPolicy(
            PredefinedRetryPolicies.DEFAULT_RETRY_CONDITION, backoffStrategy, retries, true);
    this.s3Client = this.s3ClientFactory.newS3Client(retryPolicy, maxConnections);

    registerCommandFactories();
  }

  @Override
  protected void map(Text key, TextArrayWritable value, Context context)
      throws IOException, InterruptedException {
    S3CommandFactory s3CmdFactory =
        Optional.ofNullable(cmdFactoryTable.get(key))
            .orElseThrow(
                () -> new IllegalStateException("No factory found for key " + key.toString()));
    FsckCommand cmd = s3CmdFactory.newS3Command(s3Client, value);
    try {
      cmd.execute();
      context.getCounter(S3_OPERATION_SUCCESSFUL).increment(1L);
    } catch (Exception e) {
      LOG.warn("Tried to execute command {} but failed", cmd);
      LOG.warn("Command failed with: ", e);
      context.getCounter(S3_OPERATION_FAIL).increment(1L);
    }
  }

  private void registerCommandFactories() {
    cmdFactoryTable.put(DELETE_OBJECT, new S3DeleteCommandFactory(dryRun));
  }

  private interface S3CommandFactory {
    FsckCommand newS3Command(AmazonS3 s3client, TextArrayWritable parameters);
  }

  private static class S3DeleteCommandFactory implements S3CommandFactory {
    private final boolean dryRun;

    S3DeleteCommandFactory(boolean dryRun) {
      this.dryRun = dryRun;
    }

    @Override
    public FsckCommand newS3Command(AmazonS3 s3client, TextArrayWritable parameters) {
      FsckCommand realCmd = S3DeleteCommand.newInstance(s3client, parameters.toStrings());
      if (dryRun) {
        return DryrunFsckCommand.newInstance(realCmd);
      } else {
        return realCmd;
      }
    }
  }
}
