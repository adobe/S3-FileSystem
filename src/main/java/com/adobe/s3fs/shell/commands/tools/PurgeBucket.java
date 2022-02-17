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

import com.adobe.s3fs.filesystemcheck.s3.RawS3ScanInputFormat;
import com.adobe.s3fs.utils.aws.SimpleRetryPolicies;
import com.adobe.s3fs.utils.exceptions.UncheckedException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.restrictions.Required;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static com.adobe.s3fs.shell.CommandGroups.TOOLS;

@Command(
    name = "purgeBucket",
    description = "Purges all objects entries from an S3 bucket. Assumptions are made about the key layout",
    groupNames = TOOLS)
public class PurgeBucket implements Runnable {

  @Required
  @Option(name = "--bucket")
  String bucket;

  @Option(name = "--partitions", description = "Number of partitions tasks to use.")
  int partitions = 256;

  private final Configuration configuration = new Configuration(true);

  private static final Logger LOG = LoggerFactory.getLogger(PurgeBucket.class);

  @Override
  public void run() {
    try {
      applyNonOverridableOptions(configuration);
      Job job = Job.getInstance(configuration, "PurgeBucket:" + bucket);

      job.setNumReduceTasks(0);

      job.setInputFormatClass(RawS3ScanInputFormat.class);
      job.setMapperClass(PurgeBucketMapper.class);
      job.setMapOutputKeyClass(NullWritable.class);
      job.setMapOutputValueClass(NullWritable.class);
      job.setOutputFormatClass(NullOutputFormat.class);
      job.getConfiguration().set(RawS3ScanInputFormat.BUCKET_PROP, bucket);
      job.getConfiguration().setInt(RawS3ScanInputFormat.PARTITION_COUNT_PROP, partitions);

      job.setJarByClass(PurgeBucket.class);

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
  }

  private static class PurgeBucketMapper extends Mapper<Void, S3ObjectSummary, NullWritable, NullWritable> {

    private AmazonS3 amazonS3;

    private static final Logger LOG = LoggerFactory.getLogger(PurgeBucketMapper.class);

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      super.setup(context);

      int baseDelay = context.getConfiguration().getInt("fs.s3k.purge.bucket.s3.base.delay", 10);
      int maxDelay = context.getConfiguration().getInt("fs.s3k.purge.bucket.s3.max.delay", 30000);
      int maxRetries = context.getConfiguration().getInt("fs.s3k.purge.bucket.s3.max.retries", 50);
      int maxConnections = context.getConfiguration().getInt("fs.s3k.purge.bucket.s3.max.connections", 10);

      this.amazonS3 = AmazonS3ClientBuilder.standard()
          .withClientConfiguration(new ClientConfiguration()
              .withRetryPolicy(SimpleRetryPolicies.fullJitter(baseDelay,  maxDelay, maxRetries))
              .withMaxConnections(maxConnections))
          .build();
    }

    @Override
    protected void map(Void key, S3ObjectSummary value, Context context) throws IOException, InterruptedException {
      try {
        amazonS3.deleteObject(value.getBucketName(), value.getKey());
        context.getCounter(PurgeCounters.GROUP, PurgeCounters.SUCCESSFUL).increment(1L);
      } catch (Exception e) {
        LOG.error("Error deleting {}", value);
        context.getCounter(PurgeCounters.GROUP, PurgeCounters.FAILED).increment(1L);
      }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
      amazonS3.shutdown();
      super.cleanup(context);
    }
  }
}
