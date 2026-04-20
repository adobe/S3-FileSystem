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

package com.adobe.s3fs.filesystemcheck.s3;

import com.adobe.s3fs.utils.aws.SimpleRetryPolicies;
import com.adobe.s3fs.utils.aws.s3.StreamingPrefixKeysIterator;
import com.adobe.s3fs.utils.aws.s3.model.S3ObjectLocation;
import com.adobe.s3fs.utils.collections.ListUtils;
import com.adobe.s3fs.utils.mapreduce.SerializableVoid;
import com.adobe.s3fs.utils.mapreduce.TextArrayWritable;
import com.google.common.collect.FluentIterable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.services.s3.S3Client;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static com.adobe.s3fs.filesystemcheck.mapreduce.FileSystemMRJobConfig.*;
import static com.adobe.s3fs.filesystemcheck.mapreduce.FileSystemMRJobConfig.S3_RETRIES;

public class RawS3ScanInputFormat extends InputFormat<SerializableVoid, S3ObjectLocation> {

  public static final String PARTITION_COUNT_PROP = "fs.s3k.raws3.scaninputformat.partition.count";
  public static final String BUCKET_PROP = "fs.s3k.raws3.scaninputformat.bucket";

  public static final int MAX_PARTITION_COUNT = 200 * 1000;

  private static final Logger LOG = LoggerFactory.getLogger(RawS3ScanInputFormat.class);

  @Override
  public List<InputSplit> getSplits(JobContext jobContext) {
    List<InputSplit> splits = new ArrayList<>();
    List<String> prefixAtoms = new SingleDigitS3PrefixPartitioner().prefixes();

    int userPartitions = jobContext.getConfiguration().getInt(PARTITION_COUNT_PROP, 200);

    int depth = computeLengthOfPrefixFromUserPartitions(userPartitions, prefixAtoms.size());
    LOG.info("User provided partitions {} are combined from {} partitions", userPartitions, Math.pow(prefixAtoms.size(), depth));

    List<String> partitionerPrefixes = new PermutationS3PrefixPartitioner(depth, prefixAtoms).prefixes();
    List<List<String>> splitPartitions = ListUtils.randomPartition(partitionerPrefixes, userPartitions);
    for (List<String> prefixes : splitPartitions) {
      splits.add(new RawS3ScanInputSplit(TextArrayWritable.fromList(prefixes)));
    }

    return splits;
  }

  private static int computeLengthOfPrefixFromUserPartitions(int userPartitions, int atomCount) {
    ArrayList<Integer> candidateList = new ArrayList<>();
    int candidate = atomCount;
    while (candidate < MAX_PARTITION_COUNT) {
      candidateList.add(candidate);
      candidate *= atomCount;
    }

    int negatedInsertionPoint = Collections.binarySearch(candidateList, userPartitions);
    if (negatedInsertionPoint >= 0) { // user value is actually in the list
      return negatedInsertionPoint + 1;
    }

    // compute positive index
    int insertionPoint = (negatedInsertionPoint + 1) * -1;
    if (insertionPoint == candidateList.size()) {
      return insertionPoint;
    }
    return insertionPoint + 1;
  }

  @Override
  public RecordReader<SerializableVoid, S3ObjectLocation> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) {
    return new RawS3ScanRecordReader();
  }

  private static class RawS3ScanRecordReader extends RecordReader<SerializableVoid, S3ObjectLocation> {

    private S3Client s3Client;
    private Iterator<S3ObjectLocation> objectIterator;
    private S3ObjectLocation current;

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) {
      Configuration config = taskAttemptContext.getConfiguration();

      final int baseDelay = config.getInt(S3_BACKOFF_BASE_DELAY, 10);
      final int maxDelay = config.getInt(S3_BACKOFF_MAX_DELAY, 30000);
      final int retries = config.getInt(S3_RETRIES, 50);
      final int maxConnections = config.getInt(S3_MAX_CONNECTIONS, 1000);

      RetryPolicy retryPolicy = SimpleRetryPolicies.fullJitter(baseDelay, maxDelay, retries);
      this.s3Client = DefaultS3ClientFactory.INSTANCE.newS3Client(retryPolicy, maxConnections);

      String bucket = taskAttemptContext.getConfiguration().get(BUCKET_PROP);
      List<String> prefixes = ((RawS3ScanInputSplit) inputSplit).prefixes.toStringList();
      this.objectIterator = FluentIterable.from(prefixes)
          .transformAndConcat(prefix -> () -> {
            LOG.info("Iterating over prefix {}", prefix);
            return new StreamingPrefixKeysIterator(s3Client, bucket, prefix);
          })
          .iterator();
    }

    @Override
    public boolean nextKeyValue() {
      if (!objectIterator.hasNext()) {
        return false;
      }
      this.current = objectIterator.next();
      return true;
    }

    @Override
    public SerializableVoid getCurrentKey() {
      return SerializableVoid.INSTANCE;
    }

    @Override
    public S3ObjectLocation getCurrentValue() {
      return current;
    }

    @Override
    public float getProgress() {
      return 0;
    }

    @Override
    public void close() {
      try (S3Client s3ClientCopy = s3Client) {
        // let try-with-resources close it
      }
    }
  }

  private static class RawS3ScanInputSplit extends InputSplit implements Writable {

    private TextArrayWritable prefixes = new TextArrayWritable();

    public RawS3ScanInputSplit() {}

    public RawS3ScanInputSplit(TextArrayWritable prefixes) {
      this.prefixes = prefixes;
    }

    @Override
    public long getLength() {
      return 0;
    }

    @Override
    public String[] getLocations() {
      return new String[0];
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
      prefixes.readFields(dataInput);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
      prefixes.write(dataOutput);
    }
  }
}
