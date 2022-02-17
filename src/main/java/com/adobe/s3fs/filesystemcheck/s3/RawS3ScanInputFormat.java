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
import com.adobe.s3fs.utils.collections.ListUtils;
import com.adobe.s3fs.utils.mapreduce.SerializableVoid;
import com.adobe.s3fs.utils.mapreduce.TextArrayWritable;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.collect.FluentIterable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class RawS3ScanInputFormat extends InputFormat<SerializableVoid, S3ObjectSummary> {

  public static final String PARTITION_COUNT_PROP = "fs.s3k.raws3.scaninputformat.partition.count";
  public static final String BUCKET_PROP = "fs.s3k.raws3.scaninputformat.bucket";

  public static final int MAX_PARTITION_COUNT = 200 * 1000;

  private static final Logger LOG = LoggerFactory.getLogger(RawS3ScanInputFormat.class);

  @Override
  public List<InputSplit> getSplits(JobContext jobContext) throws IOException, InterruptedException {
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
  public RecordReader<SerializableVoid, S3ObjectSummary> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
    return new RawS3ScanRecordReader();
  }

  private static class RawS3ScanRecordReader extends RecordReader<SerializableVoid, S3ObjectSummary> {

    private AmazonS3 amazonS3;
    private Iterator<S3ObjectSummary> objectIterator;
    private S3ObjectSummary current;

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
      String bucket = taskAttemptContext.getConfiguration().get(BUCKET_PROP);
      this.amazonS3 = AmazonS3ClientBuilder.standard()
          .withClientConfiguration(new ClientConfiguration().withRetryPolicy(SimpleRetryPolicies.fullJitter(10, 30000, 50)))
          .build();
      List<String> prefixes = ((RawS3ScanInputSplit) inputSplit).prefixes.toStringList();
      this.objectIterator = FluentIterable.from(prefixes)
          .transformAndConcat(prefix -> () -> {
            LOG.info("Iterating over prefix {}", prefix);
            return new StreamingPrefixKeysIterator(amazonS3, bucket, prefix);
          })
          .iterator();
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
      if (!objectIterator.hasNext()) {
        return false;
      }
      this.current = objectIterator.next();
      return true;
    }

    @Override
    public SerializableVoid getCurrentKey() throws IOException, InterruptedException {
      return SerializableVoid.INSTANCE;
    }

    @Override
    public S3ObjectSummary getCurrentValue() throws IOException, InterruptedException {
      return current;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
      return 0;
    }

    @Override
    public void close() throws IOException {
      amazonS3.shutdown();
    }
  }

  private static class RawS3ScanInputSplit extends InputSplit implements Writable {

    private TextArrayWritable prefixes = new TextArrayWritable();

    public RawS3ScanInputSplit() {}

    public RawS3ScanInputSplit(TextArrayWritable prefixes) {
      this.prefixes = prefixes;
    }

    @Override
    public long getLength() throws IOException, InterruptedException {
      return 0;
    }

    @Override
    public String[] getLocations() throws IOException, InterruptedException {
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
