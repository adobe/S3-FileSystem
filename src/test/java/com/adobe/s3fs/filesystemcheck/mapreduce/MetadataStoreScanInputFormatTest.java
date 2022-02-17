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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;

public class MetadataStoreScanInputFormatTest {

  private Job job;
  private int maxPartitions = 100;

  @Before
  public void setup() throws IOException {
    Configuration config = new Configuration();
    config.setInt(MetadataStoreScanInputFormat.PARTITION_COUNT_PROP, maxPartitions);
    job = Job.getInstance(config, "UnitTestInputFormat");
    job.setInputFormatClass(MetadataStoreScanInputFormat.class);
  }

  @Test
  public void testMetadataStoreScanInputFormat()
      throws IOException, ClassNotFoundException, InterruptedException {
    Configuration config = job.getConfiguration();
    InputFormat<?, ?> inputFormat = ReflectionUtils.newInstance(job.getInputFormatClass(), config);
    List<InputSplit> splits = inputFormat.getSplits(job);

    ByteArrayOutputStream out = new ByteArrayOutputStream(1000);
    try (DataOutputStream dos = new DataOutputStream(out)) {
      for (InputSplit split : splits) {
        Writable writable = (Writable) split;
        writable.write(dos);
      }
    }

    ByteArrayInputStream is = new ByteArrayInputStream(out.toByteArray());
    try (DataInputStream dis = new DataInputStream(is)) {
      for (int i = 0; i < maxPartitions; i++) {
        int readPartitionIdx = dis.readInt();
        int readTotalPartitions = dis.readInt();
        Assert.assertEquals(i, readPartitionIdx);
        Assert.assertEquals(maxPartitions, readTotalPartitions);
      }
    }
  }
}
