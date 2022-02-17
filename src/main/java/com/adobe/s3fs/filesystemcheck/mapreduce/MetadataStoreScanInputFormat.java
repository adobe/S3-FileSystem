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

import com.adobe.s3fs.filesystemcheck.utils.FileSystemServices;
import com.adobe.s3fs.metastore.api.MetadataStoreExtended;
import com.adobe.s3fs.metastore.api.VersionedObjectHandle;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

@SuppressWarnings("fb-contrib:BED_BOGUS_EXCEPTION_DECLARATION")
public class MetadataStoreScanInputFormat extends InputFormat<Void, VersionedObjectHandle> {

  public static final String ROOT_PATH = "fs.s3k.metastorescan.root.path";
  public static final String PARTITION_COUNT_PROP = "fs.s3k.metastorescan.partition.count";

  @Override
  public List<InputSplit> getSplits(JobContext jobContext)
      throws IOException, InterruptedException {
    int totalPartitions = jobContext.getConfiguration().getInt(PARTITION_COUNT_PROP, 256);

    List<InputSplit> splits = new ArrayList<>(totalPartitions);

    for (int i = 0; i < totalPartitions; i++) {
      splits.add(new MetadataStorePartitionScanInputSplit(i, totalPartitions));
    }
    return splits;
  }

  @Override
  public RecordReader<Void, VersionedObjectHandle> createRecordReader(
      InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
      throws IOException, InterruptedException {
    return new RecordReader<Void, VersionedObjectHandle>() {
      VersionedObjectHandle current;
      Iterator<? extends VersionedObjectHandle> iterator;
      MetadataStoreExtended metadataStoreExtended;

      @Override
      public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
          throws IOException, InterruptedException { // NOSONAR
        MetadataStorePartitionScanInputSplit scanInputSplit =
            (MetadataStorePartitionScanInputSplit) inputSplit;
        int index = scanInputSplit.index;
        int total = scanInputSplit.total;
        Path rootPath = new Path(taskAttemptContext.getConfiguration().get(ROOT_PATH));
        this.metadataStoreExtended = FileSystemServices.createMetadataStore(taskAttemptContext.getConfiguration(), rootPath);
        this.iterator = metadataStoreExtended.scanPartition(rootPath, index, total).iterator();
      }

      @Override
      public boolean nextKeyValue() throws IOException, InterruptedException { // NOSONAR
        if (!iterator.hasNext()) {
          return false;
        }
        this.current = iterator.next();
        return true;
      }

      @Override
      public Void getCurrentKey() throws IOException, InterruptedException { // NOSONAR
        return null;
      }

      @Override
      public VersionedObjectHandle getCurrentValue() throws IOException, InterruptedException { // NOSONAR
        return current;
      }

      @Override
      public float getProgress() throws IOException, InterruptedException { // NOSONAR
        return 0;
      }

      @Override
      public void close() throws IOException { // NOSONAR
        metadataStoreExtended.close();
      }
    };
  }

  private static class MetadataStorePartitionScanInputSplit extends InputSplit implements Writable {

    private int index;
    private int total;

    public MetadataStorePartitionScanInputSplit(int index, int total) {
      this.index = index;
      this.total = total;
    }

    public MetadataStorePartitionScanInputSplit() {}

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
      index = dataInput.readInt();
      total = dataInput.readInt();
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
      dataOutput.writeInt(index);
      dataOutput.writeInt(total);
    }
  }
}
