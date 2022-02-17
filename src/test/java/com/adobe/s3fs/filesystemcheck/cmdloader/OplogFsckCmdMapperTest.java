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

import com.adobe.s3fs.filesystemcheck.mapreduce.data.LogicalObjectWritable;
import com.adobe.s3fs.filesystemcheck.mapreduce.data.SourceType;
import com.adobe.s3fs.metastore.api.MetadataOperationLogExtended;
import com.adobe.s3fs.metastore.api.ObjectMetadata;
import com.adobe.s3fs.metastore.api.ObjectOperationType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.util.Optional;
import java.util.UUID;

import static com.adobe.s3fs.filesystem.FileSystemImplementation.UNCOMMITED_PATH_MARKER;
import static com.adobe.s3fs.operationlog.S3MetadataOperationLog.INFO_SUFFIX;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

public class OplogFsckCmdMapperTest {

  private static final String BUCKET = "bucket";

  private Configuration config;

  @Mock private MetadataOperationLogExtended mockOplogExtended;

  @Mock private Counter mockCounter;

  @Mock private Mapper<Text, LogicalObjectWritable, NullWritable, NullWritable>.Context mockContext;

  private OplogFsckCmdMapper mapper;

  @Before
  public void setup() throws IOException, InterruptedException {
    MockitoAnnotations.initMocks(this);
    config = new Configuration(true);
    config.setBoolean("dryRun", false);
    config.set("fs.s3k.cmd.loader.s3.bucket", BUCKET);
    when(mockContext.getConfiguration()).thenReturn(config);
    when(mockContext.getCounter(any(CmdLoaderCounters.class))).thenReturn(mockCounter);
    mapper = new OplogFsckCmdMapper((config1, bucket) -> mockOplogExtended);
    mapper.setup(mockContext);
  }

  @Test(expected = IllegalStateException.class)
  public void testUnknownOption() throws IOException, InterruptedException {
    LogicalObjectWritable mock = Mockito.mock(LogicalObjectWritable.class);
    mapper.map(new Text("unknownCommand:prefix"), mock, mockContext);
  }

  @Test
  public void testNoInteraction() throws IOException, InterruptedException {
    mapper.map(new Text("unknownCommand"), Mockito.mock(LogicalObjectWritable.class), mockContext);
    // Verify
    Mockito.verifyZeroInteractions(mockOplogExtended);
  }

  @Test
  public void testUpdateOpLog() throws IOException, InterruptedException {
    UUID uuid = UUID.randomUUID();
    LogicalObjectWritable opLog =
        new LogicalObjectWritable.Builder()
            .withSourceType(SourceType.FROM_OPLOG)
            .withLogicalPath("ks://someKey")
            .withPhysicalPath("s3n://bucket/somePath")
            .withSize(100L)
            .withCreationTime(1000L)
            .withVersion(2)
            .isDirectory(false)
            .isPendingState(false)
            .build();
    mapper.map(
        new Text("updateOpLog:" + String.format("%s%s", uuid, INFO_SUFFIX)), opLog, mockContext);
    // Verify
    ObjectMetadata objectMetadata = convertToObjectMetadata(opLog);
    Mockito.verify(mockOplogExtended, times(1))
        .amendObject(eq(objectMetadata), eq(uuid), eq(2), eq(ObjectOperationType.UPDATE));
  }

  @Test
  public void testCreateOpLog() throws IOException, InterruptedException {
    UUID uuid = UUID.randomUUID();
    LogicalObjectWritable opLog =
        new LogicalObjectWritable.Builder()
            .withSourceType(SourceType.FROM_OPLOG)
            .withLogicalPath("ks://someKey")
            .withPhysicalPath("s3n://bucket/somePath")
            .withSize(100L)
            .withCreationTime(1000L)
            .withVersion(1)
            .isDirectory(false)
            .isPendingState(false)
            .build();
    mapper.map(
        new Text("updateOpLog:" + String.format("%s%s", uuid, INFO_SUFFIX)), opLog, mockContext);
    // Verify
    ObjectMetadata objectMetadata = convertToObjectMetadata(opLog);
    Mockito.verify(mockOplogExtended, times(1))
        .amendObject(eq(objectMetadata), eq(uuid), eq(1), eq(ObjectOperationType.CREATE));
  }

  @Test(expected = IllegalStateException.class)
  public void testInfoSuffixMissing() throws IOException, InterruptedException {
    UUID uuid = UUID.randomUUID();
    mapper.map(
        new Text("updateOpLog:" + String.format("%s%s", uuid, "")),
        Mockito.mock(LogicalObjectWritable.class),
        mockContext);
    Mockito.verifyZeroInteractions(mockOplogExtended);
  }

  private ObjectMetadata convertToObjectMetadata(LogicalObjectWritable oplog) {
    return ObjectMetadata.builder()
            .key(new Path(oplog.getLogicalPath()))
            .isDirectory(oplog.isDirectory())
            .size(oplog.getSize())
            .creationTime(oplog.getCreationTime())
            .physicalPath(
                oplog.isDirectory() ? Optional.empty() : Optional.of(oplog.getPhysicalPath()))
            .physicalDataCommitted(!oplog.getPhysicalPath().equals(UNCOMMITED_PATH_MARKER))
            .build();
  }
}
