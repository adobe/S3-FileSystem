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

import com.adobe.s3fs.common.runtime.FileSystemRuntime;
import com.adobe.s3fs.filesystemcheck.mapreduce.data.LogicalObjectWritable;
import com.adobe.s3fs.filesystemcheck.mapreduce.data.SourceType;
import com.adobe.s3fs.metastore.api.OperationLogEntryState;
import com.adobe.s3fs.metastore.api.ObjectOperationType;
import com.adobe.s3fs.operationlog.LogicalFileMetadataV2;
import com.adobe.s3fs.operationlog.ObjectMetadataSerialization;
import com.adobe.s3fs.operationlog.S3MetadataOperationLog;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectMetadata;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.io.InputStream;
import java.util.UUID;

import static com.adobe.s3fs.operationlog.S3MetadataOperationLog.INFO_SUFFIX;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class OplogSerializationMapperTest {

  private static final String BUCKET = "bucket";

  private Configuration config;

  @Mock
  private Counter mockCounter;

  @Mock private Mapper<Text, LogicalObjectWritable, NullWritable, NullWritable>.Context mockContext;

  @Mock private AmazonS3 mockAmazonS3;

  @Mock private FileSystemRuntime mockRuntime;

  private OplogFsckCmdMapper mapper;

  private S3MetadataOperationLog opLogExtended;

  @Before
  public void setup() throws IOException, InterruptedException {
    MockitoAnnotations.initMocks(this);
    config = new Configuration(true);
    config.setBoolean("dryRun", false);
    config.set("fs.s3k.cmd.loader.s3.bucket", BUCKET);
    opLogExtended = new S3MetadataOperationLog(mockAmazonS3, BUCKET, mockRuntime);
    when(mockContext.getConfiguration()).thenReturn(config);
    when(mockContext.getCounter(any(CmdLoaderCounters.class))).thenReturn(mockCounter);
    mapper = new OplogFsckCmdMapper((config1, bucket) -> opLogExtended);
    mapper.setup(mockContext);
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
    ArgumentCaptor<InputStream> inputStreamCaptor = ArgumentCaptor.forClass(InputStream.class);
    verify(mockAmazonS3, times(1))
        .putObject(
            eq(BUCKET),
            eq(uuid.toString() + INFO_SUFFIX),
            inputStreamCaptor.capture(),
            any(ObjectMetadata.class));

    InputStream result = inputStreamCaptor.getValue();
    LogicalFileMetadataV2 deserializeResult = ObjectMetadataSerialization.deserializeFromV2(result);

    Assert.assertEquals("Size mismatch!", 100L, deserializeResult.getSize());
    Assert.assertEquals("Creation time mismatch!", 1000L, deserializeResult.getCreationTime());
    Assert.assertEquals("Version mismatch!", 2, deserializeResult.getVersion());
    Assert.assertEquals("Uuid mismatch!", uuid.toString(), deserializeResult.getId());
    Assert.assertEquals(
        "Logical path mismatch!", "ks://someKey", deserializeResult.getLogicalPath());
    Assert.assertEquals(
        "Physical path mismatch!", "s3n://bucket/somePath", deserializeResult.getPhysicalPath());
    Assert.assertEquals("Uuid mismatch!", uuid.toString(), deserializeResult.getId());
    Assert.assertEquals(
        "State mismatch!", OperationLogEntryState.COMMITTED, deserializeResult.getState());
    Assert.assertEquals(
        "Type mismatch!", ObjectOperationType.UPDATE, deserializeResult.getType());
    Assert.assertTrue("Type mismatch!", deserializeResult.isPhysicalDataCommitted());
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
    ArgumentCaptor<InputStream> inputStreamCaptor = ArgumentCaptor.forClass(InputStream.class);
    verify(mockAmazonS3, times(1))
        .putObject(
            eq(BUCKET),
            eq(uuid.toString() + INFO_SUFFIX),
            inputStreamCaptor.capture(),
            any(ObjectMetadata.class));

    InputStream result = inputStreamCaptor.getValue();
    LogicalFileMetadataV2 deserializeResult = ObjectMetadataSerialization.deserializeFromV2(result);

    Assert.assertEquals("Size mismatch!", 100L, deserializeResult.getSize());
    Assert.assertEquals("Creation time mismatch!", 1000L, deserializeResult.getCreationTime());
    Assert.assertEquals("Version mismatch!", 1, deserializeResult.getVersion());
    Assert.assertEquals("Uuid mismatch!", uuid.toString(), deserializeResult.getId());
    Assert.assertEquals(
        "Logical path mismatch!", "ks://someKey", deserializeResult.getLogicalPath());
    Assert.assertEquals(
        "Physical path mismatch!", "s3n://bucket/somePath", deserializeResult.getPhysicalPath());
    Assert.assertEquals("Uuid mismatch!", uuid.toString(), deserializeResult.getId());
    Assert.assertEquals(
        "State mismatch!", OperationLogEntryState.COMMITTED, deserializeResult.getState());
    Assert.assertEquals(
        "Type mismatch!", ObjectOperationType.CREATE, deserializeResult.getType());
    Assert.assertTrue("Type mismatch!", deserializeResult.isPhysicalDataCommitted());
  }

}
