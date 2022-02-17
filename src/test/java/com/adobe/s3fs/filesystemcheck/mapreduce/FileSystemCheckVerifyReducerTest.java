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

import com.adobe.s3fs.filesystemcheck.mapreduce.data.LogicalObjectWritable;
import com.adobe.s3fs.filesystemcheck.mapreduce.data.SourceType;
import com.adobe.s3fs.filesystemcheck.mapreduce.multioutputs.MultiOutputsFactory;
import com.adobe.s3fs.utils.aws.s3.S3Helpers;
import com.adobe.s3fs.utils.mapreduce.TextArrayWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static com.adobe.s3fs.filesystem.FileSystemImplementation.UNCOMMITED_PATH_MARKER;
import static com.adobe.s3fs.filesystemcheck.mapreduce.FileSystemMRJobConfig.*;
import static com.adobe.s3fs.filesystemcheck.mapreduce.FsckTestUtils.*;
import static com.adobe.s3fs.operationlog.S3MetadataOperationLog.INFO_SUFFIX;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

public class FileSystemCheckVerifyReducerTest {

  private static final Text DELETE_OBJECT = new Text("deleteObject");
  private static final Text DELETE_SINGLE_OBJECT = new Text("deleteSingleObject");
  @Mock private MultipleOutputs mockMultipleOutputs;

  @Mock private MultiOutputsFactory mockMultiOutputsFactory;

  @Mock private Counter mockCounter;

  @Mock private Reducer<Text, LogicalObjectWritable, Text, Text>.Context mockContext;

  private Configuration configuration;

  private FileSystemCheckVerifyReducer reducer;

  @Before
  public void setup() throws IOException, InterruptedException {
    MockitoAnnotations.initMocks(this);

    configuration = new Configuration(true);
    configuration.set(LOGICAL_ROOT_PATH, "ks://" + BUCKET);

    when(mockMultiOutputsFactory.newMultipleOutputs(eq(mockContext)))
        .thenReturn(mockMultipleOutputs);

    when(mockContext.getConfiguration()).thenReturn(configuration);
    when(mockContext.getCounter(any(Enum.class))).thenReturn(mockCounter);

    reducer = new FileSystemCheckVerifyReducer(mockMultiOutputsFactory);

    reducer.setup(mockContext);
  }

  @Test
  public void testSingleOpLog() throws IOException, InterruptedException {
    // Prepare
    UUID uuid = UUID.randomUUID();
    String logicalPath = logicalEntry(BUCKET, "dir1/dir2/dir3/file");
    String phyPath = physicalEntry(BUCKET, uuid);
    LogicalObjectWritable opLog = opLogWritable(logicalPath, phyPath, false, 2);
    // Act
    reducer.reduce(toText(uuid), Collections.singletonList(opLog), mockContext);
    // Verify
    ArgumentCaptor<TextArrayWritable> arrayCaptor =
        ArgumentCaptor.forClass(TextArrayWritable.class);
    verify(mockMultipleOutputs, times(1))
        .write(eq(S3_OUTPUT_NAME), eq(DELETE_OBJECT), arrayCaptor.capture(), eq(S3_OUTPUT_SUFFIX));

    Assert.assertEquals(new Text(BUCKET), arrayCaptor.getValue().get()[0]);
    Assert.assertEquals(
        new Text(String.format("%s%s", uuid, INFO_SUFFIX)), arrayCaptor.getValue().get()[1]);
  }

  @Test
  public void testSingleMetaObject() throws IOException, InterruptedException {
    // Prepare
    UUID uuid = UUID.randomUUID();
    String logicalPath = logicalEntry(BUCKET, "dir1/file");
    String phyPath = physicalEntry(BUCKET, uuid);
    LogicalObjectWritable meta = metaWritable(phyPath, logicalPath, 2);
    // Act
    reducer.reduce(toText(uuid), Collections.singletonList(meta), mockContext);
    // Verify
    verifyZeroInteractions(mockMultipleOutputs);
    verify(mockContext, times(1)).getCounter(eq(FsckCounters.PARTIAL_RESTORE_INVALID_STATE_META_ONLY));
  }

  @Test
  public void testSinglePhysicalDataOnly() throws IOException, InterruptedException {
    // Prepare
    UUID uuid = UUID.randomUUID();
    String phyPath = physicalEntry(BUCKET, uuid);
    LogicalObjectWritable s3 = s3Writable(phyPath);
    // Act
    reducer.reduce(toText(uuid), Collections.singletonList(s3), mockContext);
    // Verify
    ArgumentCaptor<TextArrayWritable> captor = ArgumentCaptor.forClass(TextArrayWritable.class);
    verify(mockMultipleOutputs, times(1))
        .write(eq(S3_OUTPUT_NAME), eq(DELETE_OBJECT), captor.capture(), eq(S3_OUTPUT_SUFFIX));
    TextArrayWritable value = captor.getValue();
    Assert.assertEquals(new Text(BUCKET), value.get()[0]);
    Assert.assertEquals(new Text(S3Helpers.getPrefix(phyPath)), value.get()[1]);
  }

  @Test
  public void testMultiplePhysicalDataOnly() throws IOException, InterruptedException {
    // Prepare
    UUID uuid = UUID.randomUUID();
    // First path
    String phyPath = physicalEntry(BUCKET, uuid);
    LogicalObjectWritable s3Path = s3Writable(phyPath);
    // Second path
    String phyPath2 = physicalEntry(BUCKET, uuid);
    LogicalObjectWritable s3Path2 = s3Writable(phyPath2);
    // Third path
    String phyPath3 = physicalEntry(BUCKET, uuid);
    LogicalObjectWritable s3Path3 = s3Writable(phyPath3);
    // Act
    reducer.reduce(toText(uuid), Arrays.asList(s3Path, s3Path2, s3Path3), mockContext);
    // Verify
    ArgumentCaptor<TextArrayWritable> captor = ArgumentCaptor.forClass(TextArrayWritable.class);
    verify(mockMultipleOutputs, times(3))
        .write(eq(S3_OUTPUT_NAME), eq(DELETE_OBJECT), captor.capture(), eq(S3_OUTPUT_SUFFIX));
    List<TextArrayWritable> allValues = captor.getAllValues();
    Assert.assertEquals(new Text(S3Helpers.getPrefix(phyPath)), allValues.get(0).get()[1]);
    Assert.assertEquals(new Text(S3Helpers.getPrefix(phyPath2)), allValues.get(1).get()[1]);
    Assert.assertEquals(new Text(S3Helpers.getPrefix(phyPath3)), allValues.get(2).get()[1]);
  }

  @Test
  public void testOpLogWithPhysicalData() throws IOException, InterruptedException {
    // Prepare
    UUID uuid = UUID.randomUUID();
    String logicalPath = logicalEntry(BUCKET, "dir1/file");
    String phyPath = physicalEntry(BUCKET, uuid);
    LogicalObjectWritable opLog = opLogWritable(logicalPath, phyPath, false, 2);
    LogicalObjectWritable s3Path = s3Writable(phyPath);
    // Act
    reducer.reduce(toText(uuid), Arrays.asList(opLog, s3Path), mockContext);
    // Verify
    ArgumentCaptor<TextArrayWritable> captor = ArgumentCaptor.forClass(TextArrayWritable.class);
    verify(mockMultipleOutputs, times(2))
        .write(eq(S3_OUTPUT_NAME), eq(DELETE_OBJECT), captor.capture(), eq(S3_OUTPUT_SUFFIX));
    List<TextArrayWritable> value = captor.getAllValues();
    TextArrayWritable opLogValue = value.get(0);
    Assert.assertEquals(new Text(String.format("%s%s", uuid, INFO_SUFFIX)), opLogValue.get()[1]);
    TextArrayWritable s3Value = value.get(1);
    Assert.assertEquals(new Text(S3Helpers.getPrefix(phyPath)), s3Value.get()[1]);
  }

  @Test
  public void testOpLogAndMetaWithVersionGraterThanOne() throws IOException, InterruptedException {
    // Prepare
    UUID uuid = UUID.randomUUID();
    String logicalPath = logicalEntry(BUCKET, "dir1/file");
    String phyPath = physicalEntry(BUCKET, uuid);
    LogicalObjectWritable opLog = opLogWritable(logicalPath, phyPath, false, 2);
    LogicalObjectWritable meta = metaWritable(phyPath, logicalPath, 2);
    // Act
    reducer.reduce(toText(uuid), Arrays.asList(opLog, meta), mockContext);
    // Verify
    verifyZeroInteractions(mockMultipleOutputs);
    verify(mockContext, times(1)).getCounter(eq(FsckCounters.PARTIAL_RESTORE_INVALID_STATE_OPLOG_AND_META_WITH_NO_PHY_DATA));
  }

  @Test
  public void testOpLogDifferentFromMetaWithVersionOne() throws IOException, InterruptedException {
    // Prepare
    UUID uuid = UUID.randomUUID();
    String logicalPath = logicalEntry(BUCKET, "dir1/file");
    String phyPath = physicalEntry(BUCKET, uuid);
    LogicalObjectWritable opLog = opLogWritable(logicalPath, phyPath, true, 2);
    LogicalObjectWritable meta = metaWritable(UNCOMMITED_PATH_MARKER, logicalPath, 1);
    // Act
    reducer.reduce(toText(uuid), Arrays.asList(opLog, meta), mockContext);
    // Verify
    Text expectedKey = new Text("updateOpLog:" + String.format("%s%s", uuid, INFO_SUFFIX));
    ArgumentCaptor<LogicalObjectWritable> newOpLogCaptor =
        ArgumentCaptor.forClass(LogicalObjectWritable.class);
    verify(mockMultipleOutputs, times(1))
        .write(
            eq(OPLOG_OUTPUT_NAME),
            eq(expectedKey),
            newOpLogCaptor.capture(),
            eq(OPLOG_OUTPUT_SUFFIX));
    LogicalObjectWritable newOpLog = newOpLogCaptor.getValue();
    Assert.assertEquals("New version incorrect!", 1, newOpLog.getVersion());
    Assert.assertFalse("New pending state incorrect!", newOpLog.isPendingState());
    Assert.assertEquals("Logical path is not the same!", logicalPath, newOpLog.getLogicalPath());
    Assert.assertEquals(
        "Phy path is not the same!", UNCOMMITED_PATH_MARKER, newOpLog.getPhysicalPath());
  }

  @Test
  public void testPendingOpLogSameVersionWithMetaWithVersionOne()
      throws IOException, InterruptedException {
    // Prepare
    UUID uuid = UUID.randomUUID();
    String logicalPath = logicalEntry(BUCKET, "dir1/file");
    LogicalObjectWritable opLog = opLogWritable(logicalPath, UNCOMMITED_PATH_MARKER, true, 1);
    LogicalObjectWritable meta = metaWritable(UNCOMMITED_PATH_MARKER, logicalPath, 1);
    // Act
    reducer.reduce(toText(uuid), Arrays.asList(opLog, meta), mockContext);
    // Verify
    Text expectedKey = new Text("updateOpLog:" + String.format("%s%s", uuid, INFO_SUFFIX));
    ArgumentCaptor<LogicalObjectWritable> newOpLogCaptor =
        ArgumentCaptor.forClass(LogicalObjectWritable.class);
    verify(mockMultipleOutputs, times(1))
        .write(
            eq(OPLOG_OUTPUT_NAME),
            eq(expectedKey),
            newOpLogCaptor.capture(),
            eq(OPLOG_OUTPUT_SUFFIX));
    LogicalObjectWritable newOpLog = newOpLogCaptor.getValue();
    Assert.assertEquals("New version incorrect!", 1, newOpLog.getVersion());
    Assert.assertFalse("New pending state incorrect!", newOpLog.isPendingState());
    Assert.assertEquals("Logical path is not the same!", logicalPath, newOpLog.getLogicalPath());
    Assert.assertEquals(
        "Phy path is not the same!", UNCOMMITED_PATH_MARKER, newOpLog.getPhysicalPath());
  }

  @Test
  public void testOpLogSameVersionWithMetaWithVersionOne()
      throws IOException, InterruptedException {
    // Prepare
    UUID uuid = UUID.randomUUID();
    String logicalPath = logicalEntry(BUCKET, "dir1/file");
    LogicalObjectWritable opLog = opLogWritable(logicalPath, UNCOMMITED_PATH_MARKER, false, 1);
    LogicalObjectWritable meta = metaWritable(UNCOMMITED_PATH_MARKER, logicalPath, 1);
    // Act
    reducer.reduce(toText(uuid), Arrays.asList(opLog, meta), mockContext);
    // Verify
    verifyZeroInteractions(mockMultipleOutputs);
  }

  @Test
  public void testMetaWithActivePhyData() throws IOException, InterruptedException {
    // Prepare
    UUID uuid = UUID.randomUUID();
    String logicalPath = logicalEntry(BUCKET, "dir1/file");
    String phyPath = physicalEntry(BUCKET, uuid);
    LogicalObjectWritable meta = metaWritable(phyPath, logicalPath, 2);
    LogicalObjectWritable s3 = s3Writable(phyPath);
    // Act
    reducer.reduce(toText(uuid), Arrays.asList(meta, s3), mockContext);
    // Verify
    verifyZeroInteractions(mockMultipleOutputs);
    verify(mockContext, times(1))
        .getCounter(eq(FsckCounters.PARTIAL_RESTORE_INVALID_STATE_META_AND_PHY_DATA));
  }

  @Test
  public void testMetaWithInactivePhyData() throws IOException, InterruptedException {
    // Prepare
    UUID uuid = UUID.randomUUID();
    String logicalPath = logicalEntry(BUCKET, "dir1/file");
    String phyPath = physicalEntry(BUCKET, uuid);
    LogicalObjectWritable meta = metaWritable(UNCOMMITED_PATH_MARKER, logicalPath, 1);
    LogicalObjectWritable s3 = s3Writable(phyPath);
    // Act
    reducer.reduce(toText(uuid), Arrays.asList(meta, s3), mockContext);
    // Verify
    verifyZeroInteractions(mockMultipleOutputs);
    verify(mockContext, times(1))
        .getCounter(eq(FsckCounters.PARTIAL_RESTORE_INVALID_STATE_META_AND_PHY_DATA));
  }

  @Test
  public void testAllElementsAvailableWithOpLogDriftedAfterRename()
      throws IOException, InterruptedException {
    // Prepare
    UUID uuid = UUID.randomUUID();
    String logicalPath = logicalEntry(BUCKET, "dir1/file");
    String nextLogicalPath = logicalEntry(BUCKET, "dir1/next_file");
    String phyPath = physicalEntry(BUCKET, uuid);
    // Meta Object
    LogicalObjectWritable meta = metaWritable(phyPath, logicalPath, 2);
    // Phy path
    LogicalObjectWritable s3 = s3Writable(phyPath);
    // Op log simulates a failed rename which only manage to log to the operation log
    LogicalObjectWritable opLog = opLogWritable(nextLogicalPath, phyPath, true, 3);
    // Act
    reducer.reduce(toText(uuid), Arrays.asList(meta, s3, opLog), mockContext);
    // Verify
    ArgumentCaptor<LogicalObjectWritable> opLogCaptor =
        ArgumentCaptor.forClass(LogicalObjectWritable.class);
    Text outputKey = new Text("updateOpLog:" + String.format("%s%s", uuid, INFO_SUFFIX));
    verify(mockMultipleOutputs, times(1))
        .write(
            eq(OPLOG_OUTPUT_NAME), eq(outputKey), opLogCaptor.capture(), eq(OPLOG_OUTPUT_SUFFIX));
    LogicalObjectWritable newOpLog = opLogCaptor.getValue();
    Assert.assertEquals("Version mismatch!", 2, newOpLog.getVersion());
    Assert.assertFalse(newOpLog.isPendingState());
    Assert.assertEquals("Logical path mismatch!", logicalPath, newOpLog.getLogicalPath());
    Assert.assertEquals("Physical path mismatch!", phyPath, newOpLog.getPhysicalPath());
  }

  @Test
  public void testAllElementsAvailableWithOpLogDriftedAfterOverwrite()
      throws IOException, InterruptedException {
    // Prepare
    UUID uuid = UUID.randomUUID();
    String logicalPath = logicalEntry(BUCKET, "dir1/file");
    String overwriteLogicalPath = logicalEntry(BUCKET, "dir1/overwrite_file");
    String phyPath = physicalEntry(BUCKET, uuid);
    String overwritePhyPath = physicalEntry(BUCKET, uuid);
    // Meta Object
    LogicalObjectWritable meta = metaWritable(phyPath, logicalPath, 2);
    // Phy path
    LogicalObjectWritable s3 = s3Writable(phyPath);
    // Overwrite phy path
    LogicalObjectWritable overwriteS3 = s3Writable(overwritePhyPath);
    // Op log simulates a failed create(overwrite=true) which
    // only managed to log to the operation log
    LogicalObjectWritable opLog = opLogWritable(overwriteLogicalPath, overwritePhyPath, true, 3);
    // Act
    reducer.reduce(toText(uuid), Arrays.asList(meta, s3, overwriteS3, opLog), mockContext);
    // Verify
    ArgumentCaptor<LogicalObjectWritable> opLogCaptor =
        ArgumentCaptor.forClass(LogicalObjectWritable.class);
    Text outputKey = new Text("updateOpLog:" + String.format("%s%s", uuid, INFO_SUFFIX));
    verify(mockMultipleOutputs, times(1))
        .write(
            eq(OPLOG_OUTPUT_NAME), eq(outputKey), opLogCaptor.capture(), eq(OPLOG_OUTPUT_SUFFIX));
    LogicalObjectWritable updateOpLog = opLogCaptor.getValue();
    Assert.assertEquals("SourceType mismatch!", SourceType.FROM_OPLOG, updateOpLog.getSourceType());
    Assert.assertEquals("Version mismatch!", 2, updateOpLog.getVersion());
    Assert.assertFalse(updateOpLog.isPendingState());
    Assert.assertEquals("Physical path mismatch!", phyPath, updateOpLog.getPhysicalPath());
    Assert.assertEquals("Logical path mismatch!", logicalPath, updateOpLog.getLogicalPath());


    ArgumentCaptor<TextArrayWritable> captor =
        ArgumentCaptor.forClass(TextArrayWritable.class);
    verify(mockMultipleOutputs, times(1))
        .write(eq(S3_OUTPUT_NAME), eq(DELETE_OBJECT), captor.capture(), eq(S3_OUTPUT_SUFFIX));
    TextArrayWritable value = captor.getValue();
    Assert.assertEquals(new Text(BUCKET), value.get()[0]);
    Assert.assertEquals(new Text(S3Helpers.getPrefix(overwritePhyPath)), value.get()[1]);
  }

  @Test
  public void testAllElementsPresentButNoActivePhyData() throws IOException, InterruptedException {
    // Prepare
    UUID uuid = UUID.randomUUID();
    String logicalPath = logicalEntry(BUCKET, "dir1/file");
    String otherLogicalPath = logicalEntry(BUCKET, "dir1/other_file");
    String phyPath = physicalEntry(BUCKET, uuid);
    String otherPhyPath = physicalEntry(BUCKET, uuid);
    // Meta Object
    LogicalObjectWritable meta = metaWritable(phyPath, logicalPath, 2);
    // Overwrite phy path
    LogicalObjectWritable otherS3 = s3Writable(otherPhyPath);
    // Op log
    LogicalObjectWritable opLog = opLogWritable(otherLogicalPath, otherPhyPath, true, 3);
    // Act
    reducer.reduce(toText(uuid), Arrays.asList(meta, otherS3, opLog), mockContext);
    // Verify
    ArgumentCaptor<LogicalObjectWritable> newOpLogCaptor =
        ArgumentCaptor.forClass(LogicalObjectWritable.class);
    Text expectedKey = new Text("updateOpLog:" + String.format("%s%s", uuid, INFO_SUFFIX));
    verify(mockMultipleOutputs, times(1))
        .write(
            eq(OPLOG_OUTPUT_NAME),
            eq(expectedKey),
            newOpLogCaptor.capture(),
            eq(OPLOG_OUTPUT_SUFFIX));

    LogicalObjectWritable newOpLog = newOpLogCaptor.getValue();
    Assert.assertEquals("New version incorrect!", 2, newOpLog.getVersion());
    Assert.assertFalse("New pending state incorrect!", newOpLog.isPendingState());
    Assert.assertEquals("Logical path is not the same!", logicalPath, newOpLog.getLogicalPath());
    Assert.assertEquals(
        "Phy path is not the same!", phyPath, newOpLog.getPhysicalPath());

    verify(mockContext, times(1))
        .getCounter(eq(FsckCounters.PARTIAL_RESTORE_INVALID_STATE_META_COMMITTED_AND_NO_PHY_DATA));
  }
}
