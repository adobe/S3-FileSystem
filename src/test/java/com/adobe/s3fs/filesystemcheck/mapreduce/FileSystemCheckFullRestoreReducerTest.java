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
import com.adobe.s3fs.utils.mapreduce.TextArrayWritable;
import com.adobe.s3fs.filesystemcheck.mapreduce.data.VersionedObjectWritable;
import com.adobe.s3fs.filesystemcheck.mapreduce.multioutputs.MultiOutputsFactory;
import com.adobe.s3fs.utils.aws.s3.S3Helpers;
import com.adobe.s3fs.metastore.internal.dynamodb.versioning.VersionedObject;
import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@RunWith(DataProviderRunner.class)
public class FileSystemCheckFullRestoreReducerTest {

  @Mock private MultipleOutputs mockMultipleOutputs;

  @Mock private MultiOutputsFactory mockMultiOutputsFactory;

  @Mock private Counter mockCounter;

  @Mock
  private Reducer<Text, LogicalObjectWritable, Text, Text>.Context mockContext;

  private Configuration configuration;

  private FileSystemCheckFullRestoreReducer reducer;

  @Before
  public void setup() throws IOException, InterruptedException {
    MockitoAnnotations.initMocks(this);

    configuration = new Configuration(true);
    configuration.set(LOGICAL_ROOT_PATH, "ks://" + BUCKET);

    when(mockMultiOutputsFactory.newMultipleOutputs(eq(mockContext))).thenReturn(mockMultipleOutputs);

    when(mockContext.getConfiguration()).thenReturn(configuration);
    when(mockContext.getCounter(any(Enum.class))).thenReturn(mockCounter);

    reducer = new FileSystemCheckFullRestoreReducer(mockMultiOutputsFactory);

    reducer.setup(mockContext);
  }

  @Test
  public void testBothValuesPresent() throws IOException, InterruptedException {
    // Prepare
    UUID uuid = UUID.randomUUID();
    String logicalPath = logicalEntry(BUCKET, "dir1/dir2/dir3/file");
    String phyPath = physicalEntry(BUCKET, uuid);
    List<LogicalObjectWritable> writables =
        Arrays.asList(opLogWritable(logicalPath, phyPath, false), s3Writable(phyPath));
    // Act
    reducer.reduce(toText(uuid), writables, mockContext);
    // Verify
    ArgumentCaptor<VersionedObjectWritable> captor =
        ArgumentCaptor.forClass(VersionedObjectWritable.class);
    verify(mockMultipleOutputs, times(1))
        .write(
            eq(METASTORE_OUTPUT_NAME),
            eq(new Text("restoreObject")),
            captor.capture(),
            eq(METASTORE_OUTPUT_SUFFIX));
    verifyVersionedObject(captor.getValue().getVersionedObject(), uuid, logicalPath, phyPath);
    verify(mockMultipleOutputs, never())
        .write(
            eq(S3_OUTPUT_NAME),
            eq(new Text("restoreObject")),
            any(TextArrayWritable.class),
            eq(S3_OUTPUT_SUFFIX));
  }

  @Test
  public void testActivePresentInOpLogButNoActivePhysicalData() throws IOException, InterruptedException {
    // Prepare
    UUID uuid = UUID.randomUUID();
    String logicalPath = logicalEntry(BUCKET, "dir1/dir2/dir3/file");
    String phyPath = physicalEntry(BUCKET, uuid);
    String inactivePhyPath = physicalEntry(BUCKET, uuid);
    // Act
    reducer.reduce(
        toText(uuid),
        Arrays.asList(
            opLogWritable(logicalPath, phyPath, false),
            s3Writable(inactivePhyPath)),
        mockContext);
    // Verify
    verify(mockContext, times(1)).getCounter(eq(FsckCounters.NUM_NO_ACTIVE_PHYSICAL_DATA));
    verify(mockMultipleOutputs, times(1))
        .write(
            eq(NO_ACTIVE_OUTPUT_NAME),
            eq(new Text("opLog")),
            eq(new Text(operationLogEntry(BUCKET, uuid))),
            eq(NO_ACTIVE_OUTPUT_SUFFIX));
    verify(mockMultipleOutputs, times(1))
        .write(
            eq(NO_ACTIVE_OUTPUT_NAME),
            eq(new Text("phyData")),
            eq(new Text(inactivePhyPath)),
            eq(NO_ACTIVE_OUTPUT_SUFFIX));
  }

  @Test
  public void testMultipleValuesPresent() throws IOException, InterruptedException {
    // Prepare
    UUID uuid = UUID.randomUUID();
    String logicalPath = logicalEntry(BUCKET, "dir1/dir2/dir3/file");
    String phyPath = physicalEntry(BUCKET, uuid);
    String inactivePhyPath1 = physicalEntry(BUCKET, uuid);
    String inactivePhyPath2 = physicalEntry(BUCKET, uuid);
    List<LogicalObjectWritable> writables =
        Arrays.asList(
            opLogWritable(logicalPath, phyPath, false),
            s3Writable(phyPath),
            s3Writable(inactivePhyPath1), // another physical path containing the same uuid
            s3Writable(inactivePhyPath2)); // another physical path containing the same uuid
    // Act
    reducer.reduce(toText(uuid), writables, mockContext);
    // Verify
    ArgumentCaptor<VersionedObjectWritable> captor =
        ArgumentCaptor.forClass(VersionedObjectWritable.class);
    verify(mockMultipleOutputs, times(1))
        .write(
            eq(METASTORE_OUTPUT_NAME),
            eq(new Text("restoreObject")),
            captor.capture(),
            eq(METASTORE_OUTPUT_SUFFIX));
    verifyVersionedObject(captor.getValue().getVersionedObject(), uuid, logicalPath, phyPath);
    ArgumentCaptor<TextArrayWritable> arrayCaptor =
        ArgumentCaptor.forClass(TextArrayWritable.class);
    verify(mockMultipleOutputs, times(2))
        .write(
            eq(S3_OUTPUT_NAME),
            eq(new Text("deleteObject")),
            arrayCaptor.capture(),
            eq(S3_OUTPUT_SUFFIX));
    Assert.assertEquals(
        new Text(S3Helpers.getPrefix(inactivePhyPath1)),
        arrayCaptor.getAllValues().get(0).get()[1]);
    Assert.assertEquals(
        new Text(S3Helpers.getPrefix(inactivePhyPath2)),
        arrayCaptor.getAllValues().get(1).get()[1]);
  }

  @Test(expected = IllegalStateException.class)
  public void testThrowsWithMultipleSimilarPhysicalPathForPendingOpLog() throws IOException, InterruptedException {
    // Prepare
    UUID uuid = UUID.randomUUID();
    String logicalPath = logicalEntry(BUCKET, "dir1/dir2/dir3/file");
    String phyPath = physicalEntry(BUCKET, uuid);
    List<LogicalObjectWritable> writables =
        Arrays.asList(
            opLogWritable(logicalPath, phyPath, true),
            s3Writable(phyPath),
            s3Writable(phyPath));
    // Act
    reducer.reduce(toText(uuid), writables, mockContext);
  }

  @Test
  public void testCreatePendingState() throws IOException, InterruptedException {
    // Prepare
    UUID uuid = UUID.randomUUID();
    String logicalPath = logicalEntry(BUCKET, "dir1/dir2/dir3/file");
    String somePhyPath1 = physicalEntry(BUCKET, uuid);
    String somePhyPath2 = physicalEntry(BUCKET, uuid);
    List<LogicalObjectWritable> writables =
        Arrays.asList(
            opLogWritable(logicalPath, UNCOMMITED_PATH_MARKER, true),
            s3Writable(somePhyPath1),
            s3Writable(somePhyPath2));
    // Act
    reducer.reduce(toText(uuid), writables, mockContext);
    // Verify
    verify(mockMultipleOutputs, times(1))
        .write(
            eq(NO_ACTIVE_OUTPUT_NAME),
            eq(new Text("phyData")),
            eq(new Text(somePhyPath1)),
            eq(NO_ACTIVE_OUTPUT_SUFFIX));
    verify(mockMultipleOutputs, times(1))
        .write(
            eq(NO_ACTIVE_OUTPUT_NAME),
            eq(new Text("phyData")),
            eq(new Text(somePhyPath2)),
            eq(NO_ACTIVE_OUTPUT_SUFFIX));
    verify(mockMultipleOutputs, times(1))
        .write(
            eq(NO_ACTIVE_OUTPUT_NAME),
            eq(new Text("opLog")),
            eq(new Text(operationLogEntry(BUCKET, uuid))),
            eq(NO_ACTIVE_OUTPUT_SUFFIX));
  }

  @Test
  public void testWithNoActivePhysicalPathForPendingOpLog() throws IOException, InterruptedException {
    // Prepare
    UUID uuid = UUID.randomUUID();
    UUID secondUuid = UUID.randomUUID();
    String logicalPath = logicalEntry(BUCKET, "dir1/dir2/dir3/file");
    String phyPath = physicalEntry(BUCKET, uuid);
    String anotherPhyPath = physicalEntry(BUCKET, secondUuid);
    List<LogicalObjectWritable> writables =
        Arrays.asList(
            opLogWritable(logicalPath, phyPath, true),
            s3Writable(anotherPhyPath));
    // Act
    reducer.reduce(toText(uuid), writables, mockContext);
    verify(mockMultipleOutputs, never())
        .write(
            eq(PENDING_OUTPUT_NAME), any(Text.class), any(Text.class), eq(PENDING_OUTPUT_SUFFIX));
    verify(mockContext, times(1)).getCounter(eq(FsckCounters.NUM_NO_ACTIVE_PHYSICAL_DATA));
    verify(mockMultipleOutputs, times(1))
        .write(
            eq(NO_ACTIVE_OUTPUT_NAME),
            eq(new Text("opLog")),
            eq(new Text(operationLogEntry(BUCKET, uuid))),
            eq(NO_ACTIVE_OUTPUT_SUFFIX));
    verify(mockMultipleOutputs, times(1))
        .write(
            eq(NO_ACTIVE_OUTPUT_NAME),
            eq(new Text("phyData")),
            eq(new Text(anotherPhyPath)),
            eq(NO_ACTIVE_OUTPUT_SUFFIX));
  }

  @Test
  public void testBothValuesPresentButOpLogIsPending() throws IOException, InterruptedException {
    // Prepare
    UUID uuid = UUID.randomUUID();
    String logicalPath = logicalEntry(BUCKET, "dir1/dir2/dir3/file");
    String phyPath = physicalEntry(BUCKET, uuid);
    List<LogicalObjectWritable> writables =
        Arrays.asList(opLogWritable(logicalPath, phyPath, true), s3Writable(phyPath));
    // Act
    reducer.reduce(toText(uuid), writables, mockContext);
    // Verify
    verify(mockMultipleOutputs, never())
        .write(
            eq(METASTORE_OUTPUT_NAME),
            eq(new Text("restoreObject")),
            any(VersionedObjectWritable.class),
            eq(METASTORE_OUTPUT_SUFFIX));
    verify(mockMultipleOutputs, times(1))
        .write(
            eq(PENDING_OUTPUT_NAME),
            eq(new Text(phyPath)),
            eq(new Text(operationLogEntry(BUCKET, uuid))),
            eq(PENDING_OUTPUT_SUFFIX));
    verify(mockMultipleOutputs, never())
        .write(
            eq(S3_OUTPUT_NAME),
            eq(new Text("deleteObject")),
            any(TextArrayWritable.class),
            eq(S3_OUTPUT_SUFFIX));
  }

  @Test
  public void testMultipleS3ValuesPresentButOpLogIsPending() throws IOException, InterruptedException {
    // Prepare
    UUID uuid = UUID.randomUUID();
    String logicalPath = logicalEntry(BUCKET, "dir1/dir2/dir3/file");
    String phyPath = physicalEntry(BUCKET, uuid);
    String inactivePhyPath1 = physicalEntry(BUCKET, uuid);
    String inactivePhyPath2 = physicalEntry(BUCKET, uuid);
    List<LogicalObjectWritable> writables =
        Arrays.asList(
            opLogWritable(logicalPath, phyPath, true),
            s3Writable(phyPath),
            s3Writable(inactivePhyPath1),
            s3Writable(inactivePhyPath2));
    // Act
    reducer.reduce(toText(uuid), writables, mockContext);
    // Verify
    verify(mockMultipleOutputs, never())
        .write(
            eq(METASTORE_OUTPUT_NAME),
            eq(new Text("restoreObject")),
            any(VersionedObjectWritable.class),
            eq(METASTORE_OUTPUT_SUFFIX));
    verify(mockMultipleOutputs, times(1))
        .write(
            eq(PENDING_OUTPUT_NAME),
            eq(new Text(phyPath)),
            eq(new Text(operationLogEntry(BUCKET, uuid))),
            eq(PENDING_OUTPUT_SUFFIX));
    ArgumentCaptor<TextArrayWritable> arrayCaptor = ArgumentCaptor.forClass(TextArrayWritable.class);
    verify(mockMultipleOutputs, times(2))
        .write(
            eq(S3_OUTPUT_NAME),
            eq(new Text("deleteObject")),
            arrayCaptor.capture(),
            eq(S3_OUTPUT_SUFFIX));
    Assert.assertEquals(
        new Text(S3Helpers.getPrefix(inactivePhyPath1)),
        arrayCaptor.getAllValues().get(0).get()[1]);
    Assert.assertEquals(
        new Text(S3Helpers.getPrefix(inactivePhyPath2)),
        arrayCaptor.getAllValues().get(1).get()[1]);
  }

  @Test
  public void testOnlyS3DataPresent() throws IOException, InterruptedException {
    // Prepare
    UUID uuid = UUID.randomUUID();
    String phyPath = physicalEntry(BUCKET, uuid);
    String prefix = new Path(phyPath).toUri().getPath().substring(1);
    // Act
    reducer.reduce(toText(uuid), Collections.singletonList(s3Writable(phyPath)), mockContext);
    // Verify
    ArgumentCaptor<TextArrayWritable> captor = ArgumentCaptor.forClass(TextArrayWritable.class);
    verify(mockMultipleOutputs, times(1))
        .write(
            eq(S3_OUTPUT_NAME),
            eq(new Text("deleteObject")),
            captor.capture(),
            eq(S3_OUTPUT_SUFFIX));
    TextArrayWritable captured = captor.getValue();
    Assert.assertEquals(new Text(BUCKET), captured.get()[0]);
    Assert.assertEquals(new Text(prefix), captured.get()[1]);
  }

  @Test
  public void testMultipleS3DataOnlyPresent() throws IOException, InterruptedException {
    // Prepare
    UUID uuid = UUID.randomUUID();
    String phyPath = physicalEntry(BUCKET, uuid);
    String prefix = new Path(phyPath).toUri().getPath().substring(1);
    String secondPhyPath = physicalEntry(BUCKET, uuid);
    String secondPrefix = new Path(secondPhyPath).toUri().getPath().substring(1);
    // Act
    reducer.reduce(
        toText(uuid),
        Arrays.asList(s3Writable(phyPath), s3Writable(secondPhyPath)),
        mockContext);
    // Verify
    ArgumentCaptor<TextArrayWritable> captor = ArgumentCaptor.forClass(TextArrayWritable.class);
    verify(mockMultipleOutputs, times(2))
        .write(
            eq(S3_OUTPUT_NAME),
            eq(new Text("deleteObject")),
            captor.capture(),
            eq(S3_OUTPUT_SUFFIX));
    List<TextArrayWritable> expectedValues = captor.getAllValues();
    Assert.assertEquals(new Text(BUCKET), expectedValues.get(0).get()[0]);
    Assert.assertEquals(new Text(prefix), expectedValues.get(0).get()[1]);
    Assert.assertEquals(new Text(BUCKET), expectedValues.get(1).get()[0]);
    Assert.assertEquals(new Text(secondPrefix), expectedValues.get(1).get()[1]);
  }

  @DataProvider
  public static Object[][] pendingState() {
    return new Object[][] {
        new Object[] {true},
        new Object[] {false},
    };
  }

  @Test
  @UseDataProvider("pendingState")
  public void testOnlyLogDataPresent(boolean pendingState) throws IOException, InterruptedException {
    // Prepare
    UUID uuid = UUID.randomUUID();
    String logicalPath = logicalEntry(BUCKET, "dir1/dir2/dir3/file2");
    String phyPath = physicalEntry(BUCKET, uuid);
    // Act
    reducer.reduce(
        toText(uuid),
        Collections.singleton(opLogWritable(logicalPath, phyPath, pendingState)),
        mockContext);
    // Verify
    ArgumentCaptor<TextArrayWritable> captor = ArgumentCaptor.forClass(TextArrayWritable.class);
    verify(mockMultipleOutputs, times(1))
        .write(
            eq(S3_OUTPUT_NAME),
            eq(new Text("deleteObject")),
            captor.capture(),
            eq(S3_OUTPUT_SUFFIX));
    Assert.assertEquals(new Text(BUCKET), captor.getValue().get()[0]);
    Assert.assertEquals(new Text(uuid.toString() + ".info"), captor.getValue().get()[1]);
  }

  @Test
  public void testNoValuePresent() throws IOException, InterruptedException {
    // Act
    reducer.reduce(toText(UUID.randomUUID()), Collections.emptyList(), mockContext);
    // Verify
    verify(mockContext, never()).getCounter(any(Enum.class));
  }

  private void verifyVersionedObject(
      VersionedObject object, UUID id, String metaPath, String phyPath) {
    Assert.assertEquals(VERSION, object.version());
    Assert.assertEquals(id, object.id());
    Assert.assertEquals(SIZE, object.metadata().getSize());
    Assert.assertEquals(CREATION_TIME, object.metadata().getCreationTime());
    Assert.assertFalse(object.metadata().isDirectory());
    Assert.assertTrue(object.metadata().physicalDataCommitted());
    Assert.assertEquals(metaPath, object.metadata().getKey().toString());
    String actualPhyPath = object.metadata().getPhysicalPath().orElseThrow(AssertionError::new);
    Assert.assertEquals("Wrong physicalPath!", phyPath, actualPhyPath);
  }
}
