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
import com.adobe.s3fs.filesystemcheck.mapreduce.multioutputs.MultiOutputsFactory;
import com.adobe.s3fs.filesystemcheck.s3.RawS3ScanInputFormat;
import com.adobe.s3fs.metastore.api.ObjectOperationType;
import com.adobe.s3fs.metastore.api.OperationLogEntryState;
import com.adobe.s3fs.metastore.api.VersionedObjectHandle;
import com.adobe.s3fs.utils.aws.s3.model.S3ObjectLocation;
import com.adobe.s3fs.utils.mapreduce.SerializableVoid;
import com.adobe.s3fs.utils.mapreduce.TextArrayWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;

import static com.adobe.s3fs.filesystemcheck.mapreduce.FileSystemMRJobConfig.*;
import static com.adobe.s3fs.filesystemcheck.mapreduce.FsckTestUtils.*;
import static com.adobe.s3fs.filesystemcheck.mapreduce.data.SourceType.FROM_OPLOG;
import static com.adobe.s3fs.filesystemcheck.mapreduce.data.SourceType.FROM_S3;
import static com.adobe.s3fs.metastore.api.ObjectOperationType.*;
import static com.adobe.s3fs.metastore.api.OperationLogEntryState.COMMITTED;
import static com.adobe.s3fs.metastore.api.OperationLogEntryState.PENDING;
import static com.adobe.s3fs.operationlog.ObjectMetadataSerialization.serializeToV2;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class FileSystemCheckS3MapperTest {

  private static final SerializableVoid KEY_VOID = SerializableVoid.INSTANCE;

  @Mock private MultipleOutputs<Text, LogicalObjectWritable> mockMultipleOutputs;

  @Mock private MultiOutputsFactory mockMosFactory;

  @Mock private S3Client mockS3;

  @Mock private Counter mockCounter;

  @Mock private Mapper<SerializableVoid, S3ObjectLocation, Text, LogicalObjectWritable>.Context mockContext;

  private Configuration configuration;

  private FileSystemCheckS3Mapper mapper;

  private Map<UUID, List<LogicalObjectWritable>> internalState = new TreeMap<>();

  private static LogicalObjectWritable extractOpLogEntry(List<LogicalObjectWritable> writables) {
    return writables.stream()
        .filter(logicalObjectWritable -> logicalObjectWritable.getSourceType() == FROM_OPLOG)
        .findFirst()
        .orElseThrow(AssertionError::new);
  }

  private static LogicalObjectWritable extractS3Entry(List<LogicalObjectWritable> writables) {
    return writables.stream()
        .filter(logicalObjectWritable -> logicalObjectWritable.getSourceType() == FROM_S3)
        .findFirst()
        .orElseThrow(AssertionError::new);
  }

  private static void verifyPendingOpLogEntry(LogicalObjectWritable operationLog, String phyPath, String logicalPath) {
    verifyWritable(operationLog, phyPath, logicalPath, FROM_OPLOG, SIZE, CREATION_TIME, VERSION, false, true);
  }

  private static void verifyOpLogEntry(
      LogicalObjectWritable operationLog, String phyPath, String logicalPath) {
    verifyWritable(operationLog, phyPath, logicalPath, FROM_OPLOG, SIZE, CREATION_TIME, VERSION, false, false);
  }

  private static void verifyS3Entry(LogicalObjectWritable s3Entry, String phyPath) {
    verifyWritable(s3Entry, phyPath, "", FROM_S3, 0L, 0L, 0, false, false);
  }

  @Before
  public void setup() throws IOException, InterruptedException {
    MockitoAnnotations.initMocks(this);

    configuration = new Configuration(true);
    configuration.set(LOGICAL_ROOT_PATH, "ks://" + BUCKET);
    configuration.set(RawS3ScanInputFormat.BUCKET_PROP, BUCKET);
    configuration.setInt(S3_DOWNLOAD_BATCH_SIZE, 2);

    when(mockContext.getConfiguration()).thenReturn(configuration);
    when(mockContext.getCounter(any(Enum.class))).thenReturn(mockCounter);

    doAnswer(
            invocation -> {
              Text text = invocation.getArgument(0);
              UUID objHandleId = toUUID(text);
              LogicalObjectWritable writable = invocation.getArgument(1);
              if (!internalState.containsKey(objHandleId)) {
                internalState.put(objHandleId, new ArrayList<>());
              }
              internalState.get(objHandleId).add(writable);
              return null;
            })
        .when(mockContext)
        .write(any(Text.class), any(LogicalObjectWritable.class));

    when(mockMosFactory.newMultipleOutputs(eq(mockContext))).thenReturn(mockMultipleOutputs);

    mapper = new FileSystemCheckS3Mapper((retryPolicy, maxConnections) -> mockS3, mockMosFactory);

    mapper.setup(mockContext);
  }

  @Test
  public void testWithBadStreamOfOpLog() throws IOException, InterruptedException {
    // Prepare
    UUID objectHandleId = UUID.randomUUID();
    String operationLogPrefix = operationLogPrefix(objectHandleId);
    String phyPath = physicalEntry(BUCKET, objectHandleId);
    String logicalPath = logicalEntry(BUCKET, "some/random/prefix/file1");
    injectBadStreamToS3ObjectOpLog(operationLogPrefix, phyPath, logicalPath, objectHandleId);

    S3Object mockS3Object = S3Object.builder()
            .key(operationLogPrefix(objectHandleId))
            .build();

    // Act
    mapper.map(KEY_VOID, toS3ObjectLocation(mockS3Object), mockContext);
    mapper.cleanup(mockContext);
    Assert.assertEquals(0, internalState.size());
    ArgumentCaptor<TextArrayWritable> captor = ArgumentCaptor.forClass(TextArrayWritable.class);
    verify(mockMultipleOutputs, times(1))
        .write(
            eq(S3_OUTPUT_NAME),
            eq(new Text("deleteObject")),
            captor.capture(),
            eq(S3_OUTPUT_SUFFIX));
    List<TextArrayWritable> expectedValues = captor.getAllValues();
    Assert.assertEquals(new Text(BUCKET), expectedValues.get(0).get()[0]);
    Assert.assertEquals(new Text(operationLogPrefix), expectedValues.get(0).get()[1]);
  }

  @Test
  public void testWithOperationLogEntryOnly() throws IOException, InterruptedException {
    // Prepare
    UUID objectHandleId = UUID.randomUUID();
    String operationLogPrefix = operationLogPrefix(objectHandleId);
    String phyPath = physicalEntry(BUCKET, objectHandleId);
    String logicalPath = logicalEntry(BUCKET, "some/random/prefix/file1");
    injectUpdateTypeToS3Object(operationLogPrefix, phyPath, logicalPath, objectHandleId);

    S3Object mockS3Object = S3Object.builder()
            .key(operationLogPrefix(objectHandleId))
            .build();

    // Act
    mapper.map(KEY_VOID, toS3ObjectLocation(mockS3Object), mockContext);
    mapper.cleanup(mockContext);
    // Verify
    Assert.assertEquals(1, internalState.size());
    Assert.assertTrue(internalState.containsKey(objectHandleId));
    List<LogicalObjectWritable> writables = internalState.get(objectHandleId);
    Assert.assertEquals("Expected only 1 value for oplog data!", 1, writables.size());
    verifyOpLogEntry(writables.get(0), phyPath, logicalPath);
  }

  @Test
  public void testPendingStateInOperationLogEntry() throws IOException, InterruptedException {
    // Prepare
    UUID objectHandleId = UUID.randomUUID();
    String phyPath = physicalEntry(BUCKET, objectHandleId);
    String operationLogPrefix = operationLogPrefix(objectHandleId);
    String logicalPath = logicalEntry(BUCKET, "some/random/prefix/file1");
    injectPendingStateToS3Object(operationLogPrefix, phyPath, logicalPath, objectHandleId);

    S3Object mockS3Object = S3Object.builder()
            .key(operationLogPrefix(objectHandleId))
            .build();

    // Act
    mapper.map(KEY_VOID, toS3ObjectLocation(mockS3Object), mockContext);
    mapper.cleanup(mockContext);
    // Verify
    Assert.assertEquals(1, internalState.size());
    Assert.assertTrue(internalState.containsKey(objectHandleId));
    List<LogicalObjectWritable> writables = internalState.get(objectHandleId);
    Assert.assertEquals("Expected only 1 value for oplog data!", 1, writables.size());
    verifyPendingOpLogEntry(writables.get(0), phyPath, logicalPath);
  }

  @Test
  public void testWithUncommittedPhyDataInOperationLogEntry() throws IOException, InterruptedException {
    // Prepare
    UUID objectHandleId = UUID.randomUUID();
    String operationLogPrefix = operationLogPrefix(objectHandleId);
    String logicalPath = logicalEntry(BUCKET, "some/random/prefix/file1");
    injectUncommittedTypeToS3Object(operationLogPrefix, logicalPath, objectHandleId);

    S3Object mockS3Object = S3Object.builder()
            .key(operationLogPrefix(objectHandleId))
            .build();

    // Act
    mapper.map(KEY_VOID, toS3ObjectLocation(mockS3Object), mockContext);
    mapper.cleanup(mockContext);
    // Verify
    Assert.assertEquals(0, internalState.size());
    ArgumentCaptor<TextArrayWritable> captor = ArgumentCaptor.forClass(TextArrayWritable.class);
    verify(mockMultipleOutputs, times(1))
        .write(
            eq(S3_OUTPUT_NAME),
            eq(new Text("deleteObject")),
            captor.capture(),
            eq(S3_OUTPUT_SUFFIX));
    TextArrayWritable captured = captor.getValue();
    Assert.assertEquals(new Text(BUCKET), captured.get()[0]);
    Assert.assertEquals(new Text(operationLogPrefix), captured.get()[1]);
  }

  @Test
  public void testWithDeletedDataInOperationLogEntry() throws IOException, InterruptedException {
    // Prepare
    UUID objectHandleId = UUID.randomUUID();
    String operationLogPrefix = operationLogPrefix(objectHandleId);
    String phyPath = physicalEntry(BUCKET, objectHandleId);
    String logicalPath = logicalEntry(BUCKET, "some/random/prefix/file1");
    injectDeletedTypeToS3Object(operationLogPrefix, phyPath, logicalPath, objectHandleId);

    S3Object mockS3Object = S3Object.builder()
            .key(operationLogPrefix(objectHandleId))
            .build();

    // Act
    mapper.map(KEY_VOID, toS3ObjectLocation(mockS3Object), mockContext);
    mapper.cleanup(mockContext);
    // Verify
    Assert.assertEquals(0, internalState.size());
    ArgumentCaptor<TextArrayWritable> captor = ArgumentCaptor.forClass(TextArrayWritable.class);
    verify(mockMultipleOutputs, times(1))
        .write(
            eq(S3_OUTPUT_NAME),
            eq(new Text("deleteObject")),
            captor.capture(),
            eq(S3_OUTPUT_SUFFIX));
    TextArrayWritable captured = captor.getValue();
    Assert.assertEquals(new Text(BUCKET), captured.get()[0]);
    Assert.assertEquals(new Text(operationLogPrefix), captured.get()[1]);
  }

  @Test
  public void testWithPhysicalDataOnly() throws IOException, InterruptedException {
    // Prepare
    UUID objectHandleId = UUID.randomUUID();
    String phyPath = physicalEntry(BUCKET, objectHandleId);

    S3Object mockS3Object = S3Object.builder()
            .key(phyPath.substring(phyPath.lastIndexOf('/')+1))
            .build();

    // Act
    mapper.map(KEY_VOID, toS3ObjectLocation(mockS3Object), mockContext);
    mapper.cleanup(mockContext);
    // Verify
    Assert.assertEquals(1, internalState.size());
    Assert.assertTrue(internalState.containsKey(objectHandleId));
    List<LogicalObjectWritable> writables = internalState.get(objectHandleId);
    Assert.assertEquals("Expected only 1 writable value for physical data!", 1, writables.size());
    verifyS3Entry(writables.get(0), phyPath);
  }

  @Test
  public void testWithBothPhysicalDataAndOpLogEntry() throws IOException, InterruptedException {
    // Prepare
    UUID objectHandleId = UUID.randomUUID();
    String operationLogPrefix = operationLogPrefix(objectHandleId);
    String phyPath = physicalEntry(BUCKET, objectHandleId);
    String logicalPath = logicalEntry(BUCKET, "some/random/prefix/file1");
    injectUpdateTypeToS3Object(operationLogPrefix, phyPath, logicalPath, objectHandleId);

    S3Object mockPhyS3Object = S3Object.builder()
            .key(phyPath.substring(phyPath.lastIndexOf('/')+1))
            .build();

    S3Object mockOpLogS3Object = S3Object.builder()
            .key(operationLogPrefix(objectHandleId))
            .build();

    // Act
    mapper.map(KEY_VOID, toS3ObjectLocation(mockPhyS3Object), mockContext);
    mapper.map(KEY_VOID, toS3ObjectLocation(mockOpLogS3Object), mockContext);
    mapper.cleanup(mockContext);
    // Verify
    Assert.assertEquals(1, internalState.size());
    Assert.assertTrue(internalState.containsKey(objectHandleId));
    List<LogicalObjectWritable> writables = internalState.get(objectHandleId);
    Assert.assertEquals("Expected only 2 writable values!", 2, writables.size());
    verifyOpLogEntry(extractOpLogEntry(writables), phyPath, logicalPath);
    verifyS3Entry(extractS3Entry(writables), phyPath);
  }

  @Test
  public void testWithMultipleObjectHandleIds() throws IOException, InterruptedException {
    // Prepare
    UUID uuid1 = UUID.randomUUID();
    UUID uuid2 = UUID.randomUUID();
    UUID uuid3 = UUID.randomUUID();
    // uuid1
    String phyPath1 = physicalEntry(BUCKET, uuid1);
    injectUpdateTypeToS3Object(
        operationLogPrefix(uuid1),
        phyPath1,
        logicalEntry(BUCKET, "some/random/prefix/file1"),
        uuid1);
    // uuid2
    String phyPath2 = physicalEntry(BUCKET, uuid2);
    injectUpdateTypeToS3Object(
        operationLogPrefix(uuid2),
        phyPath2,
        logicalEntry(BUCKET, "some/random/prefix/file2"),
        uuid2);
    // uuid3
    String phyPath3 = physicalEntry(BUCKET, uuid3);
    injectUpdateTypeToS3Object(
        operationLogPrefix(uuid3),
        phyPath3,
        logicalEntry(BUCKET, "some/random/prefix/file3"),
        uuid3);

    S3Object mockOpLogUuid1 = S3Object.builder()
            .key(operationLogPrefix(uuid1))
            .build();
    S3Object mockOpLogUuid2 = S3Object.builder()
            .key(operationLogPrefix(uuid2))
            .build();

    S3Object mockOpLogUuid3 = S3Object.builder()
            .key(operationLogPrefix(uuid3))
            .build();
    S3Object mockPhyS3Object = S3Object.builder()
            .key(phyPath1.substring(phyPath1.lastIndexOf('/')+1))
            .build();

    // Act
    mapper.map(KEY_VOID, toS3ObjectLocation(mockOpLogUuid1), mockContext);
    mapper.map(KEY_VOID, toS3ObjectLocation(mockPhyS3Object), mockContext);
    mapper.map(KEY_VOID, toS3ObjectLocation(mockOpLogUuid2), mockContext);
    mapper.map(KEY_VOID, toS3ObjectLocation(mockOpLogUuid3), mockContext);
    mapper.cleanup(mockContext);
    // Verify
    // 3 Object handle ids
    Assert.assertEquals(3, internalState.size());
    Assert.assertTrue(internalState.containsKey(uuid1));
    Assert.assertTrue(internalState.containsKey(uuid2));
    Assert.assertTrue(internalState.containsKey(uuid3));
    // 1 op log entry & 1 physical data
    Assert.assertEquals(2, internalState.get(uuid1).size());
    verifyOpLogEntry(
        extractOpLogEntry(internalState.get(uuid1)),
        phyPath1,
        logicalEntry(BUCKET, "some/random/prefix/file1"));
    verifyS3Entry(extractS3Entry(internalState.get(uuid1)), phyPath1);
    // 1 op log entry
    Assert.assertEquals(1, internalState.get(uuid2).size());
    verifyOpLogEntry(
        internalState.get(uuid2).get(0),
        phyPath2,
        logicalEntry(BUCKET, "some/random/prefix/file2"));
    // 1 op log entry
    Assert.assertEquals(1, internalState.get(uuid3).size());
    verifyOpLogEntry(
        internalState.get(uuid3).get(0),
        phyPath3,
        logicalEntry(BUCKET, "some/random/prefix/file3"));
  }

  @After
  public void clean() {
    internalState.clear();
  }

  private byte[] objectContent(
      String phyPath,
      String logicalPath,
      UUID uuid,
      boolean phyDataCommitted,
      OperationLogEntryState state,
      ObjectOperationType type)
      throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream(100);
    VersionedObjectHandle versionedObject =
        buildVersionedObject(phyPath, logicalPath, uuid, phyDataCommitted);
    serializeToV2(versionedObject, state, type, baos);
    return baos.toByteArray();
  }

  private InputStream getObjectMetadataStream(byte[] inputBuffer) {
    return new ByteArrayInputStream(inputBuffer);
  }

  private void injectStreamIntoS3ObjectMock(String prefix, InputStream is) {
    ResponseInputStream<GetObjectResponse> responseInputStream =
        new ResponseInputStream<>(GetObjectResponse.builder().build(), is);
    when(mockS3.getObject(argThat((GetObjectRequest req) ->
        req != null && req.bucket().equals(BUCKET) && req.key().equals(prefix))))
        .thenReturn(responseInputStream);
  }

  private void injectBadStreamToS3ObjectOpLog(String prefix, String phyPath, String logicalPath, UUID uuid) {
    injectStreamIntoS3ObjectMock(
        prefix,
        new ByteArrayInputStream(new byte[] {'d','e','e','d','b','e','e','f'}));
  }

  private void injectUpdateTypeToS3Object(
      String prefix, String phyPath, String logicalPath, UUID uuid) throws IOException {
    injectStreamIntoS3ObjectMock(
        prefix,
        getObjectMetadataStream(
            objectContent(phyPath, logicalPath, uuid, true, COMMITTED, UPDATE)));
  }

  private void injectUncommittedTypeToS3Object(String prefix, String logicalPath, UUID uuid)
      throws IOException {
    injectStreamIntoS3ObjectMock(
        prefix,
        getObjectMetadataStream(
            objectContent("<uncommitted>", logicalPath, uuid, false, COMMITTED, CREATE)));
  }

  private void injectDeletedTypeToS3Object(
      String prefix, String phyPath, String logicalPath, UUID uuid) throws IOException {
    injectStreamIntoS3ObjectMock(
        prefix,
        getObjectMetadataStream(
            objectContent(phyPath, logicalPath, uuid, true, COMMITTED, DELETE)));
  }

  private void injectPendingStateToS3Object(
      String prefix, String phyPath, String logicalPath, UUID uuid) throws IOException {
    injectStreamIntoS3ObjectMock(
        prefix,
        getObjectMetadataStream(objectContent(phyPath, logicalPath, uuid, true, PENDING, UPDATE)));
  }

  private S3ObjectLocation toS3ObjectLocation(S3Object s3Object) {
    return S3ObjectLocation.builder().bucket(BUCKET).s3Object(s3Object).build();
  }
}
