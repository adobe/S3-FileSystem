/*

 Copyright 2021 Adobe. All rights reserved.
 This file is licensed to you under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License. You may obtain a copy
 of the License at http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software distributed under
 the License is distributed on an "AS IS" BASIS, WITHOUT
 WARRANTIES OR REPRESENTATIONS
 OF ANY KIND, either express or implied. See the License for the specific language
 governing permissions and limitations under the License.
 */

package com.adobe.s3fs.operationlog;

import com.adobe.s3fs.common.runtime.FileSystemRuntime;
import com.adobe.s3fs.metastore.api.*;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.PutObjectResult;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;
import java.util.UUID;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class S3MetadataOperationLogTest {

  @Mock
  private AmazonS3 mockAmazonS3;

  @Mock
  private FileSystemRuntime mockRuntime;

  private ArgumentCaptor<InputStream> inputStreamArgumentCaptor;

  private S3MetadataOperationLog s3MetadataOperationLog;

  @Before
  public void init() {
    MockitoAnnotations.initMocks(this);

    inputStreamArgumentCaptor = ArgumentCaptor.forClass(InputStream.class);
    s3MetadataOperationLog = new S3MetadataOperationLog(mockAmazonS3, "bucket", mockRuntime);
  }

  @Test
  public void testLogCreateObjectIsSuccessful() throws IOException {
    VersionedObjectHandle objectHandle = mockObjectHandle(1);
    String prefix = objectHandle.id() + ".info";
    doReturn(new PutObjectResult()).when(mockAmazonS3).putObject(anyString(), anyString(), any(), any());

    Optional<MetadataOperationLog.LogEntryHandle> logEntryHandle = s3MetadataOperationLog.logCreateOperation(objectHandle);

    verify(mockAmazonS3).putObject(eq("bucket"), eq(prefix), inputStreamArgumentCaptor.capture(), any());
    assertMetadataIsWrittenToS3(objectHandle, ObjectOperationType.CREATE, OperationLogEntryState.PENDING);

    assertTrue(logEntryHandle.isPresent());
    clearInvocations(mockAmazonS3);
    assertTrue(logEntryHandle.get().commit());
    verify(mockAmazonS3).putObject(eq("bucket"), eq(prefix), inputStreamArgumentCaptor.capture(), any());
    assertMetadataIsWrittenToS3(objectHandle, ObjectOperationType.CREATE, OperationLogEntryState.COMMITTED);
  }

  @Test
  public void testLogCreateObjectRollbackIsSuccessful() throws IOException {
    VersionedObjectHandle objectHandle = mockObjectHandle(1);
    String prefix = objectHandle.id() + ".info";
    doReturn(new PutObjectResult()).when(mockAmazonS3).putObject(anyString(), anyString(), any(), any());

    Optional<MetadataOperationLog.LogEntryHandle> logEntryHandle = s3MetadataOperationLog.logCreateOperation(objectHandle);

    verify(mockAmazonS3).putObject(eq("bucket"), eq(prefix), inputStreamArgumentCaptor.capture(), any());
    assertMetadataIsWrittenToS3(objectHandle, ObjectOperationType.CREATE, OperationLogEntryState.PENDING);

    assertTrue(logEntryHandle.isPresent());
    assertTrue(logEntryHandle.get().rollback());
    verify(mockAmazonS3).deleteObject(eq("bucket"), eq(prefix));
  }

  @Test
  public void testLogCreateObjectOnFailure() {
    VersionedObjectHandle objectHandle = mockObjectHandle(1);
    doThrow(new RuntimeException()).when(mockAmazonS3).putObject(anyString(), anyString(), any(), any());

    Optional<MetadataOperationLog.LogEntryHandle> logEntryHandle = s3MetadataOperationLog.logCreateOperation(objectHandle);

    assertFalse(logEntryHandle.isPresent());
  }

  @Test
  public void testLogCreateObjectOnRollbackFailure() throws IOException {
    VersionedObjectHandle objectHandle = mockObjectHandle(1);
    String prefix = objectHandle.id() + ".info";
    doReturn(new PutObjectResult()).when(mockAmazonS3).putObject(anyString(), anyString(), any(), any());

    Optional<MetadataOperationLog.LogEntryHandle> logEntryHandle = s3MetadataOperationLog.logCreateOperation(objectHandle);

    verify(mockAmazonS3).putObject(eq("bucket"), eq(prefix), inputStreamArgumentCaptor.capture(), any());
    assertMetadataIsWrittenToS3(objectHandle, ObjectOperationType.CREATE, OperationLogEntryState.PENDING);

    assertTrue(logEntryHandle.isPresent());
    clearInvocations(mockAmazonS3);
    doThrow(new RuntimeException()).when(mockAmazonS3).deleteObject(anyString(), anyString());
    assertFalse(logEntryHandle.get().rollback());
  }

  @Test
  public void testLogCreateObjectOnCommitFailure() throws IOException {
    VersionedObjectHandle objectHandle = mockObjectHandle(1);
    String prefix = objectHandle.id() + ".info";
    doReturn(new PutObjectResult()).when(mockAmazonS3).putObject(anyString(), anyString(), any(), any());

    Optional<MetadataOperationLog.LogEntryHandle> logEntryHandle = s3MetadataOperationLog.logCreateOperation(objectHandle);

    verify(mockAmazonS3).putObject(eq("bucket"), eq(prefix), inputStreamArgumentCaptor.capture(), any());
    assertMetadataIsWrittenToS3(objectHandle, ObjectOperationType.CREATE, OperationLogEntryState.PENDING);

    assertTrue(logEntryHandle.isPresent());
    doThrow(new RuntimeException()).when(mockAmazonS3).putObject(anyString(), anyString(), any(), any());
    assertFalse(logEntryHandle.get().commit());
  }

  @Test
  public void testLogUpdateObjectOperationIsSuccessful() throws IOException {
    UUID id = UUID.randomUUID();
    VersionedObjectHandle objectHandle1 = mockObjectHandle(4, id, mockObjectMetadata(34));
    VersionedObjectHandle objectHandle2 = mockObjectHandle(5, id, mockObjectMetadata(45));

    String prefix = id + ".info";
    doReturn(new PutObjectResult()).when(mockAmazonS3).putObject(anyString(), anyString(), any(), any());

    Optional<MetadataOperationLog.LogEntryHandle> logEntryHandle =
        s3MetadataOperationLog.logUpdateOperation(objectHandle1, objectHandle2);

    verify(mockAmazonS3).putObject(eq("bucket"), eq(prefix), inputStreamArgumentCaptor.capture(), any());
    assertMetadataIsWrittenToS3(objectHandle2, ObjectOperationType.UPDATE, OperationLogEntryState.PENDING);

    assertTrue(logEntryHandle.isPresent());
    clearInvocations(mockAmazonS3);
    assertTrue(logEntryHandle.get().commit());
    verify(mockAmazonS3).putObject(eq("bucket"), eq(prefix), inputStreamArgumentCaptor.capture(), any());
    assertMetadataIsWrittenToS3(objectHandle2, ObjectOperationType.UPDATE, OperationLogEntryState.COMMITTED);
  }

  @Test
  public void testLogUpdateObjectRollbackIsSuccessful() throws IOException {
    UUID id = UUID.randomUUID();
    VersionedObjectHandle objectHandle1 = mockObjectHandle(4, id, mockObjectMetadata(34));
    VersionedObjectHandle objectHandle2 = mockObjectHandle(5, id, mockObjectMetadata(78));

    String prefix = id + ".info";
    doReturn(new PutObjectResult()).when(mockAmazonS3).putObject(anyString(), anyString(), any(), any());

    Optional<MetadataOperationLog.LogEntryHandle> logEntryHandle =
        s3MetadataOperationLog.logUpdateOperation(objectHandle1, objectHandle2);

    verify(mockAmazonS3).putObject(eq("bucket"), eq(prefix), inputStreamArgumentCaptor.capture(), any());
    assertMetadataIsWrittenToS3(objectHandle2, ObjectOperationType.UPDATE, OperationLogEntryState.PENDING);

    assertTrue(logEntryHandle.isPresent());
    clearInvocations(mockAmazonS3);
    assertTrue(logEntryHandle.get().rollback());
    verify(mockAmazonS3).putObject(eq("bucket"), eq(prefix), inputStreamArgumentCaptor.capture(), any());
    assertMetadataIsWrittenToS3(objectHandle1, ObjectOperationType.UPDATE, OperationLogEntryState.COMMITTED);
  }

  @Test
  public void testLogUpdateObjectRollbackIsSuccessfulWhenRollingToCreatedState() throws IOException {
    UUID id = UUID.randomUUID();
    VersionedObjectHandle objectHandle1 = mockObjectHandle(1, id, mockObjectMetadata(34));
    VersionedObjectHandle objectHandle2 = mockObjectHandle(2, id, mockObjectMetadata(78));

    String prefix = id + ".info";
    doReturn(new PutObjectResult()).when(mockAmazonS3).putObject(anyString(), anyString(), any(), any());

    Optional<MetadataOperationLog.LogEntryHandle> logEntryHandle =
        s3MetadataOperationLog.logUpdateOperation(objectHandle1, objectHandle2);

    verify(mockAmazonS3).putObject(eq("bucket"), eq(prefix), inputStreamArgumentCaptor.capture(), any());
    assertMetadataIsWrittenToS3(objectHandle2, ObjectOperationType.UPDATE, OperationLogEntryState.PENDING);

    assertTrue(logEntryHandle.isPresent());
    clearInvocations(mockAmazonS3);
    assertTrue(logEntryHandle.get().rollback());
    verify(mockAmazonS3).putObject(eq("bucket"), eq(prefix), inputStreamArgumentCaptor.capture(), any());
    assertMetadataIsWrittenToS3(objectHandle1, ObjectOperationType.CREATE, OperationLogEntryState.COMMITTED);
  }

  @Test
  public void testLogUpdateObjectOnFailure() {
    UUID id = UUID.randomUUID();
    VersionedObjectHandle objectHandle1 = mockObjectHandle(1, id, mockObjectMetadata(34));
    VersionedObjectHandle objectHandle2 = mockObjectHandle(2, id, mockObjectMetadata(78));

    doThrow(new RuntimeException()).when(mockAmazonS3).putObject(anyString(), anyString(), any(), any());

    Optional<MetadataOperationLog.LogEntryHandle> logEntryHandle =
        s3MetadataOperationLog.logUpdateOperation(objectHandle1, objectHandle2);

    assertFalse(logEntryHandle.isPresent());
  }

  @Test
  public void testLogUpdateObjectOnRollbackFailure() throws IOException {
    UUID id = UUID.randomUUID();
    VersionedObjectHandle objectHandle1 = mockObjectHandle(3, id, mockObjectMetadata(34));
    VersionedObjectHandle objectHandle2 = mockObjectHandle(4, id, mockObjectMetadata(78));

    String prefix = id + ".info";
    doReturn(new PutObjectResult()).when(mockAmazonS3).putObject(anyString(), anyString(), any(), any());

    Optional<MetadataOperationLog.LogEntryHandle> logEntryHandle =
        s3MetadataOperationLog.logUpdateOperation(objectHandle1, objectHandle2);

    verify(mockAmazonS3).putObject(eq("bucket"), eq(prefix), inputStreamArgumentCaptor.capture(), any());
    assertMetadataIsWrittenToS3(objectHandle2, ObjectOperationType.UPDATE, OperationLogEntryState.PENDING);

    doThrow(new RuntimeException()).when(mockAmazonS3).putObject(anyString(), anyString(), any(), any());
    assertFalse(logEntryHandle.get().rollback());
  }

  @Test
  public void testLogUpdateObjectOnCommitFailure() throws IOException {
    UUID id = UUID.randomUUID();
    VersionedObjectHandle objectHandle1 = mockObjectHandle(3, id, mockObjectMetadata(34));
    VersionedObjectHandle objectHandle2 = mockObjectHandle(4, id, mockObjectMetadata(78));

    String prefix = id + ".info";
    doReturn(new PutObjectResult()).when(mockAmazonS3).putObject(anyString(), anyString(), any(), any());

    Optional<MetadataOperationLog.LogEntryHandle> logEntryHandle =
        s3MetadataOperationLog.logUpdateOperation(objectHandle1, objectHandle2);

    verify(mockAmazonS3).putObject(eq("bucket"), eq(prefix), inputStreamArgumentCaptor.capture(), any());
    assertMetadataIsWrittenToS3(objectHandle2, ObjectOperationType.UPDATE, OperationLogEntryState.PENDING);

    doThrow(new RuntimeException()).when(mockAmazonS3).putObject(anyString(), anyString(), any(), any());
    assertFalse(logEntryHandle.get().commit());
  }

  @Test
  public void testLogDeleteObjectIsSuccessful() throws IOException {
    VersionedObjectHandle objectHandle = mockObjectHandle(4);
    String prefix = objectHandle.id() + ".info";

    doReturn(new PutObjectResult()).when(mockAmazonS3).putObject(anyString(), anyString(), any(), any());

    Optional<MetadataOperationLog.LogEntryHandle> logEntryHandle =
        s3MetadataOperationLog.logDeleteOperation(objectHandle);

    assertTrue(logEntryHandle.isPresent());
    verify(mockAmazonS3).putObject(eq("bucket"), eq(prefix), inputStreamArgumentCaptor.capture(), any());
    assertMetadataIsWrittenToS3(objectHandle, ObjectOperationType.DELETE, OperationLogEntryState.PENDING);

    clearInvocations(mockAmazonS3);
    assertTrue(logEntryHandle.get().commit());
    verify(mockAmazonS3).putObject(eq("bucket"), eq(prefix), inputStreamArgumentCaptor.capture(), any());
    assertMetadataIsWrittenToS3(objectHandle, ObjectOperationType.DELETE, OperationLogEntryState.COMMITTED);
    verify(mockAmazonS3).deleteObject(eq("bucket"), eq(prefix));
  }

  @Test
  public void testLogDeleteObjectRollbackIsSuccessful() throws IOException {
    VersionedObjectHandle objectHandle = mockObjectHandle(4);
    String prefix = objectHandle.id() + ".info";

    doReturn(new PutObjectResult()).when(mockAmazonS3).putObject(anyString(), anyString(), any(), any());

    Optional<MetadataOperationLog.LogEntryHandle> logEntryHandle =
        s3MetadataOperationLog.logDeleteOperation(objectHandle);

    assertTrue(logEntryHandle.isPresent());
    verify(mockAmazonS3).putObject(eq("bucket"), eq(prefix), inputStreamArgumentCaptor.capture(), any());
    assertMetadataIsWrittenToS3(objectHandle, ObjectOperationType.DELETE, OperationLogEntryState.PENDING);

    clearInvocations(mockAmazonS3);
    assertTrue(logEntryHandle.get().rollback());
    verify(mockAmazonS3).putObject(eq("bucket"), eq(prefix), inputStreamArgumentCaptor.capture(), any());
    assertMetadataIsWrittenToS3(objectHandle, ObjectOperationType.UPDATE, OperationLogEntryState.COMMITTED);
  }

  @Test
  public void testLogDeleteObjectRollbackIsSuccessfulWhenRollingToCreatedState() throws IOException {
    VersionedObjectHandle objectHandle = mockObjectHandle(1);
    String prefix = objectHandle.id() + ".info";

    doReturn(new PutObjectResult()).when(mockAmazonS3).putObject(anyString(), anyString(), any(), any());

    Optional<MetadataOperationLog.LogEntryHandle> logEntryHandle =
        s3MetadataOperationLog.logDeleteOperation(objectHandle);

    assertTrue(logEntryHandle.isPresent());
    verify(mockAmazonS3).putObject(eq("bucket"), eq(prefix), inputStreamArgumentCaptor.capture(), any());
    assertMetadataIsWrittenToS3(objectHandle, ObjectOperationType.DELETE, OperationLogEntryState.PENDING);

    clearInvocations(mockAmazonS3);
    assertTrue(logEntryHandle.get().rollback());
    verify(mockAmazonS3).putObject(eq("bucket"), eq(prefix), inputStreamArgumentCaptor.capture(), any());
    assertMetadataIsWrittenToS3(objectHandle, ObjectOperationType.CREATE, OperationLogEntryState.COMMITTED);
  }

  @Test
  public void testLogDeleteObjectOnFailure() {
    VersionedObjectHandle objectHandle = mockObjectHandle(1);
    doThrow(new RuntimeException()).when(mockAmazonS3).putObject(anyString(), anyString(), any(), any());

    Optional<MetadataOperationLog.LogEntryHandle> logEntryHandle =
        s3MetadataOperationLog.logDeleteOperation(objectHandle);

    assertFalse(logEntryHandle.isPresent());
  }

  @Test
  public void testLogDeleteObjectOnRollbackFailure() throws IOException {
    VersionedObjectHandle objectHandle = mockObjectHandle(1);
    String prefix = objectHandle.id() + ".info";

    doReturn(new PutObjectResult()).when(mockAmazonS3).putObject(anyString(), anyString(), any(), any());

    Optional<MetadataOperationLog.LogEntryHandle> logEntryHandle =
        s3MetadataOperationLog.logDeleteOperation(objectHandle);

    assertTrue(logEntryHandle.isPresent());
    verify(mockAmazonS3).putObject(eq("bucket"), eq(prefix), inputStreamArgumentCaptor.capture(), any());
    assertMetadataIsWrittenToS3(objectHandle, ObjectOperationType.DELETE, OperationLogEntryState.PENDING);

    clearInvocations(mockAmazonS3);
    doThrow(new RuntimeException()).when(mockAmazonS3).putObject(anyString(), any(), any(), any());
    assertFalse(logEntryHandle.get().rollback());
  }

  @Test
  public void testLogDeleteObjectOnCommitFailure() throws IOException {
    VersionedObjectHandle objectHandle = mockObjectHandle(1);
    String prefix = objectHandle.id() + ".info";

    doReturn(new PutObjectResult()).when(mockAmazonS3).putObject(anyString(), anyString(), any(), any());

    Optional<MetadataOperationLog.LogEntryHandle> logEntryHandle =
        s3MetadataOperationLog.logDeleteOperation(objectHandle);

    assertTrue(logEntryHandle.isPresent());
    verify(mockAmazonS3).putObject(eq("bucket"), eq(prefix), inputStreamArgumentCaptor.capture(), any());
    assertMetadataIsWrittenToS3(objectHandle, ObjectOperationType.DELETE, OperationLogEntryState.PENDING);

    clearInvocations(mockAmazonS3);
    doThrow(new RuntimeException()).when(mockAmazonS3).putObject(anyString(), any(), any(), any());
    assertFalse(logEntryHandle.get().commit());
  }

  @Test
  public void testAmendObject() throws IOException {
    UUID id = UUID.randomUUID();
    String prefix = id + ".info";
    ObjectMetadata objectMetadata = mockObjectMetadata(10);
    doReturn(new PutObjectResult()).when(mockAmazonS3).putObject(anyString(), anyString(), any(), any());

    assertTrue(s3MetadataOperationLog.amendObject(objectMetadata, id, 2, ObjectOperationType.UPDATE));

    verify(mockAmazonS3).putObject(eq("bucket"), eq(prefix), inputStreamArgumentCaptor.capture(), any());
    LogicalFileMetadataV2 serializedMeta = deserializeMetadata(inputStreamArgumentCaptor.getValue());
    assertSerializedMetaMatchesObjectMeta(objectMetadata, serializedMeta);
    assertEquals(2, serializedMeta.getVersion());
    assertEquals(id.toString(), serializedMeta.getId());
    assertEquals(ObjectOperationType.UPDATE, serializedMeta.getType());
    assertEquals(OperationLogEntryState.COMMITTED, serializedMeta.getState());
  }

  @Test
  public void testAmendObjectOnFailure() throws IOException {
    UUID id = UUID.randomUUID();
    String prefix = id + ".info";
    ObjectMetadata objectMetadata = mockObjectMetadata(10);
    doThrow(new RuntimeException()).when(mockAmazonS3).putObject(anyString(), anyString(), any(), any());

    assertFalse(s3MetadataOperationLog.amendObject(objectMetadata, id, 2, ObjectOperationType.UPDATE));
  }

  private void assertMetadataIsWrittenToS3(VersionedObjectHandle handle,
                                           ObjectOperationType operationType,
                                           OperationLogEntryState logState) throws IOException {
    LogicalFileMetadataV2 metadata = deserializeMetadata(inputStreamArgumentCaptor.getValue());
    assertSerializedMetaMatchesHandle(handle, metadata);
    assertEquals(logState, metadata.getState());
    assertEquals(operationType, metadata.getType());
  }

  private LogicalFileMetadataV2 deserializeMetadata(InputStream inputStream) throws IOException {
    return ObjectMetadataSerialization.deserializeFromV2(inputStream);
  }

  private void assertSerializedMetaMatchesHandle(VersionedObjectHandle handle,
                                                 LogicalFileMetadataV2 metadata) {
    assertEquals(handle.id().toString(), metadata.getId());
    assertEquals(handle.version(), metadata.getVersion());
    assertSerializedMetaMatchesObjectMeta(handle.metadata(), metadata);
  }

  private void assertSerializedMetaMatchesObjectMeta(ObjectMetadata objectMetadata, LogicalFileMetadataV2 logicalFileMetadata) {
    assertEquals(objectMetadata.physicalDataCommitted(), logicalFileMetadata.isPhysicalDataCommitted());
    assertEquals(objectMetadata.getSize(), logicalFileMetadata.getSize());
    assertEquals(objectMetadata.getCreationTime(), logicalFileMetadata.getCreationTime());
    assertEquals(objectMetadata.getPhysicalPath().get(), logicalFileMetadata.getPhysicalPath());
  }

  private VersionedObjectHandle mockObjectHandle(int version) {
    return mockObjectHandle(version, UUID.randomUUID(), mockObjectMetadata(10));
  }

  private VersionedObjectHandle mockObjectHandle(int version, UUID id, ObjectMetadata objectMetadata) {
    VersionedObjectHandle objectHandle = mock(VersionedObjectHandle.class);
    when(objectHandle.id()).thenReturn(id);
    when(objectHandle.version()).thenReturn(version);
    when(objectHandle.metadata()).thenReturn(objectMetadata);
    return objectHandle;
  }

  private ObjectMetadata mockObjectMetadata(int size) {
    return ObjectMetadata.builder()
        .key(new Path("s3n://file"))
        .creationTime(10)
        .size(size)
        .isDirectory(false)
        .physicalPath("phys_path")
        .physicalDataCommitted(true)
        .build();
  }
}
