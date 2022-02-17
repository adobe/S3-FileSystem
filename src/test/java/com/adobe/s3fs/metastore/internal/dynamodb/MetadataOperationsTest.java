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

package com.adobe.s3fs.metastore.internal.dynamodb;

import com.adobe.s3fs.metastore.api.MetadataOperationLog;
import com.adobe.s3fs.metastore.api.ObjectHandle;
import com.adobe.s3fs.metastore.api.ObjectLevelMetrics;
import com.adobe.s3fs.metastore.api.ObjectMetadata;
import com.adobe.s3fs.metastore.internal.dynamodb.operations.MetadataOperations;
import com.adobe.s3fs.metastore.internal.dynamodb.storage.ObjectMetadataStorage;
import com.adobe.s3fs.metastore.internal.dynamodb.versioning.VersionedObject;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Optional;
import java.util.UUID;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

public class MetadataOperationsTest {

  @Mock
  private ObjectMetadataStorage mockObjectStorage;

  @Mock
  private MetadataOperationLog mockOperationLog;

  @Mock
  private MetadataOperationLog.LogEntryHandle mockEntryHandle;

  @Mock
  private ObjectLevelMetrics mockObjectLevelMetrics;

  private MetadataOperations metadataOperations;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
    metadataOperations = new MetadataOperations(mockObjectStorage, mockOperationLog, mockObjectLevelMetrics);
  }

  @Test
  public void testGetObjectReturnsUnderlyingStorage() {
    Path key = new Path("ks://bucket/f");
    VersionedObject mockObject = newFile();
    doReturn(Optional.of(mockObject)).when(mockObjectStorage).getObject(key);

    Optional<VersionedObject> result = metadataOperations.getObject(key);

    assertTrue(result.isPresent());
    assertEquals(mockObject, result.get());

    Path key2 = new Path("ks://bucket/f1");
    when(mockObjectStorage.getObject(key2)).thenReturn(Optional.empty());

    Optional<VersionedObject> result2 = metadataOperations.getObject(key2);

    assertFalse(result2.isPresent());
  }

  @Test
  public void testStoreObjectIsSuccessful() {
    VersionedObject mockObj = newFile();
    when(mockObjectStorage.storeSingleObject(mockObj)).thenReturn(true);
    when(mockOperationLog.logCreateOperation(mockObj)).thenReturn(Optional.of(mockEntryHandle));

    assertTrue(metadataOperations.store(mockObj));

    verify(mockOperationLog, times(1)).logCreateOperation(mockObj);
    verify(mockObjectStorage, times(1)).storeSingleObject(mockObj);
    verify(mockEntryHandle, times(1)).commit();
  }

  @Test
  public void testStoreObjectIsSuccessfulButFailedCommitOpLog() {
    VersionedObject mockObj = newFile();
    when(mockObjectStorage.storeSingleObject(mockObj)).thenReturn(true);
    when(mockOperationLog.logCreateOperation(mockObj)).thenReturn(Optional.of(mockEntryHandle));
    when(mockEntryHandle.commit()).thenReturn(false);

    assertTrue(metadataOperations.store(mockObj));

    verify(mockObjectLevelMetrics, times(1)).incrFailedCommitOpLog();
    verify(mockEntryHandle, times(1)).commit();
  }

  @Test
  public void testStoreObjectDoesNoLogDirectoryCreations() {
    VersionedObject dir = newDirectory();
    when(mockObjectStorage.storeSingleObject(dir)).thenReturn(true);

    assertTrue(metadataOperations.store(dir));

    verify(mockObjectStorage, times(1)).storeSingleObject(dir);
    verifyZeroInteractions(mockOperationLog);
  }

  @Test
  public void testStoreObjectFailedDirectoryCreation() {
    VersionedObject dir = newDirectory();
    when(mockObjectStorage.storeSingleObject(dir)).thenReturn(false);

    assertFalse(metadataOperations.store(dir));

    verify(mockObjectStorage, times(1)).storeSingleObject(dir);
    verify(mockObjectLevelMetrics, times(1)).incrFailedDynamoDB();
    verifyZeroInteractions(mockOperationLog);

  }

  @Test
  public void testStoreObjectReturnsFalseIfOperationLogFails() {
    VersionedObject mockObj = newFile();
    when(mockOperationLog.logCreateOperation(mockObj)).thenReturn(Optional.empty());

    assertFalse(metadataOperations.store(mockObj));

    verify(mockObjectLevelMetrics, times(1)).incrFailedPendingOpLog();
    verify(mockOperationLog, times(1)).logCreateOperation(mockObj);
    verifyZeroInteractions(mockObjectStorage);
  }

  @Test
  public void testStoreObjectReturnsFalseIfStorageFails() {
    VersionedObject mockObj = newFile();
    when(mockOperationLog.logCreateOperation(mockObj)).thenReturn(Optional.of(mockEntryHandle));
    when(mockObjectStorage.storeSingleObject(mockObj)).thenReturn(false);
    when(mockEntryHandle.rollback()).thenReturn(true);

    assertFalse(metadataOperations.store(mockObj));

    verify(mockOperationLog, times(1)).logCreateOperation(mockObj);
    verify(mockObjectStorage, times(1)).storeSingleObject(mockObj);
    verify(mockEntryHandle, times(1)).rollback();
    verify(mockObjectLevelMetrics, times(1)).incrFailedDynamoDB();
    verify(mockObjectLevelMetrics, times(1)).incrOpLogSuccessfulRollback();
  }

  @Test
  public void testStoreObjectReturnsFalseIfStorageFailsAndOpLogRollbackFails() {
    VersionedObject mockObj = newFile();
    when(mockOperationLog.logCreateOperation(mockObj)).thenReturn(Optional.of(mockEntryHandle));
    when(mockObjectStorage.storeSingleObject(mockObj)).thenReturn(false);
    when(mockEntryHandle.rollback()).thenReturn(false);

    assertFalse(metadataOperations.store(mockObj));

    verify(mockOperationLog, times(1)).logCreateOperation(mockObj);
    verify(mockObjectStorage, times(1)).storeSingleObject(mockObj);
    verify(mockEntryHandle, times(1)).rollback();
    verify(mockObjectLevelMetrics, times(1)).incrFailedDynamoDB();
    verify(mockObjectLevelMetrics, times(1)).incrOpLogFailedRollback();
  }

  @Test
  public void testDeleteObjectIsSuccessful() {
    VersionedObject mockObj = newFile();
    when(mockOperationLog.logDeleteOperation(mockObj)).thenReturn(Optional.of(mockEntryHandle));
    when(mockObjectStorage.deleteSingleObject(mockObj)).thenReturn(true);

    assertTrue(metadataOperations.delete(mockObj));

    verify(mockOperationLog, times(1)).logDeleteOperation(mockObj);
    verify(mockObjectStorage, times(1)).deleteSingleObject(mockObj);
    verify(mockEntryHandle, times(1)).commit();
  }

  @Test
  public void testDeleteObjectDoesNotLogDirectoryDeletion() {
    VersionedObject dir = newDirectory();
    when(mockObjectStorage.deleteSingleObject(dir)).thenReturn(true);

    assertTrue(metadataOperations.delete(dir));

    verify(mockObjectStorage, times(1)).deleteSingleObject(dir);
    verifyZeroInteractions(mockOperationLog);
  }

  @Test
  public void testDeleteObjectFailedForDirectoryDeletion() {
    VersionedObject dir = newDirectory();
    when(mockObjectStorage.deleteSingleObject(dir)).thenReturn(false);

    assertFalse(metadataOperations.delete(dir));

    verify(mockObjectStorage, times(1)).deleteSingleObject(dir);
    verifyZeroInteractions(mockOperationLog);
    verify(mockObjectLevelMetrics, times(1)).incrFailedDynamoDB();
  }

  @Test
  public void testDeleteObjectReturnsFalseIfOperationLogFails() {
    VersionedObject mockObj = newFile();
    when(mockOperationLog.logDeleteOperation(mockObj)).thenReturn(Optional.empty());

    assertFalse(metadataOperations.delete(mockObj));

    verify(mockOperationLog, times(1)).logDeleteOperation(mockObj);
    verifyZeroInteractions(mockObjectStorage);
    verify(mockObjectLevelMetrics, times(1)).incrFailedPendingOpLog();
  }

  @Test
  public void testDeleteObjectReturnsFalseIfStorageFails() {
    VersionedObject mockObj = newFile();
    when(mockOperationLog.logDeleteOperation(mockObj)).thenReturn(Optional.of(mockEntryHandle));
    when(mockObjectStorage.deleteSingleObject(mockObj)).thenReturn(false);
    when(mockEntryHandle.rollback()).thenReturn(true);

    assertFalse(metadataOperations.delete(mockObj));

    verify(mockOperationLog, times(1)).logDeleteOperation(mockObj);
    verify(mockObjectStorage, times(1)).deleteSingleObject(mockObj);
    verify(mockEntryHandle, times(1)).rollback();
    verify(mockObjectLevelMetrics, times(1)).incrFailedDynamoDB();
    verify(mockObjectLevelMetrics, times(1)).incrOpLogSuccessfulRollback();
  }

  @Test
  public void testDeleteObjectReturnsFalseIfStorageFailsAndOpLogRollbackFails() {
    VersionedObject mockObj = newFile();
    when(mockOperationLog.logDeleteOperation(mockObj)).thenReturn(Optional.of(mockEntryHandle));
    when(mockObjectStorage.deleteSingleObject(mockObj)).thenReturn(false);
    when(mockEntryHandle.rollback()).thenReturn(false);

    assertFalse(metadataOperations.delete(mockObj));

    verify(mockOperationLog, times(1)).logDeleteOperation(mockObj);
    verify(mockObjectStorage, times(1)).deleteSingleObject(mockObj);
    verify(mockEntryHandle, times(1)).rollback();
    verify(mockObjectLevelMetrics, times(1)).incrFailedDynamoDB();
    verify(mockObjectLevelMetrics, times(1)).incrOpLogFailedRollback();
  }

  @Test
  public void testDeleteObjectIsSuccessfulButCommitOpLogFailed() {
    VersionedObject mockObj = newFile();
    when(mockOperationLog.logDeleteOperation(mockObj)).thenReturn(Optional.of(mockEntryHandle));
    when(mockObjectStorage.deleteSingleObject(mockObj)).thenReturn(true);
    when(mockEntryHandle.commit()).thenReturn(false);

    assertTrue(metadataOperations.delete(mockObj));

    verify(mockEntryHandle, times(1)).commit();
    verify(mockObjectLevelMetrics, times(1)).incrFailedCommitOpLog();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testRenameFileThrowsErrorForDirectories() {
    metadataOperations.renameFile(newDirectory(), new Path("ks://bucket/f"));
  }

  @Test
  public void testRenameFileIsSuccessful() {
    VersionedObject src = newFile();
    Path dstPath = new Path("ks://bucket/f2");

    ObjectMetadataStorage.Transaction mockTransaction = mock(ObjectMetadataStorage.Transaction.class);
    when(mockObjectStorage.createTransaction()).thenReturn(mockTransaction);
    when(mockTransaction.commit()).thenReturn(true);

    ArgumentCaptor<VersionedObject> objectCaptor = ArgumentCaptor.forClass(VersionedObject.class);
    doAnswer(inv -> null).when(mockTransaction).addItemToStore(objectCaptor.capture());

    when(mockOperationLog.logUpdateOperation(eq(src), any(VersionedObject.class))).thenReturn(Optional.of(mockEntryHandle));

    assertTrue(metadataOperations.renameFile(src, dstPath));

    VersionedObject dst = objectCaptor.getValue();
    verify(mockOperationLog, times(1)).logUpdateOperation(src, dst);
    verify(mockTransaction, times(1)).addItemToStore(dst);
    verify(mockTransaction, times(1)).addItemToDelete(src);
    assertEquals(dstPath, dst.metadata().getKey());
    assertEquals(src.metadata().isDirectory(), dst.metadata().isDirectory());
    assertEquals(src.metadata().getSize(), dst.metadata().getSize());
    assertEquals(src.metadata().physicalDataCommitted(), dst.metadata().physicalDataCommitted());
    assertEquals(src.metadata().getPhysicalPath().get(), dst.metadata().getPhysicalPath().get());
    assertEquals(src.metadata().getCreationTime(), dst.metadata().getCreationTime());
    assertEquals(src.version() + 1, dst.version());
    verify(mockEntryHandle, times(1)).commit();
  }

  @Test
  public void testRenameFileIsSuccessfulButCommitOpLogFailed() {
    VersionedObject src = newFile();
    Path dstPath = new Path("ks://bucket/f2");

    ObjectMetadataStorage.Transaction mockTransaction = mock(ObjectMetadataStorage.Transaction.class);
    when(mockObjectStorage.createTransaction()).thenReturn(mockTransaction);
    when(mockTransaction.commit()).thenReturn(true);

    ArgumentCaptor<VersionedObject> objectCaptor = ArgumentCaptor.forClass(VersionedObject.class);
    doAnswer(inv -> null).when(mockTransaction).addItemToStore(objectCaptor.capture());

    when(mockOperationLog.logUpdateOperation(eq(src), any(VersionedObject.class))).thenReturn(Optional.of(mockEntryHandle));

    when(mockEntryHandle.commit()).thenReturn(false);

    assertTrue(metadataOperations.renameFile(src, dstPath));

    verify(mockObjectLevelMetrics, times(1)).incrFailedCommitOpLog();
  }

  @Test
  public void testRenameFileReturnsFalseIfOperationLogFails() {
    VersionedObject src = newFile();
    Path dstPath = new Path("ks://bucket/f2");
    doAnswer(inv -> Optional.empty()).when(mockOperationLog).logUpdateOperation(eq(src), any(VersionedObject.class));

    assertFalse(metadataOperations.renameFile(src, dstPath));

    verifyZeroInteractions(mockObjectStorage);
    verify(mockObjectLevelMetrics, times(1)).incrFailedPendingOpLog();
  }

  @Test
  public void testRenameFileReturnFalseIfStorageCommitFails() {
    VersionedObject src = newFile();
    Path dstPath = new Path("ks://bucket/f2");

    ObjectMetadataStorage.Transaction mockTransaction = mock(ObjectMetadataStorage.Transaction.class);
    when(mockObjectStorage.createTransaction()).thenReturn(mockTransaction);
    when(mockTransaction.commit()).thenReturn(false);

    ArgumentCaptor<VersionedObject> objectCaptor = ArgumentCaptor.forClass(VersionedObject.class);
    doAnswer(inv -> null).when(mockTransaction).addItemToStore(objectCaptor.capture());

    when(mockOperationLog.logUpdateOperation(eq(src), any(VersionedObject.class))).thenReturn(Optional.of(mockEntryHandle));
    when(mockEntryHandle.rollback()).thenReturn(true);

    assertFalse(metadataOperations.renameFile(src, dstPath));

    VersionedObject dst = objectCaptor.getValue();
    verify(mockOperationLog, times(1)).logUpdateOperation(src, dst);
    verify(mockTransaction, times(1)).addItemToStore(dst);
    verify(mockTransaction, times(1)).addItemToDelete(src);
    assertEquals(dstPath, dst.metadata().getKey());
    assertEquals(src.metadata().isDirectory(), dst.metadata().isDirectory());
    assertEquals(src.metadata().getSize(), dst.metadata().getSize());
    assertEquals(src.metadata().physicalDataCommitted(), dst.metadata().physicalDataCommitted());
    assertEquals(src.metadata().getPhysicalPath().get(), dst.metadata().getPhysicalPath().get());
    assertEquals(src.metadata().getCreationTime(), dst.metadata().getCreationTime());
    assertEquals(src.version() + 1, dst.version());
    verify(mockEntryHandle, times(1)).rollback();
    verify(mockObjectLevelMetrics, times(1)).incrFailedDynamoDB();
    verify(mockObjectLevelMetrics, times(1)).incrOpLogSuccessfulRollback();
  }

  @Test
  public void testRenameFileReturnFalseIfStorageCommitFailsAndOpLogRollbackFails() {
    VersionedObject src = newFile();
    Path dstPath = new Path("ks://bucket/f2");

    ObjectMetadataStorage.Transaction mockTransaction = mock(ObjectMetadataStorage.Transaction.class);
    when(mockObjectStorage.createTransaction()).thenReturn(mockTransaction);
    when(mockTransaction.commit()).thenReturn(false);

    ArgumentCaptor<VersionedObject> objectCaptor = ArgumentCaptor.forClass(VersionedObject.class);
    doAnswer(inv -> null).when(mockTransaction).addItemToStore(objectCaptor.capture());

    when(mockOperationLog.logUpdateOperation(eq(src), any(VersionedObject.class))).thenReturn(Optional.of(mockEntryHandle));
    when(mockEntryHandle.rollback()).thenReturn(false);

    assertFalse(metadataOperations.renameFile(src, dstPath));

    VersionedObject dst = objectCaptor.getValue();
    verify(mockOperationLog, times(1)).logUpdateOperation(src, dst);
    verify(mockTransaction, times(1)).addItemToStore(dst);
    verify(mockTransaction, times(1)).addItemToDelete(src);
    assertEquals(dstPath, dst.metadata().getKey());
    assertEquals(src.metadata().isDirectory(), dst.metadata().isDirectory());
    assertEquals(src.metadata().getSize(), dst.metadata().getSize());
    assertEquals(src.metadata().physicalDataCommitted(), dst.metadata().physicalDataCommitted());
    assertEquals(src.metadata().getPhysicalPath().get(), dst.metadata().getPhysicalPath().get());
    assertEquals(src.metadata().getCreationTime(), dst.metadata().getCreationTime());
    assertEquals(src.version() + 1, dst.version());
    verify(mockEntryHandle, times(1)).rollback();
    verify(mockObjectLevelMetrics, times(1)).incrFailedDynamoDB();
    verify(mockObjectLevelMetrics, times(1)).incrOpLogFailedRollback();
  }

  @Test
  public void testListReturnStorageList() {
    VersionedObject dir = newDirectory();
    Iterable<ObjectHandle> mockStorageResponse = mock(Iterable.class);
    doReturn(mockStorageResponse).when(mockObjectStorage).list(dir);

    assertEquals(mockStorageResponse, metadataOperations.list(dir));
  }

  @Test
  public void testScanReturnsStorageResponse() {
    Path key = new Path("ks://bucket/f");
    Iterable<ObjectHandle> mockStorageResponse = mock(Iterable.class);
    doReturn(mockStorageResponse).when(mockObjectStorage).scan(key, 0,3);

    assertEquals(mockStorageResponse, metadataOperations.scan(key, 0,3));
  }

  private VersionedObject newFile() {
    ObjectMetadata metadata = ObjectMetadata.builder()
        .key(new Path("ks://bucket/f"))
        .isDirectory(false)
        .creationTime(100)
        .size(200)
        .physicalPath("phys_path")
        .physicalDataCommitted(true)
        .build();
    VersionedObject handle = VersionedObject.builder()
        .metadata(metadata)
        .id(UUID.randomUUID())
        .version(1)
        .build();
    return handle;
  }

  private VersionedObject newDirectory() {
    ObjectMetadata metadata = ObjectMetadata.builder()
        .key(new Path("ks://bucket/d"))
        .isDirectory(true)
        .creationTime(100)
        .size(0)
        .physicalPath(Optional.empty())
        .build();
    VersionedObject handle = VersionedObject.builder()
        .metadata(metadata)
        .id(UUID.randomUUID())
        .version(1)
        .build();
    return handle;
  }
}
