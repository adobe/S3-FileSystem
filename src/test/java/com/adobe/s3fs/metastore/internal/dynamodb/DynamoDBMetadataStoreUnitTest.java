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

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

import com.adobe.s3fs.metastore.api.ObjectHandle;
import com.adobe.s3fs.metastore.api.ObjectMetadata;
import com.adobe.s3fs.metastore.internal.dynamodb.operations.MetadataOperations;
import com.adobe.s3fs.metastore.internal.dynamodb.operations.MetadataTreeOperations;
import com.adobe.s3fs.metastore.internal.dynamodb.versioning.VersionedObject;

import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Optional;
import java.util.UUID;

public class DynamoDBMetadataStoreUnitTest {

  private DynamoDBMetadataStore metadataStore;

  @Mock private MetadataTreeOperations mockMetadataTreeOperations;
  @Mock private MetadataOperations mockMetadataOperations;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
    metadataStore = new DynamoDBMetadataStore(mockMetadataOperations, mockMetadataTreeOperations);
  }

  @Test
  public void testStoreObjectIsSuccessfulForFiles() {
    Path key = new Path("ks://bucket/d/f");
    ObjectMetadata object = ObjectMetadata.builder()
        .key(key)
        .isDirectory(false)
        .creationTime(100)
        .size(500)
        .physicalPath(UUID.randomUUID().toString())
        .physicalDataCommitted(true)
        .build();
    ArgumentCaptor<VersionedObject> handleCaptor = ArgumentCaptor.forClass(VersionedObject.class);
    doReturn(true).when(mockMetadataOperations).store(handleCaptor.capture());

    VersionedObject handle = (VersionedObject) metadataStore.createObject(object);

    verify(mockMetadataOperations, times(1)).store(handleCaptor.getValue());
    assertEquals(handle, handleCaptor.getValue());
    assertEquals(object, handle.metadata());
    assertEquals(1, handle.version());
  }

  @Test
  public void testStoreObjectThrowsErrorForFilesIfStorageFails() {
    Path key = new Path("ks://bucket/d/f");
    ObjectMetadata object = ObjectMetadata.builder()
        .key(key)
        .isDirectory(false)
        .creationTime(100)
        .size(500)
        .physicalPath(UUID.randomUUID().toString())
        .physicalDataCommitted(true)
        .build();
    ArgumentCaptor<VersionedObject> handleCaptor = ArgumentCaptor.forClass(VersionedObject.class);
    doReturn(false).when(mockMetadataOperations).store(handleCaptor.capture());

    try {
      ObjectHandle ignored = metadataStore.createObject(object);
      assertTrue("Error should be raised", false);
    } catch (Exception e) {
      // ignored
    }

    verify(mockMetadataOperations, times(1)).store(handleCaptor.getValue());
  }

  @Test
  public void testStoreObjectIsSuccessfulForDirectories() {
    Path key = new Path("ks://bucket/d/d1");
    ObjectMetadata object = ObjectMetadata.builder()
        .key(key)
        .isDirectory(true)
        .creationTime(100)
        .size(0)
        .build();
    ArgumentCaptor<VersionedObject> handleCaptor = ArgumentCaptor.forClass(VersionedObject.class);
    doReturn(true).when(mockMetadataOperations).store(handleCaptor.capture());

    VersionedObject handle = (VersionedObject) metadataStore.createObject(object);

    verify(mockMetadataOperations, times(1)).store(handleCaptor.getValue());
    assertEquals(handle, handleCaptor.getValue());
    assertEquals(object, handle.metadata());
    assertEquals(1, handle.version());
  }

  @Test
  public void testStoreObjectThrowsErrorForDirectoriesIfStorageFails() {
    Path key = new Path("ks://bucket/d/d1");
    ObjectMetadata object = ObjectMetadata.builder()
        .key(key)
        .isDirectory(true)
        .creationTime(100)
        .size(0)
        .build();
    ArgumentCaptor<VersionedObject> handleCaptor = ArgumentCaptor.forClass(VersionedObject.class);
    doReturn(false).when(mockMetadataOperations).store(handleCaptor.capture());

    try {
      ObjectHandle ignored = metadataStore.createObject(object);
      assertTrue("Error should be raised", false);
    } catch (Exception e) {
      // ignored
    }

    verify(mockMetadataOperations, times(1)).store(handleCaptor.getValue());
  }

  @Test
  public void testRenameObjectOnSuccessfulCase() {
    ObjectMetadata object = ObjectMetadata.builder()
        .key(new Path("ks://bucket/d/f"))
        .isDirectory(false)
        .creationTime(100)
        .size(500)
        .physicalPath(UUID.randomUUID().toString())
        .physicalDataCommitted(true)
        .build();
    Path destinationKey = new Path("ks://bucket/d/f1");
    VersionedObject handle = VersionedObject.builder()
        .metadata(object)
        .id(UUID.randomUUID())
        .version(1)
        .build();
    doReturn(true).when(mockMetadataOperations).renameFile(handle, destinationKey);

    assertTrue(metadataStore.renameObject(handle, destinationKey));
    verifyZeroInteractions(mockMetadataTreeOperations);
  }

  @Test
  public void testRenameSingleObjectWhenCommitFails() {
    ObjectMetadata object = ObjectMetadata.builder()
        .key(new Path("ks://bucket/d/f"))
        .isDirectory(false)
        .creationTime(100)
        .size(500)
        .physicalPath(UUID.randomUUID().toString())
        .physicalDataCommitted(true)
        .build();
    Path destinationKey = new Path("ks://bucket/d/f1");
    VersionedObject handle = VersionedObject.builder()
        .metadata(object)
        .id(UUID.randomUUID())
        .version(1)
        .build();
    doReturn(false).when(mockMetadataOperations).renameFile(handle, destinationKey);

    assertFalse(metadataStore.renameObject(handle, destinationKey));

    verifyZeroInteractions(mockMetadataTreeOperations);
  }

  @Test
  public void testRenameSingleObjectWhenRuntimeErrorOccurs() {
    ObjectMetadata object = ObjectMetadata.builder()
        .key(new Path("ks://bucket/d/f"))
        .isDirectory(false)
        .creationTime(100)
        .size(500)
        .physicalPath(UUID.randomUUID().toString())
        .physicalDataCommitted(true)
        .build();
    Path destinationKey = new Path("ks://bucket/d/f1");
    VersionedObject handle = VersionedObject.builder()
        .metadata(object)
        .id(UUID.randomUUID())
        .version(1)
        .build();
    doThrow(new RuntimeException()).when(mockMetadataOperations).renameFile(handle, destinationKey);

    try {
     boolean ignored = metadataStore.renameObject(handle, destinationKey);
     fail();
    } catch (Exception e) {
      // nothing to do
    }

    verifyZeroInteractions(mockMetadataTreeOperations);
  }

  @Test
  public void testRenameDirectoryIsSuccessful() {
    ObjectMetadata object = ObjectMetadata.builder()
        .key(new Path("ks://bucket/d/di"))
        .isDirectory(true)
        .creationTime(100)
        .size(0)
        .physicalPath(Optional.empty())
        .build();
    VersionedObject handle = VersionedObject.builder()
        .metadata(object)
        .id(UUID.randomUUID())
        .version(1)
        .build();
    Path destinationKey = new Path("ks://bucket/d/df");
    doReturn(true).when(mockMetadataTreeOperations).renameObjectTree(eq(handle), eq(destinationKey));

    assertTrue(metadataStore.renameObject(handle, destinationKey));

    verifyZeroInteractions(mockMetadataOperations);
  }

  @Test
  public void testRenameDirectoryOnFailure() {
    ObjectMetadata object = ObjectMetadata.builder()
        .key(new Path("ks://bucket/d/di"))
        .isDirectory(true)
        .creationTime(100)
        .size(0)
        .physicalPath(Optional.empty())
        .build();
    VersionedObject handle = VersionedObject.builder()
        .metadata(object)
        .id(UUID.randomUUID())
        .version(1)
        .build();
    Path destinationKey = new Path("ks://bucket/d/df");
    doReturn(false).when(mockMetadataTreeOperations).renameObjectTree(eq(handle), eq(destinationKey));

    try {
      metadataStore.renameObject(handle, destinationKey);
      assertTrue("Error shoud be thrown", false);
    } catch (Exception e) {
      // ignored
    }

    verifyZeroInteractions(mockMetadataOperations);
  }

  @Test
  public void testDeleteDirectoryOnSuccessfulCase() {
    ObjectMetadata object = ObjectMetadata.builder()
        .key(new Path("ks://bucket/d"))
        .isDirectory(true)
        .creationTime(100)
        .size(0)
        .physicalDataCommitted(false)
        .build();
    VersionedObject versionedObject = VersionedObject.builder()
        .metadata(object)
        .id(UUID.randomUUID())
        .version(1)
        .build();
    doReturn(true).when(mockMetadataTreeOperations).deleteObjectTree(eq(versionedObject), any());

    assertTrue(metadataStore.deleteObject(versionedObject, o -> true));

    verifyZeroInteractions(mockMetadataOperations);
  }

  @Test
  public void testDeleteDirectoryOnFailure() {
    ObjectMetadata object = ObjectMetadata.builder()
        .key(new Path("ks://bucket/d"))
        .isDirectory(true)
        .creationTime(100)
        .size(0)
        .physicalDataCommitted(false)
        .build();
    VersionedObject versionedObject = VersionedObject.builder()
        .metadata(object)
        .id(UUID.randomUUID())
        .version(1)
        .build();
    doReturn(false).when(mockMetadataTreeOperations).deleteObjectTree(eq(versionedObject), any());

    try {
      metadataStore.deleteObject(versionedObject, o -> true);
      assertTrue("Error shoud be thrown", false);
    } catch (Exception e) {
      // ignored
    }
  }

  @Test
  public void testDeleteObjectOnSuccessfulCase() {
    ObjectMetadata object = ObjectMetadata.builder()
        .key(new Path("ks://bucket/d/f"))
        .isDirectory(false)
        .creationTime(100)
        .size(500)
        .physicalPath(UUID.randomUUID().toString())
        .physicalDataCommitted(true)
        .build();
    VersionedObject handle = VersionedObject.builder()
        .metadata(object)
        .id(UUID.randomUUID())
        .version(1)
        .build();
    doReturn(true).when(mockMetadataOperations).delete(eq(handle));

    assertTrue(metadataStore.deleteObject(handle, o -> true));

    verifyZeroInteractions(mockMetadataTreeOperations);
  }

  @Test
  public void testDeleteObjectOnFailure() {
    ObjectMetadata object = ObjectMetadata.builder()
        .key(new Path("ks://bucket/d/f"))
        .isDirectory(false)
        .creationTime(100)
        .size(500)
        .physicalPath(UUID.randomUUID().toString())
        .physicalDataCommitted(true)
        .build();
    VersionedObject handle = VersionedObject.builder()
        .metadata(object)
        .id(UUID.randomUUID())
        .version(1)
        .build();
    doReturn(false).when(mockMetadataTreeOperations).deleteObjectTree(eq(handle), any());

    try {
      metadataStore.deleteObject(handle, o -> true);
      assertTrue("Error shoud be thrown", false);
    } catch (Exception e) {
      // ignored
    }
  }

  @Test
  public void testListing() {
    ObjectMetadata dir = ObjectMetadata.builder()
        .key(new Path("ks://bucket/d"))
        .isDirectory(true)
        .creationTime(100)
        .size(0)
        .physicalPath(Optional.empty())
        .build();
    VersionedObject handle = VersionedObject.builder()
        .metadata(dir)
        .id(UUID.randomUUID())
        .version(1)
        .build();
    Iterable<? extends ObjectHandle> mockResponse = mock(Iterable.class);
    doReturn(mockResponse).when(mockMetadataOperations).list(handle);

    assertEquals(mockResponse, metadataStore.listChildObjects(handle));
  }

  @Test
  public void testScan() {
    Path key = new Path("ks://bucket/f");
    Iterable<? extends ObjectHandle> mockResponse = mock(Iterable.class);
    doReturn(mockResponse).when(mockMetadataOperations).scan(key, 1,3);

    assertEquals(mockResponse, metadataStore.scanPartition(key, 1, 3));
  }
}
