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

import com.adobe.s3fs.metastore.api.ObjectMetadata;
import com.adobe.s3fs.metastore.internal.dynamodb.hashing.KeyOperations;
import com.adobe.s3fs.metastore.internal.dynamodb.storage.DynamoDBItem;
import com.adobe.s3fs.metastore.internal.dynamodb.storage.DynamoDBStorage;
import com.adobe.s3fs.metastore.internal.dynamodb.storage.HashingAwareObjectStorage;
import com.adobe.s3fs.metastore.internal.dynamodb.versioning.VersionedObject;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.*;

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

public class HashingAwareObjectStorageTest {

  @Mock
  private KeyOperations mockKeyOperations;

  @Mock
  private DynamoDBStorage mockDynamoDBStorage;

  private HashingAwareObjectStorage objectStorage;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
    objectStorage = new HashingAwareObjectStorage(mockKeyOperations, mockDynamoDBStorage);
  }

  @Test
  public void testGetObjectIsSuccessfulForFiles() {
    Path key = new Path("ks://bucket/d/f");
    when(mockKeyOperations.logicalKeyToHashKey(key)).thenReturn("hash");
    when(mockKeyOperations.logicalKeyToSortKey(key)).thenReturn("sort");
    DynamoDBItem item = DynamoDBItem.builder()
        .hashKey("hash")
        .sortKey("sort")
        .isDirectory(false)
        .creationTime(100)
        .size(400)
        .physicalPath("phys_path")
        .physicalDataCommitted(true)
        .id(UUID.randomUUID())
        .version(1)
        .build();
    doReturn(Optional.of(item)).when(mockDynamoDBStorage).getItem(eq("hash"), eq("sort"));
    doReturn(Optional.of(key)).when(mockKeyOperations).hashAndSortKeyToLogicalKey(eq("hash"), eq("sort"));

    Optional<VersionedObject> result = objectStorage.getObject(key);

    assertTrue(result.isPresent());
    assertVersionedObjectMatchesDynamoItem(result.get(), item);
  }

  @Test
  public void testGetObjectReturnsEmptyOnMissingItemForFiles() {
    Path key = new Path("ks://bucket/d/f");
    when(mockKeyOperations.logicalKeyToHashKey(key)).thenReturn("hash");
    when(mockKeyOperations.logicalKeyToSortKey(key)).thenReturn("sort");
    doReturn(Optional.empty()).when(mockDynamoDBStorage).getItem(eq("hash"), eq("sort"));

    Optional<VersionedObject> result = objectStorage.getObject(key);

    assertFalse(result.isPresent());
  }

  @Test
  public void testGetObjectIsSuccessfulForDirectories() {
    Path key = new Path("ks://bucket/d/d1");
    when(mockKeyOperations.logicalKeyToHashKey(key)).thenReturn("hash");
    when(mockKeyOperations.logicalKeyToSortKey(key)).thenReturn("sort");
    DynamoDBItem item = DynamoDBItem.builder()
        .hashKey("hash")
        .sortKey("sort")
        .isDirectory(true)
        .creationTime(100)
        .size(0)
        .physicalPath(Optional.empty())
        .id(UUID.randomUUID())
        .version(1)
        .build();
    doReturn(Optional.of(item)).when(mockDynamoDBStorage).getItem(eq("hash"), eq("sort"));
    doReturn(Optional.of(key)).when(mockKeyOperations).hashAndSortKeyToLogicalKey(eq("hash"), eq("sort"));

    Optional<VersionedObject> result = objectStorage.getObject(key);

    assertTrue(result.isPresent());
    assertVersionedObjectMatchesDynamoItem(result.get(), item);
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
    VersionedObject versionedObject = VersionedObject.builder()
        .metadata(object)
        .id(UUID.randomUUID())
        .version(1)
        .build();
    ArgumentCaptor<DynamoDBItem> itemCaptor = ArgumentCaptor.forClass(DynamoDBItem.class);
    doAnswer(it -> null).when(mockDynamoDBStorage).putItem(itemCaptor.capture());
    when(mockKeyOperations.logicalKeyToHashKey(key)).thenReturn("hash");
    when(mockKeyOperations.logicalKeyToSortKey(key)).thenReturn("sort");

    assertTrue(objectStorage.storeSingleObject(versionedObject));

    assertVersionedObjectMatchesDynamoItem(versionedObject, itemCaptor.getValue());
    assertEquals("hash", itemCaptor.getValue().getHashKey());
    assertEquals("sort", itemCaptor.getValue().getSortKey());
    verify(mockDynamoDBStorage, times(1)).putItem(any(DynamoDBItem.class));
  }

  @Test
  public void testStoreReturnsFalseErrorForFilesIfStorageFails() {
    Path key = new Path("ks://bucket/d/f");
    ObjectMetadata object = ObjectMetadata.builder()
        .key(key)
        .isDirectory(false)
        .creationTime(100)
        .size(500)
        .physicalPath(UUID.randomUUID().toString())
        .physicalDataCommitted(true)
        .build();
    VersionedObject versionedObject = VersionedObject.builder()
        .metadata(object)
        .id(UUID.randomUUID())
        .version(1)
        .build();
    ArgumentCaptor<DynamoDBItem> itemCaptor = ArgumentCaptor.forClass(DynamoDBItem.class);
    doThrow(new RuntimeException()).when(mockDynamoDBStorage).putItem(itemCaptor.capture());
    when(mockKeyOperations.logicalKeyToHashKey(key)).thenReturn("hash");
    when(mockKeyOperations.logicalKeyToSortKey(key)).thenReturn("sort");

    assertFalse(objectStorage.storeSingleObject(versionedObject));

    assertVersionedObjectMatchesDynamoItem(versionedObject, itemCaptor.getValue());
    assertEquals("hash", itemCaptor.getValue().getHashKey());
    assertEquals("sort", itemCaptor.getValue().getSortKey());
    verify(mockDynamoDBStorage, times(1)).putItem(any(DynamoDBItem.class));
  }

  @Test
  public void testDeleteObjectIsSuccessfulForFiles() {
    Path key = new Path("ks://bucket/d/f");
    ObjectMetadata object = ObjectMetadata.builder()
        .key(key)
        .isDirectory(false)
        .creationTime(100)
        .size(500)
        .physicalPath(UUID.randomUUID().toString())
        .physicalDataCommitted(true)
        .build();
    VersionedObject versionedObject = VersionedObject.builder()
        .metadata(object)
        .id(UUID.randomUUID())
        .version(1)
        .build();
    doAnswer(it -> null).when(mockDynamoDBStorage).deleteItem(eq("hash"), eq("sort"));
    when(mockKeyOperations.logicalKeyToHashKey(key)).thenReturn("hash");
    when(mockKeyOperations.logicalKeyToSortKey(key)).thenReturn("sort");

    assertTrue(objectStorage.deleteSingleObject(versionedObject));

    verify(mockDynamoDBStorage, times(1)).deleteItem(eq("hash"), eq("sort"));
  }

  @Test
  public void testDeleteObjectReturnsFalseForFiles() {
    Path key = new Path("ks://bucket/d/f");
    ObjectMetadata object = ObjectMetadata.builder()
        .key(key)
        .isDirectory(false)
        .creationTime(100)
        .size(500)
        .physicalPath(UUID.randomUUID().toString())
        .physicalDataCommitted(true)
        .build();
    VersionedObject versionedObject = VersionedObject.builder()
        .metadata(object)
        .id(UUID.randomUUID())
        .version(1)
        .build();
    doThrow(new RuntimeException()).when(mockDynamoDBStorage).deleteItem(eq("hash"), eq("sort"));
    when(mockKeyOperations.logicalKeyToHashKey(key)).thenReturn("hash");
    when(mockKeyOperations.logicalKeyToSortKey(key)).thenReturn("sort");

    assertFalse(objectStorage.deleteSingleObject(versionedObject));

    verify(mockDynamoDBStorage, times(1)).deleteItem(eq("hash"), eq("sort"));
  }

  @Test
  public void testStoreObjectIsSuccessfulForDirectories() {
    Path key = new Path("ks://bucket/d/d1");
    ObjectMetadata object = ObjectMetadata.builder()
        .key(key)
        .isDirectory(true)
        .creationTime(100)
        .size(0)
        .physicalPath(Optional.empty())
        .build();
    VersionedObject versionedObject = VersionedObject.builder()
        .metadata(object)
        .id(UUID.randomUUID())
        .version(1)
        .build();
    ArgumentCaptor<DynamoDBItem> itemCaptor = ArgumentCaptor.forClass(DynamoDBItem.class);
    doAnswer(it -> null).when(mockDynamoDBStorage).putItem(itemCaptor.capture());
    when(mockKeyOperations.logicalKeyToHashKey(key)).thenReturn("hash");
    when(mockKeyOperations.logicalKeyToSortKey(key)).thenReturn("sort");

    assertTrue(objectStorage.storeSingleObject(versionedObject));

    assertVersionedObjectMatchesDynamoItem(versionedObject, itemCaptor.getValue());
    assertEquals("hash", itemCaptor.getValue().getHashKey());
    assertEquals("sort", itemCaptor.getValue().getSortKey());
    verify(mockDynamoDBStorage, times(1)).putItem(any(DynamoDBItem.class));

  }

  @Test
  public void testStoreObjectReturnsFalseForDirectoriesIfStorageFails() {
    Path key = new Path("ks://bucket/d/d1");
    ObjectMetadata object = ObjectMetadata.builder()
        .key(key)
        .isDirectory(true)
        .creationTime(100)
        .size(0)
        .physicalPath(Optional.empty())
        .build();
    VersionedObject versionedObject = VersionedObject.builder()
        .metadata(object)
        .id(UUID.randomUUID())
        .version(1)
        .build();
    ArgumentCaptor<DynamoDBItem> itemCaptor = ArgumentCaptor.forClass(DynamoDBItem.class);
    doThrow(new RuntimeException()).when(mockDynamoDBStorage).putItem(itemCaptor.capture());
    when(mockKeyOperations.logicalKeyToHashKey(key)).thenReturn("hash");
    when(mockKeyOperations.logicalKeyToSortKey(key)).thenReturn("sort");

    assertFalse(objectStorage.storeSingleObject(versionedObject));

    assertVersionedObjectMatchesDynamoItem(versionedObject, itemCaptor.getValue());
    assertEquals("hash", itemCaptor.getValue().getHashKey());
    assertEquals("sort", itemCaptor.getValue().getSortKey());
    verify(mockDynamoDBStorage, times(1)).putItem(any(DynamoDBItem.class));
  }

  @Test
  public void testDeleteObjectIsSuccessfulForDirectories() {
    Path key = new Path("ks://bucket/d/d1");
    ObjectMetadata object = ObjectMetadata.builder()
        .key(key)
        .isDirectory(true)
        .creationTime(100)
        .size(0)
        .physicalPath(Optional.empty())
        .build();
    VersionedObject versionedObject = VersionedObject.builder()
        .metadata(object)
        .id(UUID.randomUUID())
        .version(1)
        .build();
    doAnswer(it -> null).when(mockDynamoDBStorage).deleteItem(eq("hash"), eq("sort"));
    when(mockKeyOperations.logicalKeyToHashKey(key)).thenReturn("hash");
    when(mockKeyOperations.logicalKeyToSortKey(key)).thenReturn("sort");

    assertTrue(objectStorage.deleteSingleObject(versionedObject));

    verify(mockDynamoDBStorage, times(1)).deleteItem(eq("hash"), eq("sort"));
  }

  @Test
  public void testDeleteObjectReturnsFalseForDirectories() {
    Path key = new Path("ks://bucket/d/d1");
    ObjectMetadata object = ObjectMetadata.builder()
        .key(key)
        .isDirectory(true)
        .creationTime(100)
        .size(0)
        .physicalPath(Optional.empty())
        .build();
    VersionedObject versionedObject = VersionedObject.builder()
        .metadata(object)
        .id(UUID.randomUUID())
        .version(1)
        .build();
    doThrow(new RuntimeException()).when(mockDynamoDBStorage).deleteItem(eq("hash"), eq("sort"));
    when(mockKeyOperations.logicalKeyToHashKey(key)).thenReturn("hash");
    when(mockKeyOperations.logicalKeyToSortKey(key)).thenReturn("sort");

    assertFalse(objectStorage.deleteSingleObject(versionedObject));

    verify(mockDynamoDBStorage, times(1)).deleteItem(eq("hash"), eq("sort"));
  }

  @Test
  public void testListing() {
    Path rootKey = new Path("ks://bucket/d");
    ObjectMetadata rootObject = ObjectMetadata.builder()
        .key(rootKey)
        .isDirectory(true)
        .creationTime(111)
        .size(0)
        .physicalPath(Optional.empty())
        .build();
    VersionedObject rootVersioned = VersionedObject.builder()
        .metadata(rootObject)
        .id(UUID.randomUUID())
        .version(1)
        .build();
    ObjectMetadata expectedChild1 = ObjectMetadata.builder()
        .key(new Path("ks://bucket/d/f1"))
        .isDirectory(false)
        .creationTime(100)
        .size(200)
        .physicalPath("phys_path_1")
        .build();
    UUID child1ID = UUID.randomUUID();
    VersionedObject child1Versioned = VersionedObject.builder()
        .metadata(expectedChild1)
        .id(child1ID)
        .version(1)
        .build();
    ObjectMetadata expectedChild2 = ObjectMetadata.builder()
        .key(new Path("ks://bucket/d/f2"))
        .isDirectory(true)
        .creationTime(50)
        .size(0)
        .physicalPath(Optional.empty())
        .build();
    UUID child2ID = UUID.randomUUID();
    VersionedObject child2Versioned = VersionedObject.builder()
        .metadata(expectedChild2)
        .id(child2ID)
        .version(1)
        .build();
    DynamoDBItem item1 = DynamoDBItem.builder()
        .hashKey("hash1")
        .sortKey("sort1")
        .isDirectory(false)
        .creationTime(100)
        .size(200)
        .physicalPath("phys_path_1")
        .version(1)
        .id(child1ID)
        .build();
    DynamoDBItem item2 = DynamoDBItem.builder()
        .hashKey("hash2")
        .sortKey("sort2")
        .isDirectory(true)
        .creationTime(50)
        .size(0)
        .physicalPath(Optional.empty())
        .version(1)
        .id(child2ID)
        .build();

    when(mockKeyOperations.logicalKeyToAllPossibleHashKeys(rootKey)).thenReturn(Arrays.asList("hash1", "hash2"));
    when(mockKeyOperations.hashAndSortKeyToLogicalKey("hash1", "sort1"))
        .thenReturn(Optional.of(new Path("ks://bucket/d/f1")));
    when(mockKeyOperations.hashAndSortKeyToLogicalKey("hash2", "sort2"))
        .thenReturn(Optional.of(new Path("ks://bucket/d/f2")));
    when(mockDynamoDBStorage.list("hash1")).thenReturn(Arrays.asList(item1));
    when(mockDynamoDBStorage.list("hash2")).thenReturn(Arrays.asList(item2));

    Set<VersionedObject> result = Sets.newHashSet(objectStorage.list(rootVersioned));

    assertEquals(Sets.newHashSet(child1Versioned, child2Versioned), result);
  }

  @Test
  public void testScan() {
    Path rootKey = new Path("ks://bucket/d");
    ObjectMetadata expectedChild1 = ObjectMetadata.builder()
        .key(new Path("ks://bucket/d/f1"))
        .isDirectory(false)
        .creationTime(100)
        .size(200)
        .physicalPath("phys_path_1")
        .build();
    UUID child1ID = UUID.randomUUID();
    VersionedObject child1Versioned = VersionedObject.builder()
        .metadata(expectedChild1)
        .id(child1ID)
        .version(1)
        .build();
    ObjectMetadata expectedChild2 = ObjectMetadata.builder()
        .key(new Path("ks://bucket/d/f2"))
        .isDirectory(true)
        .creationTime(50)
        .size(0)
        .physicalPath(Optional.empty())
        .build();
    UUID child2ID = UUID.randomUUID();
    VersionedObject child2Versioned = VersionedObject.builder()
        .metadata(expectedChild2)
        .id(child2ID)
        .version(1)
        .build();

    DynamoDBItem item1 = DynamoDBItem.builder()
        .hashKey("hash1")
        .sortKey("sort1")
        .isDirectory(false)
        .creationTime(100)
        .size(200)
        .physicalPath("phys_path_1")
        .version(1)
        .id(child1ID)
        .build();
    DynamoDBItem item2 = DynamoDBItem.builder()
        .hashKey("hash2")
        .sortKey("sort2")
        .isDirectory(true)
        .creationTime(50)
        .size(0)
        .physicalPath(Optional.empty())
        .id(child2ID)
        .version(1)
        .build();
    DynamoDBItem item3 = DynamoDBItem.builder()
        .hashKey("hash3")
        .sortKey("sort3")
        .isDirectory(true)
        .creationTime(222)
        .id(UUID.randomUUID())
        .version(1)
        .size(0)
        .physicalPath(Optional.empty())
        .build();

    when(mockKeyOperations.logicalKeyToAllPossibleHashKeys(rootKey)).thenReturn(Arrays.asList("hash1", "hash2"));
    when(mockKeyOperations.hashAndSortKeyToLogicalKey("hash1", "sort1"))
        .thenReturn(Optional.of(new Path("ks://bucket/d/f1")));
    when(mockKeyOperations.hashAndSortKeyToLogicalKey("hash2", "sort2"))
        .thenReturn(Optional.of(new Path("ks://bucket/d/f2")));
    when(mockKeyOperations.hashAndSortKeyToLogicalKey("hash3", "sort3"))
        .thenReturn(Optional.of(new Path("ks://bucket/d")));
    when(mockDynamoDBStorage.scan(0, 2)).thenReturn(Arrays.asList(item1));
    when(mockDynamoDBStorage.scan(1, 2)).thenReturn(Arrays.asList(item2, item3));

    List<VersionedObject> result = Lists.newArrayList(
        Iterables.concat(objectStorage.scan(rootKey, 0,2), objectStorage.scan(rootKey, 1, 2)));
    Set<VersionedObject> resultSet = Sets.newHashSet(result);

    assertEquals(Sets.newHashSet(child1Versioned, child2Versioned), resultSet);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testScanThrowsErrorForInvalidPartitionCount() {
    Iterable<VersionedObject> ignored = objectStorage.scan(new Path("ks://bucket/d"),0, 0);
  }

  private static void assertVersionedObjectMatchesDynamoItem(VersionedObject versionedObject, DynamoDBItem item) {
    ObjectMetadata objectMetadata = versionedObject.metadata();
    assertEquals(objectMetadata.isDirectory(), item.isDirectory());
    assertEquals(objectMetadata.getCreationTime(), item.getCreationTime());
    assertEquals(objectMetadata.getSize(), item.getSize());
    if (objectMetadata.isDirectory()) {
      assertFalse(objectMetadata.getPhysicalPath().isPresent());
      assertFalse(item.getPhysicalPath().isPresent());
      assertFalse(objectMetadata.physicalDataCommitted());
      assertFalse(item.physicalDataCommitted());
    } else {
      assertEquals(objectMetadata.getPhysicalPath().get(), item.getPhysicalPath().get());
      assertEquals(objectMetadata.physicalDataCommitted(), item.physicalDataCommitted());
    }
    assertEquals(versionedObject.id(), item.id());
    assertEquals(versionedObject.version(), item.version());
  }
}
