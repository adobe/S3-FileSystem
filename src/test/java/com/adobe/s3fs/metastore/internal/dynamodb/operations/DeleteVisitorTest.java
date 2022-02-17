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

package com.adobe.s3fs.metastore.internal.dynamodb.operations;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import com.adobe.s3fs.metastore.api.ObjectHandle;
import com.adobe.s3fs.metastore.api.ObjectMetadata;
import com.adobe.s3fs.metastore.internal.dynamodb.versioning.VersionedObject;

import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;

public class DeleteVisitorTest {

  private DeleteVisitor deleteVisitor;

  @Mock private Function<ObjectHandle, Boolean> mockCallback;

  @Mock private MetadataOperations mockMetadataOperations;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
    deleteVisitor = new DeleteVisitor( mockMetadataOperations, mockCallback);
  }

  @Test
  public void testPreVisitDirectoryReturnsContinue() {
    ObjectMetadata metadata =
        ObjectMetadata.builder()
            .key(new Path("ks://bucket/a/b"))
            .isDirectory(true)
            .size(0)
            .creationTime(Instant.now().getEpochSecond())
            .physicalPath(Optional.empty())
            .build();
    VersionedObject directory = VersionedObject.builder()
        .metadata(metadata)
        .id(UUID.randomUUID())
        .version(1)
        .build();

    assertEquals(true, deleteVisitor.preVisitDirectoryObject(directory));

    verifyZeroInteractions(mockCallback);
  }

  @Test
  public void testVisitFileReturnsContinueIfSuccessful() {
    Path path = new Path("ks://bucket/a/b");
    ObjectMetadata metadata =
        ObjectMetadata.builder()
            .key(path)
            .isDirectory(false)
            .size(100)
            .creationTime(Instant.now().getEpochSecond())
            .physicalPath("some_path")
            .physicalDataCommitted(true)
            .build();
    VersionedObject file = VersionedObject.builder()
        .metadata(metadata)
        .id(UUID.randomUUID())
        .version(1)
        .build();
    when(mockCallback.apply(file)).thenReturn(true);
    when(mockMetadataOperations.delete(file)).thenReturn(true);

    assertEquals(true, deleteVisitor.visitFileObject(file));
    verify(mockMetadataOperations, times(1)).delete(file);
    verify(mockCallback, times(1)).apply(file);
  }

  @Test
  public void testVisitFileReturnsStopIfDeleteFromStorageFailed() {
    Path path = new Path("ks://bucket/a/b");
    ObjectMetadata metadata =
        ObjectMetadata.builder()
            .key(path)
            .isDirectory(false)
            .size(100)
            .creationTime(Instant.now().getEpochSecond())
            .physicalPath("some_path")
            .physicalDataCommitted(true)
            .build();
    VersionedObject file = VersionedObject.builder()
        .metadata(metadata)
        .id(UUID.randomUUID())
        .version(1)
        .build();
    doReturn(false).when(mockMetadataOperations).delete(eq(file));

    assertEquals(false, deleteVisitor.visitFileObject(file));

    verify(mockMetadataOperations, times(1)).delete(file);
    verifyZeroInteractions(mockCallback);
  }

  @Test
  public void testVisitFileReturnsStopIfCallbackFailed() {
    Path path = new Path("ks://bucket/a/b");
    ObjectMetadata metadata =
        ObjectMetadata.builder()
            .key(path)
            .isDirectory(false)
            .size(100)
            .creationTime(Instant.now().getEpochSecond())
            .physicalPath("some_path")
            .physicalDataCommitted(true)
            .build();
    VersionedObject file = VersionedObject.builder()
        .metadata(metadata)
        .id(UUID.randomUUID())
        .version(1)
        .build();
    doReturn(true).when(mockMetadataOperations).delete(file);
    doReturn(false).when(mockCallback).apply(file);

    assertEquals(false, deleteVisitor.visitFileObject(file));

    verify(mockMetadataOperations, times(1)).delete(file);
    verify(mockCallback, times(1)).apply(file);
  }

  @Test
  public void testPostVisitDirectoryReturnsContinueIfSuccessful() {
    ObjectMetadata metadata =
        ObjectMetadata.builder()
            .key(new Path("ks://bucket/a/b"))
            .isDirectory(true)
            .size(0)
            .creationTime(Instant.now().getEpochSecond())
            .physicalPath(Optional.empty())
            .build();
    VersionedObject directory = VersionedObject.builder()
        .metadata(metadata)
        .id(UUID.randomUUID())
        .version(1)
        .build();
    when(mockCallback.apply(directory)).thenReturn(true);
    when(mockMetadataOperations.delete(directory)).thenReturn(true);

    assertEquals(true, deleteVisitor.postVisitDirectoryObject(directory));

    verify(mockMetadataOperations, times(1)).delete(directory);
    verify(mockCallback, times(1)).apply(directory);
  }

  @Test
  public void testPostVisitDirectoryReturnsStopIfStorageFailed() {
    ObjectMetadata metadata =
        ObjectMetadata.builder()
            .key(new Path("ks://bucket/a/b"))
            .isDirectory(true)
            .size(0)
            .creationTime(Instant.now().getEpochSecond())
            .physicalPath(Optional.empty())
            .build();
    VersionedObject directory = VersionedObject.builder()
        .metadata(metadata)
        .id(UUID.randomUUID())
        .version(1)
        .build();
    when(mockCallback.apply(directory)).thenReturn(true);
    when(mockMetadataOperations.delete(directory)).thenReturn(false);

    assertEquals(false, deleteVisitor.postVisitDirectoryObject(directory));

    verify(mockMetadataOperations, times(1)).delete(directory);
    verifyZeroInteractions(mockCallback);
  }

  @Test
  public void testPostVisitDirectoryReturnsStopIfCallbackFailed() {
    ObjectMetadata metadata =
        ObjectMetadata.builder()
            .key(new Path("ks://bucket/a/b"))
            .isDirectory(true)
            .size(0)
            .creationTime(Instant.now().getEpochSecond())
            .physicalPath(Optional.empty())
            .build();
    VersionedObject directory = VersionedObject.builder()
        .metadata(metadata)
        .id(UUID.randomUUID())
        .version(1)
        .build();
    when(mockMetadataOperations.delete(directory)).thenReturn(true);
    when(mockCallback.apply(directory)).thenReturn(false);

    assertEquals(false, deleteVisitor.postVisitDirectoryObject(directory));

    verify(mockMetadataOperations, times(1)).delete(directory);
    verify(mockCallback, times(1)).apply(directory);
  }

  @Test
  public void testChildVisitorReturnSameVisitor() {
    ObjectMetadata directoryMetadata =
        ObjectMetadata.builder()
            .key(new Path("ks://bucket/a/b"))
            .isDirectory(true)
            .size(0)
            .creationTime(Instant.now().getEpochSecond())
            .physicalPath(Optional.empty())
            .build();
    VersionedObject directory = VersionedObject.builder()
        .metadata(directoryMetadata)
        .id(UUID.randomUUID())
        .version(1)
        .build();
    ObjectMetadata fileMetadata =
        ObjectMetadata.builder()
            .key(new Path("ks://bucket/a/b/f"))
            .isDirectory(false)
            .size(100)
            .creationTime(Instant.now().getEpochSecond())
            .physicalPath("some_path")
            .physicalDataCommitted(true)
            .build();
    VersionedObject file = VersionedObject.builder()
        .metadata(fileMetadata)
        .id(UUID.randomUUID())
        .version(1)
        .build();

    assertEquals(deleteVisitor, deleteVisitor.childVisitor(directory, file));
  }
}
