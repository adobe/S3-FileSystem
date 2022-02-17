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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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

public class RenameVisitorTest {

  @Mock private MetadataOperations mockMetadataOperations;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void testPreVisitCreatesTheDestinationDirectory() {
    Path destinationPath = new Path("ks://bucket/d/df");
    RenameVisitor visitor = new RenameVisitor(destinationPath, mockMetadataOperations);
    Path sourcePath = new Path("ks://bucket/d/di");
    ObjectMetadata metadata =
        ObjectMetadata.builder()
            .key(sourcePath)
            .isDirectory(true)
            .creationTime(Instant.now().getEpochSecond())
            .size(0)
            .physicalPath(Optional.empty())
            .build();
    VersionedObject sourceDir = VersionedObject.builder()
        .metadata(metadata)
        .id(UUID.randomUUID())
        .version(1)
        .build();
    VersionedObject movedDirectory = sourceDir.moveTo(destinationPath);
    when(mockMetadataOperations.store(movedDirectory)).thenReturn(true);

    assertTrue(visitor.preVisitDirectoryObject(sourceDir));
    verify(mockMetadataOperations, times(1)).store(movedDirectory);
  }

  @Test
  public void testPreVisitReturnsFalseIfItCannotCreateTheDestination() {
    Path destinationPath = new Path("ks://bucket/d/df");
    RenameVisitor visitor = new RenameVisitor(destinationPath, mockMetadataOperations);
    Path sourcePath = new Path("ks://bucket/d/di");
    ObjectMetadata metadata =
        ObjectMetadata.builder()
            .key(sourcePath)
            .isDirectory(true)
            .creationTime(Instant.now().getEpochSecond())
            .size(0)
            .physicalPath(Optional.empty())
            .build();
    VersionedObject sourceDir = VersionedObject.builder()
        .metadata(metadata)
        .id(UUID.randomUUID())
        .version(1)
        .build();
    VersionedObject movedDirectory = sourceDir.moveTo(destinationPath);
    when(mockMetadataOperations.store(movedDirectory)).thenReturn(false);

    assertFalse(visitor.preVisitDirectoryObject(sourceDir));
    verify(mockMetadataOperations, times(1)).store(movedDirectory);
  }

  @Test
  public void testVisitFileCorrectlyMovesAFile() {
    Path destinationPath = new Path("ks://bucket/d/ff");
    RenameVisitor visitor = new RenameVisitor(destinationPath, mockMetadataOperations);
    Path sourcePath = new Path("ks://bucket/d/fi");
    ObjectMetadata metadata =
        ObjectMetadata.builder()
            .key(sourcePath)
            .isDirectory(false)
            .creationTime(Instant.now().getEpochSecond())
            .size(100)
            .physicalPath(UUID.randomUUID().toString())
            .physicalDataCommitted(true)
            .build();
    VersionedObject sourceFile = VersionedObject.builder()
        .metadata(metadata)
        .id(UUID.randomUUID())
        .version(1)
        .build();
    when(mockMetadataOperations.renameFile(sourceFile, destinationPath)).thenReturn(true);

    assertTrue(visitor.visitFileObject(sourceFile));

    verify(mockMetadataOperations, times(1)).renameFile(sourceFile, destinationPath);
  }

  @Test
  public void testVisitFileReturnsFalseIfMetadataOperationReturnsFalse() {
    Path destinationPath = new Path("ks://bucket/d/ff");
    RenameVisitor visitor = new RenameVisitor(destinationPath, mockMetadataOperations);
    Path sourcePath = new Path("ks://bucket/d/fi");
    ObjectMetadata metadata =
        ObjectMetadata.builder()
            .key(sourcePath)
            .isDirectory(false)
            .creationTime(Instant.now().getEpochSecond())
            .size(100)
            .physicalPath(UUID.randomUUID().toString())
            .physicalDataCommitted(true)
            .build();
    VersionedObject sourceFile = VersionedObject.builder()
        .metadata(metadata)
        .id(UUID.randomUUID())
        .version(1)
        .build();
    VersionedObject destinationFile = sourceFile.moveTo(destinationPath);
    when(mockMetadataOperations.renameFile(sourceFile, destinationPath)).thenReturn(false);

    assertFalse(visitor.visitFileObject(sourceFile));

    verify(mockMetadataOperations, times(1)).renameFile(sourceFile, destinationPath);
  }

  @Test
  public void testPostVisitDirectoryDeletesTheSourceDirectory() {
    Path destinationPath = new Path("ks://bucket/d/df");
    RenameVisitor visitor = new RenameVisitor(destinationPath, mockMetadataOperations);
    Path sourcePath = new Path("ks://bucket/d/di");
    ObjectMetadata metadata =
        ObjectMetadata.builder()
            .key(sourcePath)
            .isDirectory(true)
            .creationTime(Instant.now().getEpochSecond())
            .size(0)
            .physicalPath(Optional.empty())
            .build();
    VersionedObject sourceDir = VersionedObject.builder()
        .metadata(metadata)
        .id(UUID.randomUUID())
        .version(1)
        .build();
    when(mockMetadataOperations.delete(sourceDir)).thenReturn(true);

    assertTrue(visitor.postVisitDirectoryObject(sourceDir));

    verify(mockMetadataOperations, times(1)).delete(sourceDir);
  }

  @Test
  public void testPostVisitDirectoryReturnsFalseIfItCannotDeleteTheSourceDir() {
    Path destinationPath = new Path("ks://bucket/d/df");
    RenameVisitor visitor = new RenameVisitor(destinationPath, mockMetadataOperations);
    Path sourcePath = new Path("ks://bucket/d/di");
    ObjectMetadata metadata =
        ObjectMetadata.builder()
            .key(sourcePath)
            .isDirectory(true)
            .creationTime(Instant.now().getEpochSecond())
            .size(0)
            .physicalPath(Optional.empty())
            .build();
    VersionedObject sourceDir = VersionedObject.builder()
        .metadata(metadata)
        .id(UUID.randomUUID())
        .version(1)
        .build();
    when(mockMetadataOperations.delete(sourceDir)).thenReturn(false);

    assertFalse(visitor.postVisitDirectoryObject(sourceDir));

    verify(mockMetadataOperations, times(1)).delete(sourceDir);
  }

  @Test
  public void testChildVisitorIsCreatedCorrectly() {
    Path destinationPath = new Path("ks://bucket/d/d2");
    RenameVisitor visitor = new RenameVisitor(destinationPath, mockMetadataOperations);
    ObjectMetadata dirMetadata =
        ObjectMetadata.builder()
            .key(new Path("ks://bucket/d/d1/"))
            .isDirectory(true)
            .creationTime(Instant.now().getEpochSecond())
            .size(0)
            .physicalPath(Optional.empty())
            .build();
    VersionedObject dir = VersionedObject.builder()
        .metadata(dirMetadata)
        .version(1)
        .id(UUID.randomUUID())
        .build();
    ObjectMetadata childMetadata =
        ObjectMetadata.builder()
            .key(new Path("ks://bucket/d/d1/f"))
            .isDirectory(false)
            .creationTime(Instant.now().getEpochSecond())
            .size(100)
            .physicalPath(UUID.randomUUID().toString())
            .physicalDataCommitted(true)
            .build();
    VersionedObject child = VersionedObject.builder()
        .metadata(childMetadata)
        .id(UUID.randomUUID())
        .version(1)
        .build();

    RenameVisitor childVisitor = (RenameVisitor) visitor.childVisitor(dir, child);

    assertEquals(new Path("ks://bucket/d/d2/f"), childVisitor.getDestinationPath());
  }
}
