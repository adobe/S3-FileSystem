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

package com.adobe.s3fs.filesystem;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import com.adobe.s3fs.common.exceptions.UncommittedFileException;
import com.adobe.s3fs.metastore.api.MetadataStore;
import com.adobe.s3fs.metastore.api.ObjectHandle;
import com.adobe.s3fs.metastore.api.ObjectMetadata;
import com.adobe.s3fs.storage.api.PathTranslator;
import com.adobe.s3fs.storage.api.PhysicalStorage;

import com.google.common.collect.Lists;

import junit.framework.AssertionFailedError;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

public class FileSystemImplementationTest {

  @Mock
  private MetadataStore mockMetadataStore;

  @Mock
  private PhysicalStorage mockPhysicalStorage;

  @Mock
  private PathTranslator mockPathTranslator;

  @Mock
  private FileSystemMetrics mockFsMetrics;

  private Random random = new Random(0);

  private FileSystemImplementation fileSystemImplementation;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
    fileSystemImplementation =
        new FileSystemImplementation(
            mockMetadataStore, mockPhysicalStorage, mockPathTranslator, mockFsMetrics);
  }

  @Test
  public void testGetFileStatusForFile() throws IOException {
    Path path = new Path("s3://bucket/file");
    Path metaPath = new Path("ks://bucket/file");
    ObjectHandle objectMetadata = newFile(metaPath);
    doReturn(Optional.of(objectMetadata)).when(mockMetadataStore).getObject(metaPath);

    FileStatus fileStatus = fileSystemImplementation.getFileStatus(path);

    assertEquals(path, fileStatus.getPath());
    assertFileStatusSameAsObjectMetadata(fileStatus, objectMetadata.metadata());
  }

  @Test(expected = FileNotFoundException.class)
  public void testGetFileStatusThrowErrorForInexistentFile() throws IOException {
    Path path = new Path("s3://bucket/file");
    Path metaPath = new Path("ks://bucket/file");
    when(mockMetadataStore.getObject(metaPath)).thenReturn(Optional.empty());

    FileStatus ignored = fileSystemImplementation.getFileStatus(path);
  }

  @Test
  public void testGetFileStatusForDirectory() throws IOException {
    Path path = new Path("s3://bucket/dir");
    Path metaPath = new Path("ks://bucket/dir");
    ObjectHandle objectMetadata = newDirectory(metaPath);
    doReturn(Optional.of(objectMetadata)).when(mockMetadataStore).getObject(metaPath);

    FileStatus fileStatus = fileSystemImplementation.getFileStatus(path);

    assertEquals(path, fileStatus.getPath());
    assertFileStatusSameAsObjectMetadata(fileStatus, objectMetadata.metadata());
  }

  @Test
  public void testListStatus() throws IOException {
    ObjectHandle rootObject = newDirectory(new Path("ks://bucket/root"));
    Path rootPath = new Path("s3://bucket/root");
    ObjectHandle child1 = newDirectory(new Path("ks://bucket/root/dir"));
    Path child1Path = new Path("s3://bucket/root/dir");
    ObjectHandle child2 = newFile(new Path("ks://bucket/root/file"));
    Path child2Path = new Path("s3://bucket/root/file");
    doReturn(Optional.of(rootObject)).when(mockMetadataStore).getObject(new Path("ks://bucket/root"));
    doReturn(Arrays.asList(child1, child2)).when(mockMetadataStore).listChildObjects(rootObject);

    List<FileStatus> listing = Lists.newArrayList(fileSystemImplementation.listStatus(rootPath));

    assertEquals(2, listing.size());

    FileStatus child1File = listing.stream()
        .filter(it -> it.getPath().equals(child1Path))
        .findFirst()
        .orElseThrow(AssertionFailedError::new);
    assertEquals(child1Path, child1File.getPath());
    assertFileStatusSameAsObjectMetadata(child1File, child1.metadata());

    FileStatus child2File = listing.stream()
        .filter(it -> it.getPath().equals(child2Path))
        .findFirst()
        .orElseThrow(AssertionFailedError::new);
    assertEquals(child2Path, child2File.getPath());
    assertFileStatusSameAsObjectMetadata(child2File, child2.metadata());
  }

  @Test(expected = FileNotFoundException.class)
  public void testListStatusThrowsErrorForInexistentDirectory() throws IOException {
    Path path = new Path("s3://bucket/dir");
    Path metaPath = new Path("ks://bucket/dir");
    when(mockMetadataStore.getObject(metaPath)).thenReturn(Optional.empty());

    FileStatus[] ignored = fileSystemImplementation.listStatus(path);
  }

  @Test
  public void testListStatusOnFileReturnsTheFile() throws IOException {
    Path path = new Path("s3://bucket/file");
    Path metaPath = new Path("ks://bucket/file");
    ObjectHandle objectMetadata = newFile(metaPath);
    doReturn(Optional.of(objectMetadata)).when(mockMetadataStore).getObject(metaPath);

    List<FileStatus> listing = Lists.newArrayList(fileSystemImplementation.listStatus(path));

    assertEquals(1, listing.size());
    assertEquals(path, listing.get(0).getPath());
    assertFileStatusSameAsObjectMetadata(listing.get(0), objectMetadata.metadata());
  }

  @Test(expected = FileNotFoundException.class)
  public void testOpenFileThrowsErrorForInexistentFile() throws IOException {
    Path path = new Path("s3://bucket/file");
    Path metaPath = new Path("ks://bucket/file");
    when(mockMetadataStore.getObject(metaPath)).thenReturn(Optional.empty());

    FSDataInputStream ignored = fileSystemImplementation.open(path);
  }

  @Test(expected = FileNotFoundException.class)
  public void testOpenFileThrowsErrorForDirectoryObject() throws IOException {
    Path path = new Path("s3://bucket/dir");
    Path metaPath = new Path("ks://bucket/dir");
    ObjectHandle objectMetadata = newDirectory(metaPath);
    doReturn(Optional.of(objectMetadata)).when(mockMetadataStore).getObject(metaPath);

    FSDataInputStream ignored = fileSystemImplementation.open(path);
  }

  @Test
  public void testOpenFileCanBeUsedToReadAllData() throws IOException {
    Path path = new Path("s3://bucket/file");
    Path metaPath = new Path("ks://bucket/file");
    ObjectMetadata objectMetadata = ObjectMetadata.builder()
        .key(metaPath)
        .isDirectory(false)
        .size(20)
        .creationTime(100)
        .physicalDataCommitted(true)
        .physicalPath(Optional.of("s3a://bucket/physical_path"))
        .build();
    ObjectHandle handle = mock(ObjectHandle.class);
    when(handle.metadata()).thenReturn(objectMetadata);
    doReturn(Optional.of(handle)).when(mockMetadataStore).getObject(eq(metaPath));
    byte[] data = randomBytes(20);
    when(mockPhysicalStorage.openKey(new Path("s3a://bucket/physical_path"))).thenReturn(new SeekableByteArrayInputStream(data));

    FSDataInputStream result = fileSystemImplementation.open(path);

    byte[] resultedByteArray = IOUtils.toByteArray(result);
    assertArrayEquals(data, resultedByteArray);
  }

  @Test(expected = UncommittedFileException.class)
  public void testOpenFileThrowsErrorIfDataIsUncommitted() throws IOException {
    Path path = new Path("s3://bucket/file");
    Path metaPath = new Path("ks://bucket/file");
    ObjectMetadata objectMetadata = ObjectMetadata.builder()
        .key(metaPath)
        .isDirectory(false)
        .size(20)
        .creationTime(100)
        .physicalDataCommitted(false)
        .physicalPath(Optional.of("s3a://bucket/physical_path"))
        .build();
    ObjectHandle handle = mock(ObjectHandle.class);
    when(handle.metadata()).thenReturn(objectMetadata);
    doReturn(Optional.of(handle)).when(mockMetadataStore).getObject(eq(metaPath));

    try {
      FSDataInputStream ignored = fileSystemImplementation.open(path);
    } finally {
      verifyZeroInteractions(mockPhysicalStorage);
    }
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testAppendThrowsError() throws IOException {
    FSDataOutputStream ignored = fileSystemImplementation.append(new Path("s3://bucket/file"), 1024, null);
  }

  @Test
  public void testMkdirsReturnsTrueForExistingDirectories() throws IOException {
    Path metaPath1 = new Path("ks://bucket/d");
    Path metaPath2 = new Path("ks://bucket/d/d1");
    mockMetadataStoreToReturnExistingDirectories(metaPath1, metaPath2);
    Path path2 = new Path("s3://bucket/d/d1");

    assertTrue(fileSystemImplementation.mkdirs(path2));
    verify(mockMetadataStore, never()).createObject(any(ObjectMetadata.class));
  }

  @Test
  public void testMkdirsThrowsErrorIfMetadataStoreFailsWhenCreatingLeaf() throws IOException {
    Path metaPath1 = new Path("ks://bucket/d");
    Path metaPath2 = new Path("ks://bucket/d/d1");
    mockMetadataStoreToReturnExistingDirectories(metaPath1);
    mockMetadataStoreToReturnMissingDirectories(metaPath2);
    ArgumentCaptor<ObjectMetadata> objectMetadataCaptor = ArgumentCaptor.forClass(ObjectMetadata.class);
    doThrow(new RuntimeException()).when(mockMetadataStore).createObject(objectMetadataCaptor.capture());

    try {
      boolean ingored = fileSystemImplementation.mkdirs(new Path("s3://bucket/d/d1"));
      fail();
    } catch (RuntimeException e) {}

    assertEquals(1, objectMetadataCaptor.getAllValues().size());
    assertTrue(objectMetadataCaptor.getAllValues().get(0).isDirectory());
    assertEquals(metaPath2, objectMetadataCaptor.getAllValues().get(0).getKey());
  }

  @Test
  public void testMkdirsReturnsFalseIfMetadataStoreFailsOnCreatingIntermediaryParent() throws IOException {
    Path metaPath1 = new Path("ks://bucket/d");
    Path metaPath2 = new Path("ks://bucket/d/d1");
    Path metaPath3 = new Path("ks://bucket/d/d1/d2");
    Path path3 = new Path("s3://bucket/d/d1/d2");
    mockMetadataStoreToReturnExistingDirectories(metaPath1);
    mockMetadataStoreToReturnMissingDirectories(metaPath2, metaPath3);
    ArgumentCaptor<ObjectMetadata> objectMetadataCaptor = ArgumentCaptor.forClass(ObjectMetadata.class);
    doThrow(new RuntimeException()).when(mockMetadataStore).createObject(objectMetadataCaptor.capture());

    try {
      assertFalse(fileSystemImplementation.mkdirs(path3));
      fail();
    } catch (RuntimeException e) {}

    assertEquals(1, objectMetadataCaptor.getAllValues().size());
    assertEquals(metaPath2, objectMetadataCaptor.getAllValues().get(0).getKey());
    assertTrue(objectMetadataCaptor.getAllValues().get(0).isDirectory());
  }

  @Test
  public void  testMkdirsIsSuccessfulWhenParentExists() throws IOException {
    Path metaPath1 = new Path("ks://bucket/d");
    mockMetadataStoreToReturnExistingDirectories(metaPath1);
    Path metaPath2 = new Path("ks://bucket/d/d1");
    Path path2 = new Path("s3://bucket/d/d1");
    mockMetadataStoreToReturnMissingDirectories(metaPath2);
    ArgumentCaptor<ObjectMetadata> objectMetadataCaptor = ArgumentCaptor.forClass(ObjectMetadata.class);
    ObjectHandle handle = mock(ObjectHandle.class);
    doReturn(handle).when(mockMetadataStore).createObject(objectMetadataCaptor.capture());

    assertTrue(fileSystemImplementation.mkdirs(path2));

    assertEquals(1, objectMetadataCaptor.getAllValues().size());
    assertEquals(metaPath2, objectMetadataCaptor.getAllValues().get(0).getKey());
    assertTrue(objectMetadataCaptor.getAllValues().get(0).isDirectory());
  }

  @Test
  public void testMkdirsIsSuccessfulWhenCreatingIntermediaryParent() throws IOException {
    Path metaPath1 = new Path("ks://bucket/d");
    Path metaPath2 = new Path("ks://bucket/d/d1");
    Path metaPath3 = new Path("ks://bucket/d/d1/d2");
    Path path3 = new Path("s3://bucket/d/d1/d2");
    mockMetadataStoreToReturnExistingDirectories(metaPath1);
    mockMetadataStoreToReturnMissingDirectories(metaPath2, metaPath3);
    ArgumentCaptor<ObjectMetadata> objectMetadataCaptor = ArgumentCaptor.forClass(ObjectMetadata.class);
    ObjectHandle handle = mock(ObjectHandle.class);
    doReturn(handle).when(mockMetadataStore).createObject(objectMetadataCaptor.capture());

    assertTrue(fileSystemImplementation.mkdirs(path3));

    assertEquals(2, objectMetadataCaptor.getAllValues().size());
    assertEquals(metaPath2, objectMetadataCaptor.getAllValues().get(0).getKey());
    assertTrue(objectMetadataCaptor.getAllValues().get(0).isDirectory());
    assertEquals(metaPath3, objectMetadataCaptor.getAllValues().get(1).getKey());
    assertTrue(objectMetadataCaptor.getAllValues().get(1).isDirectory());
  }

  @Test
  public void testDeleteInexistentObjectReturnsCorrectly() throws IOException {
    Path metaPath = new Path("ks://bucket/file");
    Path path = new Path("s3://bucket/file");
    when(mockMetadataStore.getObject(metaPath)).thenReturn(Optional.empty());

    assertEquals(DeleteOutcome.ObjectAlreadyDeleted, fileSystemImplementation.delete(path, true));
  }

  @Test(expected = IOException.class)
  public void testDeleteNonEmptyDirectoryWithoutRecursiveFlagThrowsError() throws IOException {
    Path metaPath = new Path("ks://bucket/dir");
    Path path = new Path("s3://bucket/dir");
    mockMetadataStoreToReturnExistingDirectories(metaPath);
    doAnswer(inv -> {
      ObjectMetadata objectMetadata = inv.<ObjectHandle>getArgument(0).metadata();
      return objectMetadata.getKey().equals(metaPath) ?
             Arrays.asList(newFile(new Path(metaPath, "file"))) : Collections.emptyList();
    }).when(mockMetadataStore).listChildObjects(any(ObjectHandle.class));

    DeleteOutcome ignored = fileSystemImplementation.delete(path, false);
  }

  @Test
  public void testDeleteEmptyDirectoryWithoutRecursiveFlagSucceeds() throws IOException {
    Path metaPath = new Path("ks://bucket/dir");
    Path path = new Path("s3://bucket/dir");
    mockMetadataStoreToReturnExistingDirectories(metaPath);
    when(mockMetadataStore.listChildObjects(any(ObjectHandle.class))).thenReturn(Collections.emptyList());

    DeleteOutcome ignored = fileSystemImplementation.delete(path, false);
  }

  @Test
  public void testDeleteFileReturnsTrueIfMetadataDeleteIsSuccessful() throws IOException {
    Path metaPath = new Path("ks://bucket/file");
    Path path = new Path("s3://bucket/file");
    ObjectHandle file = newFile(metaPath);
    doReturn(Optional.of(file)).when(mockMetadataStore).getObject(metaPath);
    ArgumentCaptor<Function<ObjectHandle, Boolean>> callbackCaptor = ArgumentCaptor.forClass(Function.class);
    when(mockMetadataStore.deleteObject(eq(file), callbackCaptor.capture())).thenReturn(true);

    assertEquals(DeleteOutcome.Successful, fileSystemImplementation.delete(path, false));
    PhysicalFileDeleteCallback callback = (PhysicalFileDeleteCallback) callbackCaptor.getValue();
    assertEquals(mockPhysicalStorage, callback.getPhysicalStorage());
  }

  @Test
  public void testDeleteFileReturnsFalseIfMetadataDeleteFail() throws IOException {
    Path metaPath = new Path("ks://bucket/file");
    Path path = new Path("s3://bucket/file");
    ObjectHandle file = newFile(metaPath);
    doReturn(Optional.of(file)).when(mockMetadataStore).getObject(metaPath);
    ArgumentCaptor<Function<ObjectHandle, Boolean>> callbackCaptor = ArgumentCaptor.forClass(Function.class);
    when(mockMetadataStore.deleteObject(eq(file), callbackCaptor.capture())).thenReturn(false);

    assertEquals(DeleteOutcome.Failed, fileSystemImplementation.delete(path, false));
    PhysicalFileDeleteCallback callback = (PhysicalFileDeleteCallback) callbackCaptor.getValue();
    assertEquals(mockPhysicalStorage, callback.getPhysicalStorage());
  }

  @Test
  public void testDeleteDirectoryReturnsTrueIfMetadataDeleteIsSuccessful() throws IOException {
    Path metaPath = new Path("ks://bucket/dir");
    Path path = new Path("s3://bucket/dir");
    ObjectHandle directory = newDirectory(metaPath);
    doReturn(Optional.of(directory)).when(mockMetadataStore).getObject(metaPath);
    ArgumentCaptor<Function<ObjectHandle, Boolean>> callbackCaptor = ArgumentCaptor.forClass(Function.class);
    when(mockMetadataStore.deleteObject(eq(directory), callbackCaptor.capture())).thenReturn(true);

    assertEquals(DeleteOutcome.Successful, fileSystemImplementation.delete(path, true));
    PhysicalFileDeleteCallback callback = (PhysicalFileDeleteCallback) callbackCaptor.getValue();
    assertEquals(mockPhysicalStorage, callback.getPhysicalStorage());
  }

  @Test
  public void testDeleteDirectoryReturnsFalseIfMetadataDeleteFails() throws IOException {
    Path metaPath = new Path("ks://bucket/file");
    Path path = new Path("s3://bucket/file");
    ObjectHandle directory = newDirectory(metaPath);
    doReturn(Optional.of(directory)).when(mockMetadataStore).getObject(metaPath);
    ArgumentCaptor<Function<ObjectHandle, Boolean>> callbackCaptor = ArgumentCaptor.forClass(Function.class);
    when(mockMetadataStore.deleteObject(eq(directory), callbackCaptor.capture())).thenReturn(false);

    assertEquals(DeleteOutcome.Failed, fileSystemImplementation.delete(path, true));
    PhysicalFileDeleteCallback callback = (PhysicalFileDeleteCallback) callbackCaptor.getValue();
    assertEquals(mockPhysicalStorage, callback.getPhysicalStorage());
  }

  @Test(expected = FileNotFoundException.class)
  public void testRenameThrowsErrorIfSourceDoesNotExist() throws IOException {
    Path metaSrc = new Path("ks://bucket/src");
    Path src = new Path("s3://bucket/src");
    Path metaDst = new Path("ks://bucket/dst");
    Path dst = new Path("s3://bucket/dst");
    when(mockMetadataStore.getObject(metaSrc)).thenReturn(Optional.empty());
    when(mockMetadataStore.getObject(metaDst)).thenReturn(Optional.empty());

    boolean ignored = fileSystemImplementation.rename(src, dst);
  }

  @Test()
  public void testRenameReturnsFalseIfDestinationFileExists() throws IOException {
    Path metaSrc = new Path("ks://bucket/src");
    Path src = new Path("s3://bucket/src");
    ObjectHandle srcFile = newFile(metaSrc);
    Path metaDst = new Path("ks://bucket/dst");
    Path dst = new Path("s3://bucket/dst");
    ObjectHandle dstFile = newFile(metaDst);
    doReturn(Optional.of(newDirectory(new Path("ks://bucket/"))))
        .when(mockMetadataStore).getObject(new Path("ks://bucket/"));
    doReturn(Optional.of(srcFile)).when(mockMetadataStore).getObject(metaSrc);
    doReturn(Optional.of(dstFile)).when(mockMetadataStore).getObject(metaDst);

    assertFalse(fileSystemImplementation.rename(src, dst));
  }

  @Test
  public void testRenameMovesDirectoryInExistingDirectoryDestination() throws IOException {
    Path metaSrc = new Path("ks://bucket/src");
    Path src = new Path("s3://bucket/src");
    ObjectHandle srcDirectory = newDirectory(metaSrc);
    Path metaDst = new Path("ks://bucket/dst");
    Path dst = new Path("s3://bucket/dst");
    ObjectHandle dstDirectory = newDirectory(metaDst);
    doReturn(Optional.of(newDirectory(new Path("ks://bucket/"))))
        .when(mockMetadataStore).getObject(new Path("ks://bucket/"));
    doReturn(Optional.of(srcDirectory)).when(mockMetadataStore).getObject(metaSrc);
    doReturn(Optional.of(dstDirectory)).when(mockMetadataStore).getObject(metaDst);
    when(mockMetadataStore.renameObject(srcDirectory, new Path("ks://bucket/dst/src"))).thenReturn(true);

    assertTrue(fileSystemImplementation.rename(src, dst));

    verify(mockMetadataStore, times(1)).renameObject(srcDirectory, new Path("ks://bucket/dst/src"));
  }

  @Test
  public void testRenameMovesFileInExistingDirectoryDestination() throws IOException {
    Path metaSrc = new Path("ks://bucket/src");
    Path src = new Path("s3://bucket/src");
    ObjectHandle srcDirectory = newFile(metaSrc);
    Path metaDst = new Path("ks://bucket/dst");
    Path dst = new Path("s3://bucket/dst");
    ObjectHandle dstDirectory = newDirectory(metaDst);
    doReturn(Optional.of(newDirectory(new Path("ks://bucket/"))))
        .when(mockMetadataStore).getObject(new Path("ks://bucket/"));
    doReturn(Optional.of(srcDirectory)).when(mockMetadataStore).getObject(metaSrc);
    doReturn(Optional.of(dstDirectory)).when(mockMetadataStore).getObject(metaDst);
    when(mockMetadataStore.renameObject(srcDirectory, new Path("ks://bucket/dst/src"))).thenReturn(true);

    assertTrue(fileSystemImplementation.rename(src, dst));

    verify(mockMetadataStore, times(1)).renameObject(srcDirectory, new Path("ks://bucket/dst/src"));
  }

  @Test
  public void testRenameReturnsTrueWhenMovingFileOverExistingDirectory() throws IOException {
    Path metaSrc = new Path("ks://bucket/src");
    Path src = new Path("s3://bucket/src");
    ObjectHandle srcFile = newFile(metaSrc);
    Path metaDst = new Path("ks://bucket/dest-dir");
    Path dst = new Path("s3://bucket/dest-dir");
    ObjectHandle dstDirectory = newDirectory(metaDst);
    doReturn(Optional.of(newDirectory(new Path("ks://bucket/"))))
        .when(mockMetadataStore).getObject(new Path("ks://bucket/"));
    doReturn(Optional.of(srcFile)).when(mockMetadataStore).getObject(metaSrc);
    doReturn(Optional.of(dstDirectory)).when(mockMetadataStore).getObject(metaDst);
    when(mockMetadataStore.renameObject(srcFile, new Path(metaDst, metaSrc.getName()))).thenReturn(true);

    assertTrue(fileSystemImplementation.rename(src, dst));
  }

  @Test(expected = IOException.class)
  public void testRenamingADirectoryAsAChildOfItselfThrowsError() throws IOException {
    Path metaSrc = new Path("ks://bucket/src");
    Path src = new Path("s3://bucket/src");
    ObjectHandle srcDirectory = newDirectory(metaSrc);
    Path metaDst = new Path("ks://bucket/src/folder/dst");
    Path dst = new Path("s3://bucket/src/folder/dst");
    doReturn(Optional.of(srcDirectory)).when(mockMetadataStore).getObject(metaSrc);
    doReturn(Optional.empty()).when(mockMetadataStore).getObject(metaDst);

    boolean ignored = fileSystemImplementation.rename(src, dst);
  }

  @Test
  public void testRenameFileReturnsTrueIfMetadataRenameIsSuccessful() throws IOException {
    Path metaSrc = new Path("ks://bucket/src");
    Path src = new Path("s3://bucket/src");
    ObjectHandle srcFile = newFile(metaSrc);
    Path metaDst = new Path("ks://bucket/dst");
    Path dst = new Path("s3://bucket/dst");
    doReturn(Optional.of(newDirectory(new Path("ks://bucket/"))))
        .when(mockMetadataStore).getObject(new Path("ks://bucket/"));
    doReturn(Optional.of(srcFile)).when(mockMetadataStore).getObject(metaSrc);
    doReturn(Optional.empty()).when(mockMetadataStore).getObject(metaDst);
    when(mockMetadataStore.renameObject(srcFile, metaDst)).thenReturn(true);

    assertTrue(fileSystemImplementation.rename(src, dst));
  }

  @Test
  public void testRenameFileOntoItselfReturnsTrue() throws IOException {
    Path src = new Path("s3://bucket/src");
    Path dst = new Path("s3://bucket/src");

    assertTrue(fileSystemImplementation.rename(src, dst));
  }

  @Test
  public void testRenameFileReturnsFalseIfMetadataRenameFailed() throws IOException {
    Path metaSrc = new Path("ks://bucket/src");
    Path src = new Path("s3://bucket/src");
    ObjectHandle srcFile = newFile(metaSrc);
    Path metaDst = new Path("ks://bucket/dst");
    Path dst = new Path("s3://bucket/dst");
    doReturn(Optional.of(newDirectory(new Path("ks://bucket/"))))
        .when(mockMetadataStore).getObject(new Path("ks://bucket/"));
    doReturn(Optional.of(srcFile)).when(mockMetadataStore).getObject(metaSrc);
    doReturn(Optional.empty()).when(mockMetadataStore).getObject(metaDst);
    when(mockMetadataStore.renameObject(srcFile, metaDst)).thenReturn(false);

    assertFalse(fileSystemImplementation.rename(src, dst));
  }

  @Test
  public void testRenameDirectoryReturnsTrueIfMetadataRenameIsSuccessful() throws IOException {
    Path metaSrc = new Path("ks://bucket/src");
    Path src = new Path("s3://bucket/src");
    ObjectHandle srcDirectory = newDirectory(metaSrc);
    Path metaDst = new Path("ks://bucket/dst");
    Path dst = new Path("s3://bucket/dst");
    doReturn(Optional.of(newDirectory(new Path("ks://bucket/"))))
        .when(mockMetadataStore).getObject(new Path("ks://bucket/"));
    doReturn(Optional.of(srcDirectory)).when(mockMetadataStore).getObject(metaSrc);
    doReturn(Optional.empty()).when(mockMetadataStore).getObject(metaDst);
    when(mockMetadataStore.renameObject(srcDirectory, metaDst)).thenReturn(true);

    assertTrue(fileSystemImplementation.rename(src, dst));
  }

  @Test
  public void testRenameDirectoryReturnsFalseIfMetadataRenameFailed() throws IOException {
    Path metaSrc = new Path("ks://bucket/src");
    Path src = new Path("s3://bucket/src");
    ObjectHandle srcDirectory = newDirectory(metaSrc);
    Path metaDst = new Path("ks://bucket/dst");
    Path dst = new Path("s3://bucket/dst");
    doReturn(Optional.of(newDirectory(new Path("ks://bucket/"))))
        .when(mockMetadataStore).getObject(new Path("ks://bucket/"));
    doReturn(Optional.of(srcDirectory)).when(mockMetadataStore).getObject(metaSrc);
    doReturn(Optional.empty()).when(mockMetadataStore).getObject(metaDst);
    when(mockMetadataStore.renameObject(srcDirectory, metaDst)).thenReturn(false);

    assertFalse(fileSystemImplementation.rename(src, dst));
  }

  @Test(expected = IOException.class)
  public void testCreatingAlreadyExistingFileWithoutOverwriteThrowsError() throws IOException {
    Path metaPath = new Path("ks://bucket/file");
    Path path = new Path("s3://bucket/file");
    ObjectHandle file = newFile(metaPath);
    doReturn(Optional.of(file)).when(mockMetadataStore).getObject(metaPath);
    when(mockPathTranslator.newUniquePath(any(ObjectHandle.class))).thenReturn(new Path("s3a://bucket/random_path"));

    FSDataOutputStream ignored = fileSystemImplementation.create(path, false);
  }

  @Test(expected = IOException.class)
  public void testCreateFileWithOverwriteOverExistingDirectoryThrowsError() throws IOException {
    Path metaPath = new Path("ks://bucket/file");
    Path path = new Path("s3://bucket/file");
    ObjectHandle directory = newDirectory(metaPath);
    doReturn(Optional.of(directory)).when(mockMetadataStore).getObject(metaPath);

    FSDataOutputStream ignored = fileSystemImplementation.create(path, true);
  }

  @Test
  public void testCreateFileWithOverwriteReturnedStreamWriteAllDataToPhysicalStorage() throws IOException {
    Path metaPath = new Path("ks://bucket/file");
    ObjectHandle file = newFile(metaPath, "s3a://bucket/initial_random_path");
    Path path = new Path("s3://bucket/file");
    when(mockPathTranslator.newUniquePath(file)).thenReturn(new Path("s3a://bucket/random_path"));
    ByteArrayOutputStream captureStream = new ByteArrayOutputStream();
    when(mockPhysicalStorage.createKey(new Path("s3a://bucket/random_path"))).thenReturn(captureStream);
    doReturn(Optional.of(file)).when(mockMetadataStore).getObject(metaPath);
    ArgumentCaptor<ObjectMetadata> objectMetadataCaptor = ArgumentCaptor.forClass(ObjectMetadata.class);
    mockMetadataStoreToUpdateObjectHandle(file, objectMetadataCaptor);

    byte[] data = randomBytes(30);
    try (FSDataOutputStream resultedStream = fileSystemImplementation.create(path, true)) {
      IOUtils.write(data, resultedStream);
    }

    assertArrayEquals(data, captureStream.toByteArray());

    List<ObjectMetadata> capturedObjects = objectMetadataCaptor.getAllValues();
    assertEquals(1, capturedObjects.size());

    ObjectMetadata updatedObject = capturedObjects.get(0);
    assertEquals(metaPath, updatedObject.getKey());
    assertFalse(updatedObject.isDirectory());
    assertTrue(updatedObject.physicalDataCommitted());
    assertEquals(30, updatedObject.getSize());
    assertEquals("s3a://bucket/random_path", updatedObject.getPhysicalPath().get());
    verify(mockPhysicalStorage, times(1)).deleteKey(new Path("s3a://bucket/initial_random_path"));
    verify(mockFsMetrics, times(1)).recordSuccessfulOverwrite();
  }

  @Test
  public void testCreateFileWithOverwriteIsSuccessfulEvenIfOriginalFilePhysicalDataCantBeDeleted() throws IOException {
    Path metaPath = new Path("ks://bucket/file");
    ObjectHandle file = newFile(metaPath, "s3a://bucket/initial_random_path");
    Path path = new Path("s3://bucket/file");
    when(mockPathTranslator.newUniquePath(file)).thenReturn(new Path("s3a://bucket/random_path"));
    ByteArrayOutputStream captureStream = new ByteArrayOutputStream();
    when(mockPhysicalStorage.createKey(new Path("s3a://bucket/random_path"))).thenReturn(captureStream);
    doThrow(new IOException()).when(mockPhysicalStorage).deleteKey(new Path("s3a://bucket/initial_random_path"));
    doReturn(Optional.of(file)).when(mockMetadataStore).getObject(metaPath);
    ArgumentCaptor<ObjectMetadata> objectMetadataCaptor = ArgumentCaptor.forClass(ObjectMetadata.class);
    mockMetadataStoreToUpdateObjectHandle(file, objectMetadataCaptor);

    byte[] data = randomBytes(30);
    try (FSDataOutputStream resultedStream = fileSystemImplementation.create(path, true)) {
      IOUtils.write(data, resultedStream);
    }

    assertArrayEquals(data, captureStream.toByteArray());

    List<ObjectMetadata> capturedObjects = objectMetadataCaptor.getAllValues();
    assertEquals(1, capturedObjects.size());

    ObjectMetadata updatedObject = capturedObjects.get(0);
    assertEquals(metaPath, updatedObject.getKey());
    assertFalse(updatedObject.isDirectory());
    assertTrue(updatedObject.physicalDataCommitted());
    assertEquals(30, updatedObject.getSize());
    assertEquals("s3a://bucket/random_path", updatedObject.getPhysicalPath().get());
    verify(mockPhysicalStorage, times(1)).deleteKey(new Path("s3a://bucket/initial_random_path"));
    verify(mockFsMetrics, times(1)).recordFailedOverwrittenFilePhysicalDelete();
  }

  @Test
  public void testCreateFileReturnedStreamWritesAllDataToPhysicalStorage() throws IOException {
    Path metaPath = new Path("ks://bucket/file");
    Path path = new Path("s3://bucket/file");
    ArgumentCaptor<ObjectMetadata> objectMetadataCaptor = ArgumentCaptor.forClass(ObjectMetadata.class);
    ObjectHandle handle = mockMetadataStoreToCreateObjectHandle(objectMetadataCaptor);
    when(mockPathTranslator.newUniquePath(handle)).thenReturn(new Path("s3a://bucket/random_path"));
    ByteArrayOutputStream captureStream = new ByteArrayOutputStream();
    when(mockPhysicalStorage.createKey(new Path("s3a://bucket/random_path"))).thenReturn(captureStream);
    doReturn(Optional.empty()).when(mockMetadataStore).getObject(metaPath);
    mockMetadataStoreToUpdateObjectHandle(handle, objectMetadataCaptor);

    byte[] data = randomBytes(30);
    try (FSDataOutputStream resultedStream = fileSystemImplementation.create(path, false)) {
      IOUtils.write(data, resultedStream);
    }

    assertArrayEquals(data, captureStream.toByteArray());

    List<ObjectMetadata> capturedObjects = objectMetadataCaptor.getAllValues();
    assertEquals(2, capturedObjects.size());

    ObjectMetadata nonCommittedObject = capturedObjects.get(0);
    assertEquals(metaPath, nonCommittedObject.getKey());
    assertFalse(nonCommittedObject.isDirectory());
    assertFalse(nonCommittedObject.physicalDataCommitted());
    assertEquals(0, nonCommittedObject.getSize());
    assertEquals(FileSystemImplementation.UNCOMMITED_PATH_MARKER, nonCommittedObject.getPhysicalPath().get());

    ObjectMetadata committedObject = capturedObjects.get(1);
    assertEquals(metaPath, committedObject.getKey());
    assertFalse(committedObject.isDirectory());
    assertTrue(committedObject.physicalDataCommitted());
    assertEquals(30, committedObject.getSize());
    assertEquals("s3a://bucket/random_path", committedObject.getPhysicalPath().get());

    verify(mockFsMetrics, times(1)).recordSuccessfulCreate();
  }

  @Test(expected = RuntimeException.class)
  public void testCreateFileReturnedStreamThrowsErrorIfMetadataStoreFails() throws IOException {
    Path metaPath = new Path("ks://bucket/file");
    Path path = new Path("s3://bucket/file");
    ObjectHandle file = newFile(metaPath);
    doReturn(Optional.of(file)).when(mockMetadataStore).getObject(metaPath);
    when(mockPathTranslator.newUniquePath(file)).thenReturn(new Path("s3a://bucket/random_path"));
    when(mockPhysicalStorage.createKey(new Path("s3a://bucket/random_path"))).thenReturn(new ByteArrayOutputStream());
    doThrow(new RuntimeException()).when(mockMetadataStore).updateObject(any(ObjectHandle.class), any(ObjectMetadata.class));

    byte[] data = randomBytes(30);
    try (FSDataOutputStream resultedStream = fileSystemImplementation.create(path, true)) {
      IOUtils.write(data, resultedStream);
    }
  }

  @Test
  public void testCreateFileReturnedStreamThrowsErrorIfPhysicalStorageFails() throws IOException {
    Path metaPath = new Path("ks://bucket/file");
    Path path = new Path("s3://bucket/file");
    ObjectHandle file = newFile(metaPath);
    doReturn(Optional.of(file)).when(mockMetadataStore).getObject(metaPath);
    when(mockPathTranslator.newUniquePath(file)).thenReturn(new Path("s3a://bucket/random_path"));
    ArgumentCaptor<ObjectMetadata> objectMetadataCaptor = ArgumentCaptor.forClass(ObjectMetadata.class);
    mockMetadataStoreToUpdateObjectHandle(file, objectMetadataCaptor);
    when(mockPhysicalStorage.createKey(new Path("s3a://bucket/random_path")))
        .thenReturn(new ErrorThrowingByteArrayOutputStream(true));

    byte[] data = randomBytes(30);
    FSDataOutputStream resultedStream = fileSystemImplementation.create(path, true);
    try {
      IOUtils.write(data, resultedStream);
    } finally {
      try {
        resultedStream.close();
        fail("IOException should have been thrown on close");
      } catch (IOException ignored) {
      }
    }
    verifyZeroInteractions(mockFsMetrics);
  }

  @Test
  public void testCreateFileCreatesParentDirectoriesIfTheyDontExist() throws IOException {
    Path metaPath = new Path("ks://bucket/d");
    Path metaPath1 = new Path("ks://bucket/d/d1");
    Path metaPath2 = new Path("ks://bucket/d/d1/d2");
    mockMetadataStoreToReturnExistingDirectories(metaPath);
    mockMetadataStoreToReturnMissingDirectories(metaPath1, metaPath2);
    ArgumentCaptor<ObjectMetadata> objectMetadataCaptor = ArgumentCaptor.forClass(ObjectMetadata.class);
    ObjectHandle handle = mockMetadataStoreToCreateObjectHandle(objectMetadataCaptor);
    mockMetadataStoreToUpdateObjectHandle(handle, objectMetadataCaptor);
    Path fileMetaPath = new Path("ks://bucket/d/d1/d2/f");
    Path filePath = new Path("s3://bucket/d/d1/d2/f");
    when(mockPathTranslator.newUniquePath(handle)).thenReturn(new Path("s3a://bucket/random_path"));
    ByteArrayOutputStream captureStream = new ByteArrayOutputStream();
    when(mockPhysicalStorage.createKey(new Path("s3a://bucket/random_path"))).thenReturn(captureStream);

    byte[] data = randomBytes(30);
    try (FSDataOutputStream resultedStream = fileSystemImplementation.create(filePath, false)) {
      IOUtils.write(data, resultedStream);
    }

    assertArrayEquals(data, captureStream.toByteArray());

    // 2 parents and 1 file (with two calls to mark physical data committed)
    assertEquals(4, objectMetadataCaptor.getAllValues().size());

    ObjectMetadata parent1 = objectMetadataCaptor.getAllValues().get(0);
    assertTrue(parent1.isDirectory());
    assertEquals(metaPath1, parent1.getKey());

    ObjectMetadata parent2 = objectMetadataCaptor.getAllValues().get(1);
    assertTrue(parent2.isDirectory());
    assertEquals(metaPath2, parent2.getKey());

    ObjectMetadata nonCommittedObject = objectMetadataCaptor.getAllValues().get(2);
    assertEquals(fileMetaPath, nonCommittedObject.getKey());
    assertFalse(nonCommittedObject.isDirectory());
    assertFalse(nonCommittedObject.physicalDataCommitted());
    assertEquals(0, nonCommittedObject.getSize());
    assertEquals(FileSystemImplementation.UNCOMMITED_PATH_MARKER, nonCommittedObject.getPhysicalPath().get());

    ObjectMetadata committedObject = objectMetadataCaptor.getAllValues().get(3);
    assertEquals(fileMetaPath, committedObject.getKey());
    assertFalse(committedObject.isDirectory());
    assertTrue(committedObject.physicalDataCommitted());
    assertEquals(30, committedObject.getSize());
    assertEquals("s3a://bucket/random_path", committedObject.getPhysicalPath().get());

    verify(mockFsMetrics, times(1)).recordSuccessfulCreate();
  }

  @Test
  public void testCreateFileThrowsErrorIfParentsCantBeCreated() throws IOException {
    Path metaPath = new Path("ks://bucket/d");
    Path metaPath1 = new Path("ks://bucket/d/d1");
    mockMetadataStoreToReturnExistingDirectories(metaPath);
    mockMetadataStoreToReturnMissingDirectories(metaPath1);
    Path filePath = new Path("s3://bucket/d/d1/f");
    doThrow(new RuntimeException()).when(mockMetadataStore).createObject(any(ObjectMetadata.class));
    when(mockPathTranslator.newUniquePath(any(ObjectHandle.class))).thenReturn(new Path("s3a://bucket/random_path"));

    FSDataOutputStream shouldBeNull = null;
    try {
      shouldBeNull = fileSystemImplementation.create(filePath, false);
      fail("Error should be thrown");
    } catch (RuntimeException ignored) {

    }

    assertNull(shouldBeNull);
    verify(mockMetadataStore, times(1)).createObject(any(ObjectMetadata.class));
    verifyZeroInteractions(mockPhysicalStorage);
    verifyZeroInteractions(mockFsMetrics);
  }

  @Test
  public void testCloseClosesDependencies() throws IOException {
    fileSystemImplementation.close();

    verify(mockMetadataStore, times(1)).close();
    verify(mockPhysicalStorage, times(1)).close();
  }

  private static byte[] randomBytes(int size) {
    byte[] result = new byte[size];
    new Random().nextBytes(result);
    return result;
  }

  private ObjectHandle newFile(Path path) {
    ObjectMetadata metadata = ObjectMetadata.builder()
        .key(path)
        .isDirectory(false)
        .creationTime(Math.abs(random.nextInt()))
        .size(Math.abs(random.nextInt()))
        .physicalPath(UUID.randomUUID().toString())
        .physicalDataCommitted(true)
        .build();
    ObjectHandle objectHandle = mock(ObjectHandle.class);
    when(objectHandle.metadata()).thenReturn(metadata);
    return objectHandle;
  }

  private ObjectHandle newFile(Path path, String physicalPath) {
    ObjectMetadata metadata = ObjectMetadata.builder()
        .key(path)
        .isDirectory(false)
        .creationTime(Math.abs(random.nextInt()))
        .size(Math.abs(random.nextInt()))
        .physicalPath(physicalPath)
        .physicalDataCommitted(true)
        .build();
    ObjectHandle objectHandle = mock(ObjectHandle.class);
    when(objectHandle.metadata()).thenReturn(metadata);
    return objectHandle;
  }

  private ObjectHandle newDirectory(Path path) {
    ObjectMetadata metadata = ObjectMetadata.builder()
        .key(path)
        .isDirectory(true)
        .creationTime(Math.abs(random.nextInt()))
        .size(0)
        .physicalPath(Optional.empty())
        .build();
    ObjectHandle objectHandle = mock(ObjectHandle.class);
    when(objectHandle.metadata()).thenReturn(metadata);
    return objectHandle;
  }

  private void assertFileStatusSameAsObjectMetadata(FileStatus fileStatus, ObjectMetadata objectMetadata) throws IOException {
    assertEquals(objectMetadata.getSize(), fileStatus.getLen());
    assertEquals(objectMetadata.getCreationTime() * 1000, fileStatus.getModificationTime());
    assertEquals(0, fileStatus.getAccessTime());
    assertEquals(objectMetadata.isDirectory(), fileStatus.isDirectory());
    if (objectMetadata.isDirectory()) {
      assertEquals(new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL, false), fileStatus.getPermission());
    } else {
      assertEquals(new FsPermission(FsAction.READ_WRITE, FsAction.READ_WRITE, FsAction.READ_WRITE, false), fileStatus.getPermission());
    }
    assertEquals(UserGroupInformation.getCurrentUser().getShortUserName(), fileStatus.getOwner());
    assertEquals(UserGroupInformation.getCurrentUser().getShortUserName(), fileStatus.getGroup());
  }

  private void mockMetadataStoreToReturnExistingDirectories(Path... directories) {
    for (Path directory : directories) {
      ObjectMetadata objectMetadata = ObjectMetadata.builder()
          .key(directory)
          .isDirectory(true)
          .creationTime(Instant.now().getEpochSecond())
          .size(0)
          .physicalPath(Optional.empty())
          .build();
      ObjectHandle handle = mock(ObjectHandle.class);
      when(handle.metadata()).thenReturn(objectMetadata);
      doReturn(Optional.of(handle)).when(mockMetadataStore).getObject(eq(directory));
    }
  }
  private void mockMetadataStoreToReturnMissingDirectories(Path... directories) {
    for (Path directory : directories) {
      doReturn(Optional.empty()).when(mockMetadataStore).getObject(eq(directory));
    }
  }

  private void mockMetadataStoreToUpdateObjectHandle(ObjectHandle handle, ArgumentCaptor<ObjectMetadata> captor) {
    doAnswer(inv -> {
      ObjectHandle updatedHandle = mock(ObjectHandle.class);
      ObjectMetadata objectMetadata = inv.getArgument(1);
      when(updatedHandle.metadata()).thenReturn(objectMetadata);
      return updatedHandle;
    }).when(mockMetadataStore).updateObject(eq(handle), captor.capture());
  }

  private ObjectHandle mockMetadataStoreToCreateObjectHandle(ArgumentCaptor<ObjectMetadata> captor) {
    ObjectHandle handle = mock(ObjectHandle.class);
    doAnswer(inv -> {
      ObjectMetadata objectMetadata = inv.getArgument(0);
      when(handle.metadata()).thenReturn(objectMetadata);
      return handle;
    }).when(mockMetadataStore).createObject(captor.capture());
    return handle;
  }

  private static class ErrorThrowingByteArrayOutputStream extends ByteArrayOutputStream {
    private final boolean errorOnClose;

    public ErrorThrowingByteArrayOutputStream(boolean errorOnClose) {
      this.errorOnClose = errorOnClose;
    }

    @Override
    public void close() throws IOException {
      if (errorOnClose) {
        throw new IOException();
      }
    }
  }

  private static class SeekableByteArrayInputStream extends ByteArrayInputStream implements Seekable, PositionedReadable {

    public SeekableByteArrayInputStream(byte[] buffer) {
      super(buffer);
    }

    @Override
    public void seek(long l) throws IOException {
      if (l >= count) {
        throw new EOFException();
      }
      pos = (int)l;
    }

    @Override
    public long getPos() throws IOException {
      return pos;
    }

    @Override
    public boolean seekToNewSource(long l) throws IOException {
      return false;
    }

    @Override
    public int read(long l, byte[] bytes, int i, int i1) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void readFully(long l, byte[] bytes, int i, int i1) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void readFully(long l, byte[] bytes) throws IOException {
      throw new UnsupportedOperationException();
    }
  }
}
