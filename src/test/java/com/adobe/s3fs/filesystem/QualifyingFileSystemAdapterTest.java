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

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.net.URI;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

public class QualifyingFileSystemAdapterTest {

  @Mock
  private FileSystemImplementation mockFileSystemImplementation;

  @Mock
  private QualifyingFileSystemMetrics mockQualifyingFsMetrics;

  private QualifyingFileSystemAdapter qualifyingFileSystemAdapter;

  private static final URI DEFAULT_URI = URI.create("s3://bucket");

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);

    qualifyingFileSystemAdapter =
        new QualifyingFileSystemAdapter(
            DEFAULT_URI, mockFileSystemImplementation, mockQualifyingFsMetrics);
  }

  @Test
  public void testOpenWithAbsolutePath() throws IOException {
    Path path = new Path("s3://bucket/f");
    FSDataInputStream mockInputStream = mock(FSDataInputStream.class);
    when(mockFileSystemImplementation.open(path)).thenReturn(mockInputStream);

    FSDataInputStream result = qualifyingFileSystemAdapter.open(path);

    assertEquals(mockInputStream, result);
    verify(mockFileSystemImplementation, times(1)).open(path);
  }

  @Test
  public void testOpenWithRelativePath() throws IOException {
    qualifyingFileSystemAdapter.setWorkingDirectory(new Path("s3://bucket/d"));
    Path relative = new Path("f");
    Path absolute = new Path("s3://bucket/d/f");
    FSDataInputStream mockInputStream = mock(FSDataInputStream.class);
    when(mockFileSystemImplementation.open(absolute)).thenReturn(mockInputStream);

    FSDataInputStream result = qualifyingFileSystemAdapter.open(relative);

    assertEquals(mockInputStream, result);
    verify(mockFileSystemImplementation, times(1)).open(absolute);
  }

  @Test
  public void testCreateWithAbsolutePath() throws IOException {
    Path path = new Path("s3://bucket/f");
    FSDataOutputStream mockOutputStream = mock(FSDataOutputStream.class);
    when(mockFileSystemImplementation.create(path, true)).thenReturn(mockOutputStream);

    FSDataOutputStream result = qualifyingFileSystemAdapter.create(path, true);

    assertEquals(mockOutputStream, result);
    verify(mockFileSystemImplementation, times(1)).create(path, true);
  }

  @Test
  public void testFailedCreateWithAbsolutePath() throws IOException {
    Path path = new Path("s3://bucket/f");
    doThrow(new IOException())
        .when(mockFileSystemImplementation)
        .create(any(Path.class), eq(false));

    try {
      qualifyingFileSystemAdapter.create(path, false);
    } catch (IOException ignored) {
    }
    verify(mockQualifyingFsMetrics, times(1)).recordFailedCreate();
  }

  @Test
  public void testFailedOverwriteWithAbsolutePath() throws IOException {
    Path path = new Path("s3://bucket/f");
    doThrow(new IOException())
        .when(mockFileSystemImplementation)
        .create(any(Path.class), eq(true));

    try {
      qualifyingFileSystemAdapter.create(path, true);
    } catch (IOException ignored) {
    }
    verify(mockQualifyingFsMetrics, times(1)).recordFailedOverwrite();
  }

  @Test
  public void testCreateWithRelativePath() throws IOException {
    qualifyingFileSystemAdapter.setWorkingDirectory(new Path("s3://bucket/d"));
    Path absolute = new Path("s3://bucket/d/f");
    Path relative = new Path("f");
    FSDataOutputStream mockOutputStream = mock(FSDataOutputStream.class);
    when(mockFileSystemImplementation.create(absolute, true)).thenReturn(mockOutputStream);

    FSDataOutputStream result = qualifyingFileSystemAdapter.create(relative, true);

    assertEquals(mockOutputStream, result);
    verify(mockFileSystemImplementation, times(1)).create(absolute, true);
  }

  @Test
  public void testRenameWithAbsolutePaths() throws IOException {
    Path src = new Path("s3://bucket/src");
    Path dst = new Path("s3://bucket/dst");
    when(mockFileSystemImplementation.rename(src, dst)).thenReturn(true);

    assertTrue(qualifyingFileSystemAdapter.rename(src, dst));

    verify(mockFileSystemImplementation, times(1)).rename(src, dst);
    verify(mockQualifyingFsMetrics, times(1)).recordSuccessfulRename();
  }

  @Test
  public void testFailedRenameWithAbsolutePaths() throws IOException {
    Path src = new Path("s3://bucket/src");
    Path dst = new Path("s3://bucket/dst");
    doThrow(new IOException())
        .when(mockFileSystemImplementation)
        .rename(any(Path.class), any(Path.class));

    try {
      qualifyingFileSystemAdapter.rename(src, dst);
    } catch (IOException ignored) {

    }
    verify(mockQualifyingFsMetrics, times(1)).recordFailedRename();
  }

  @Test
  public void testRenameWithRelativePaths() throws IOException {
    qualifyingFileSystemAdapter.setWorkingDirectory(new Path("s3://bucket/d"));
    Path srcRelative = new Path("src");
    Path dstRelative = new Path("dst");
    Path srcAbsolute = new Path("s3://bucket/d/src");
    Path dstAbsolute = new Path("s3://bucket/d/dst");
    when(mockFileSystemImplementation.rename(srcAbsolute, dstAbsolute)).thenReturn(true);

    assertTrue(qualifyingFileSystemAdapter.rename(srcRelative, dstRelative));

    verify(mockFileSystemImplementation, times(1)).rename(srcAbsolute, dstAbsolute);
  }

  @Test
  public void testDeleteWithAbsolutePath() throws IOException {
    Path path = new Path("s3://bucket/f");
    when(mockFileSystemImplementation.delete(path, true)).thenReturn(DeleteOutcome.Successful);

    assertTrue(qualifyingFileSystemAdapter.delete(path, true));

    verify(mockFileSystemImplementation, times(1)).delete(path, true);
    verify(mockQualifyingFsMetrics, times(1)).recordSuccessfulDelete();
  }

  @Test
  public void testFailedDeleteWithAbsolutePath() throws IOException {
    Path path = new Path("s3://bucket/f");
    when(mockFileSystemImplementation.delete(path, true)).thenReturn(DeleteOutcome.Failed);

    assertFalse(qualifyingFileSystemAdapter.delete(path, true));

    verify(mockQualifyingFsMetrics, times(1)).recordFailedDelete();
  }

  @Test
  public void testFailedDeleteObjectAlreadyDeletedWithAbsolutePath() throws IOException {
    Path path = new Path("s3://bucket/f");
    when(mockFileSystemImplementation.delete(path, true)).thenReturn(DeleteOutcome.ObjectAlreadyDeleted);

    assertFalse(qualifyingFileSystemAdapter.delete(path, true));

    verifyZeroInteractions(mockQualifyingFsMetrics);
  }

  @Test
  public void testFailedObjectAlreadyDeletedWithRelativePath() throws IOException {
    qualifyingFileSystemAdapter.setWorkingDirectory(new Path("s3://bucket/d"));
    Path absolute = new Path("s3://bucket/d/f");
    Path relative = new Path("f");
    when(mockFileSystemImplementation.delete(absolute, true)).thenReturn(DeleteOutcome.ObjectAlreadyDeleted);

    assertFalse(qualifyingFileSystemAdapter.delete(relative, true));

    verifyZeroInteractions(mockQualifyingFsMetrics);
  }

  @Test
  public void testFailedDeleteWithAbsolutePathDueThrow() throws IOException {
    Path path = new Path("s3://bucket/f");
    doThrow(new IOException())
        .when(mockFileSystemImplementation)
        .delete(any(Path.class), eq(true));

    try {
      qualifyingFileSystemAdapter.delete(path, true);
    } catch (IOException ignored) {

    }
    verify(mockQualifyingFsMetrics, times(1)).recordFailedDelete();
  }

  @Test
  public void testDeleteWithRelativePath() throws IOException {
    qualifyingFileSystemAdapter.setWorkingDirectory(new Path("s3://bucket/d"));
    Path absolute = new Path("s3://bucket/d/f");
    Path relative = new Path("f");
    when(mockFileSystemImplementation.delete(absolute, true)).thenReturn(DeleteOutcome.Successful);

    assertTrue(qualifyingFileSystemAdapter.delete(relative, true));

    verify(mockFileSystemImplementation, times(1)).delete(absolute, true);
  }

  @Test
  public void testGetFileStatusWithAbsolutePath() throws IOException {
    Path path = new Path("s3://bucket/f");
    FileStatus mockFileStatus = mock(FileStatus.class);
    when(mockFileSystemImplementation.getFileStatus(path)).thenReturn(mockFileStatus);

    FileStatus result = qualifyingFileSystemAdapter.getFileStatus(path);

    assertEquals(mockFileStatus, result);
    verify(mockFileSystemImplementation, times(1)).getFileStatus(path);
  }

  @Test
  public void testGetFileStatusWithRelativePath() throws IOException {
    qualifyingFileSystemAdapter.setWorkingDirectory(new Path("s3://bucket/d"));
    Path absolute = new Path("s3://bucket/d/f");
    Path relative = new Path("f");
    FileStatus mockFileStatus = mock(FileStatus.class);
    when(mockFileSystemImplementation.getFileStatus(absolute)).thenReturn(mockFileStatus);

    FileStatus result = qualifyingFileSystemAdapter.getFileStatus(relative);

    assertEquals(mockFileStatus, result);
    verify(mockFileSystemImplementation, times(1)).getFileStatus(absolute);
  }

  @Test
  public void testListStatusWithAbsolutePath() throws IOException {
    Path path = new Path("s3://bucket/f");
    FileStatus mockFileStatus = mock(FileStatus.class);
    FileStatus[] fileStatusArray = new FileStatus[]{mockFileStatus};
    when(mockFileSystemImplementation.listStatus(path)).thenReturn(fileStatusArray);

    FileStatus[] result = qualifyingFileSystemAdapter.listStatus(path);

    assertArrayEquals(fileStatusArray, result);
    verify(mockFileSystemImplementation, times(1)).listStatus(path);
  }

  @Test
  public void testListStatusWithRelativePath() throws IOException {
    qualifyingFileSystemAdapter.setWorkingDirectory(new Path("s3://bucket/d"));
    Path absolute = new Path("s3://bucket/d/f");
    Path relative = new Path("f");
    FileStatus mockFileStatus = mock(FileStatus.class);
    FileStatus[] fileStatusArray = new FileStatus[]{mockFileStatus};
    when(mockFileSystemImplementation.listStatus(absolute)).thenReturn(fileStatusArray);

    FileStatus[] result = qualifyingFileSystemAdapter.listStatus(relative);

    assertArrayEquals(fileStatusArray, result);
    verify(mockFileSystemImplementation, times(1)).listStatus(absolute);
  }

  @Test
  public void testMkdirWithAbsolutePath() throws IOException {
    Path path = new Path("s3://bucket/f");
    when(mockFileSystemImplementation.mkdirs(path)).thenReturn(true);

    assertTrue(qualifyingFileSystemAdapter.mkdirs(path));

    verify(mockFileSystemImplementation, times(1)).mkdirs(path);
    verify(mockQualifyingFsMetrics, times(1)).recordSuccessfulMkdir();
  }

  @Test
  public void testMkdirWithRelativePath() throws IOException {
    qualifyingFileSystemAdapter.setWorkingDirectory(new Path("s3://bucket/d"));
    Path aboslute = new Path("s3://bucket/d/f");
    Path relative = new Path("f");
    when(mockFileSystemImplementation.mkdirs(aboslute)).thenReturn(true);

    assertTrue(qualifyingFileSystemAdapter.mkdirs(relative));

    verify(mockFileSystemImplementation, times(1)).mkdirs(aboslute);
  }

  @Test
  public void testCloseClosesUnderlyingObject() throws IOException {
    qualifyingFileSystemAdapter.close();

    verify(mockFileSystemImplementation, times(1)).close();
  }
}
