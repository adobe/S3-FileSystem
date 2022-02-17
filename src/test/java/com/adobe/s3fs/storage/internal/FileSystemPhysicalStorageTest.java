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

package com.adobe.s3fs.storage.internal;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.atomic.AtomicInteger;

public class FileSystemPhysicalStorageTest {

  @Mock
  private FileSystemPhysicalStorageConfiguration mockStorageConfiguration;

  @Mock
  private FileSystem mockFileSystem;

  private FileSystemPhysicalStorage physicalStorage;

  private static final int MAX_RETRIES = 3;
  private static final int DELAY_BETWEEN_RETRIES = 5;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
    when(mockStorageConfiguration.getEventualConsistencyMaxRetries()).thenReturn(MAX_RETRIES);
    when(mockStorageConfiguration.getEventualConsistencyDelayBetweenRetriesMillis()).thenReturn(DELAY_BETWEEN_RETRIES);

    physicalStorage = new FileSystemPhysicalStorage(mockFileSystem, mockStorageConfiguration);
  }

  @Test
  public void testCreateReturnsUnderlyingFilesSystemStream() throws IOException {
    Path path = new Path("s3a://bucket/f");
    FSDataOutputStream mockOutputStream = mock(FSDataOutputStream.class);
    when(mockFileSystem.create(path, true)).thenReturn(mockOutputStream);

    assertEquals(mockOutputStream, physicalStorage.createKey(path));
  }

  @Test
  public void testOpenRetriesUntilUnderlyingFileIsVisibleAndReturnsTheStream() throws IOException {
    Path path = new Path("s3a://bucket/f");

    FSDataInputStream mockInputStream = mock(FSDataInputStream.class);
    AtomicInteger tries = new AtomicInteger();
    doAnswer(it -> {
     if (tries.incrementAndGet() < MAX_RETRIES) {
       throw new FileNotFoundException();
     }
     return mockInputStream;
    }).when(mockFileSystem).open(eq(path));

    assertEquals(mockInputStream, physicalStorage.openKey(path));
  }

  @Test(expected = FileNotFoundException.class)
  public void testOpenThrowsErrorIfRetriesForUnderlyingVisibilityAreExhausted() throws IOException {
    Path path = new Path("s3a://bucket/f");

    doAnswer(it -> {
      throw new FileNotFoundException();
    }).when(mockFileSystem).open(eq(path));

    InputStream ignored =  physicalStorage.openKey(path);
  }

  @Test(expected = TestIOException.class)
  public void testOpenThrowsUnderlyingIOError() throws IOException {
    Path path = new Path("s3a://bucket/f");

    doAnswer(it -> {
      throw new TestIOException();
    }).when(mockFileSystem).open(eq(path));

    InputStream ignored =  physicalStorage.openKey(path);
  }

  @Test(expected = TestRuntimeException.class)
  public void testOpenThrowsUnderlyingRuntimeError() throws IOException {
    Path path = new Path("s3a://bucket/f");

    doAnswer(it -> {
      throw new TestRuntimeException();
    }).when(mockFileSystem).open(eq(path));

    InputStream ignored =  physicalStorage.openKey(path);
  }

  @Test
  public void testExistsReturnsUnderlyingFileSystemResult() throws IOException {
    Path path = new Path("s3a://bucket/f");
    when(mockFileSystem.exists(path)).thenReturn(true);

    assertTrue(physicalStorage.exists(path));

    Path path1 = new Path("s3a://bucket/f1");
    when(mockFileSystem.exists(path1)).thenReturn(false);

    assertFalse(physicalStorage.exists(path1));
  }

  @Test
  public void testDeleteIsSuccessful() throws IOException {
    Path path = new Path("s3a://bucket/f");
    when(mockFileSystem.exists(path)).thenReturn(true);
    when(mockFileSystem.delete(path, true)).thenReturn(true);

    physicalStorage.deleteKey(path);

    verify(mockFileSystem, times(1)).delete(path, true);
  }

  @Test(expected = IOException.class)
  public void testDeleteThrowsErrorIfUnsuccessful() throws IOException {
    Path path = new Path("s3a://bucket/f");
    when(mockFileSystem.exists(path)).thenReturn(true);
    when(mockFileSystem.delete(path, true)).thenReturn(false);

    physicalStorage.deleteKey(path);

  }

  @Test
  public void testDeleteDoesNotThrowErrorIfFileDoesNotExists() throws IOException {
    Path path = new Path("s3a://bucket/f");
    when(mockFileSystem.exists(path)).thenReturn(false);
    when(mockFileSystem.delete(path, true)).thenReturn(false);

    physicalStorage.deleteKey(path);
  }

  @Test
  public void testCloseClosesFileSystem() throws IOException {
    physicalStorage.close();

    verify(mockFileSystem, times(1)).close();
  }

  private static class TestRuntimeException extends RuntimeException {}

  private static class TestIOException extends IOException {}
}
