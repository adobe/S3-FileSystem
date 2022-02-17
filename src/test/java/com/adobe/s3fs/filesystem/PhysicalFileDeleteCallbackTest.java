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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import com.adobe.s3fs.metastore.api.ObjectHandle;
import com.adobe.s3fs.metastore.api.ObjectMetadata;
import com.adobe.s3fs.storage.api.PhysicalStorage;

import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.util.Optional;

public class PhysicalFileDeleteCallbackTest {

  @Mock
  private PhysicalStorage mockPhysicalStorage;

  @Mock
  private FileSystemMetrics mockFsMetrics;

  private PhysicalFileDeleteCallback physicalFileDeleteCallback;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
    physicalFileDeleteCallback = new PhysicalFileDeleteCallback(mockPhysicalStorage, mockFsMetrics);
  }

  @Test
  public void testDirectoriesAreIgnored() {
    ObjectMetadata directory = ObjectMetadata.builder()
        .key(new Path("ks://bucket/file"))
        .isDirectory(true)
        .creationTime(100)
        .size(0)
        .physicalPath(Optional.empty())
        .build();
    ObjectHandle mockHandle = mock(ObjectHandle.class);
    when(mockHandle.metadata()).thenReturn(directory);

    assertTrue(physicalFileDeleteCallback.apply(mockHandle));

    verifyZeroInteractions(mockPhysicalStorage);
    verifyZeroInteractions(mockFsMetrics);
  }

  @Test
  public void testDeleteUncommittedFilesReturnsTrue() {
    ObjectMetadata directory = ObjectMetadata.builder()
        .key(new Path("ks://bucket/file"))
        .isDirectory(false)
        .creationTime(100)
        .size(0)
        .physicalPath("UNCOMMITTED")
        .physicalDataCommitted(false)
        .build();
    ObjectHandle mockHandle = mock(ObjectHandle.class);
    when(mockHandle.metadata()).thenReturn(directory);

    assertTrue(physicalFileDeleteCallback.apply(mockHandle));

    verifyZeroInteractions(mockPhysicalStorage);
    verifyZeroInteractions(mockFsMetrics);
  }

  @Test
  public void testFileIsDeletedFromPhysicalStorage() throws IOException {
    ObjectMetadata file = ObjectMetadata.builder()
        .key(new Path("ks://bucket/file"))
        .isDirectory(false)
        .creationTime(200)
        .size(100)
        .physicalPath("s3a://bucket/physical_path")
        .physicalDataCommitted(true)
        .build();
    ObjectHandle mockHandle = mock(ObjectHandle.class);
    when(mockHandle.metadata()).thenReturn(file);

    assertTrue(physicalFileDeleteCallback.apply(mockHandle));

    verify(mockPhysicalStorage, times(1)).deleteKey(new Path("s3a://bucket/physical_path"));
    verifyZeroInteractions(mockFsMetrics);
  }

  @Test
  public void testCallbackReturnsFalseWhenPhysicalStorageFails() throws IOException {
    ObjectMetadata file = ObjectMetadata.builder()
        .key(new Path("ks://bucket/file"))
        .isDirectory(false)
        .creationTime(200)
        .size(100)
        .physicalDataCommitted(true)
        .physicalPath("s3a://bucket/physical_path")
        .build();
    ObjectHandle mockHandle = mock(ObjectHandle.class);
    when(mockHandle.metadata()).thenReturn(file);
    doThrow(new RuntimeException()).when(mockPhysicalStorage).deleteKey(new Path("s3a://bucket/physical_path"));

    assertFalse(physicalFileDeleteCallback.apply(mockHandle));

    verify(mockPhysicalStorage, times(1)).deleteKey(new Path("s3a://bucket/physical_path"));
    verify(mockFsMetrics, times(1)).recordFailedPhysicalDelete();
  }
}
