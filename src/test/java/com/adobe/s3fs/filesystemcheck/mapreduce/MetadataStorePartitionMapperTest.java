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

package com.adobe.s3fs.filesystemcheck.mapreduce;

import com.adobe.s3fs.metastore.api.ObjectMetadata;
import com.adobe.s3fs.metastore.api.VersionedObjectHandle;
import com.adobe.s3fs.metastore.internal.dynamodb.versioning.VersionedObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.util.UUID;

import static com.adobe.s3fs.filesystemcheck.mapreduce.FileSystemMRJobConfig.LOGICAL_ROOT_PATH;
import static com.adobe.s3fs.filesystemcheck.mapreduce.FsckTestUtils.*;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class MetadataStorePartitionMapperTest {

  @Mock private Mapper<Void, VersionedObjectHandle, Text, NullWritable>.Context mockContext;

  @Mock private Counter mockCounter;

  private Configuration config;

  private MetadataStorePartitionMapper mapper;

  @Before
  public void setup() throws IOException, InterruptedException {
    MockitoAnnotations.initMocks(this);

    config = new Configuration(true);
    config.set(LOGICAL_ROOT_PATH, "ks://" + BUCKET);

    when(mockContext.getConfiguration()).thenReturn(config);
    when(mockContext.getCounter(any(Enum.class))).thenReturn(mockCounter);

    mapper = new MetadataStorePartitionMapper();
    mapper.setup(mockContext);
  }

  @Test
  public void testAllParentsOfSingleFile() throws IOException, InterruptedException {
    // Prepare
    UUID uuid = UUID.randomUUID();
    String phyPath = physicalEntry(BUCKET, uuid);
    String logicalPath = logicalEntry(BUCKET, "dir1/dir2/dir3/dir4/file");
    VersionedObjectHandle handle = buildVersionedObject(phyPath, logicalPath, uuid, true);
    // Act
    mapper.map(null, handle, mockContext);
    // Verify
    String firstParent = "dir1/dir2/dir3/dir4";
    String secondParent = "dir1/dir2/dir3";
    String thirdParent = "dir1/dir2";
    String lastParent = "dir1";
    ArgumentCaptor<Text> textCaptor = ArgumentCaptor.forClass(Text.class);
    verify(mockContext, times(4)).write(textCaptor.capture(), any(NullWritable.class));

    // First parent (from the end)
    assertEquals(new Text(logicalEntry(BUCKET, firstParent)), textCaptor.getAllValues().get(0));

    // Second parent (from the end)
    assertEquals(new Text(logicalEntry(BUCKET, secondParent)), textCaptor.getAllValues().get(1));

    // Third parent (from the end)
    assertEquals(new Text(logicalEntry(BUCKET, thirdParent)), textCaptor.getAllValues().get(2));

    // Forth parent (from the end)
    assertEquals(new Text(logicalEntry(BUCKET, lastParent)), textCaptor.getAllValues().get(3));
  }

  @Test
  public void testParentDirOfMultipleFiles() throws IOException, InterruptedException {
    // Prepare
    // file1
    UUID uuid1 = UUID.randomUUID();
    String phyPath1 = physicalEntry(BUCKET, uuid1);
    VersionedObjectHandle handle1 =
        buildVersionedObject(phyPath1, logicalEntry(BUCKET, "dir1/dir2/file1"), uuid1, true);
    // file2
    UUID uuid2 = UUID.randomUUID();
    String phyPath2 = physicalEntry(BUCKET, uuid2);
    VersionedObjectHandle handle2 =
        buildVersionedObject(phyPath2, logicalEntry(BUCKET, "dir1/dir2/file2"), uuid2, true);
    // file3
    UUID uuid3 = UUID.randomUUID();
    String phyPath3 = physicalEntry(BUCKET, uuid3);
    VersionedObjectHandle handle3 =
        buildVersionedObject(phyPath3, logicalEntry(BUCKET, "dir1/file3"), uuid3, true);
    // Act
    mapper.map(null, handle1, mockContext);
    mapper.map(null, handle2, mockContext);
    mapper.map(null, handle3, mockContext);
    // Verify
    String firstParent = "dir1/dir2";
    String secondParent = "dir1";
    ArgumentCaptor<Text> textCaptor = ArgumentCaptor.forClass(Text.class);
    // Verify how many dirs were written
    verify(mockContext, times(2)).write(textCaptor.capture(), any(NullWritable.class));

    // First parent (from the end)
    assertEquals(new Text(logicalEntry(BUCKET, firstParent)), textCaptor.getAllValues().get(0));

    // Second parent (from the end)
    assertEquals(new Text(logicalEntry(BUCKET, secondParent)), textCaptor.getAllValues().get(1));
  }

  @Test
  public void testDirectoryIsIgnored() throws IOException, InterruptedException {
    // Prepare
    ObjectMetadata metadata =
        ObjectMetadata.builder()
            .key(new Path(logicalEntry(BUCKET, "dir1/dir2")))
            .isDirectory(true)
            .creationTime(CREATION_TIME)
            .size(0)
            .build();
    VersionedObjectHandle handle =
        VersionedObject.builder().metadata(metadata).id(UUID.randomUUID()).version(VERSION).build();
    // Act
    mapper.map(null, handle, mockContext);
    // Verify
    verify(mockContext, never()).write(any(Text.class), any(NullWritable.class));
  }
}
