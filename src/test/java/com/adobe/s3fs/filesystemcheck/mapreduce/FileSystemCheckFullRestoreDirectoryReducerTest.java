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

import com.adobe.s3fs.filesystemcheck.mapreduce.data.VersionedObjectWritable;
import com.adobe.s3fs.filesystemcheck.mapreduce.multioutputs.MultiOutputsFactory;
import com.adobe.s3fs.metastore.internal.dynamodb.versioning.VersionedObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.util.Collections;

import static com.adobe.s3fs.filesystemcheck.mapreduce.FileSystemMRJobConfig.*;
import static com.adobe.s3fs.filesystemcheck.mapreduce.FsckTestUtils.BUCKET;
import static com.adobe.s3fs.filesystemcheck.mapreduce.FsckTestUtils.logicalEntry;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class FileSystemCheckFullRestoreDirectoryReducerTest {

  @Mock private MultipleOutputs mockMultipleOutputs;

  @Mock private MultiOutputsFactory mockMultiOutputsFactory;

  @Mock
  private Reducer<Text, NullWritable, NullWritable, NullWritable>.Context mockContext;

  @Mock private Counter mockCounter;

  private Configuration config;

  private FileSystemCheckFullRestoreDirectoryReducer reducer;

  private static void verifyMetadata(VersionedObject versionedObject, String logicalPath) {
    assertTrue(versionedObject.metadata().isDirectory());
    assertEquals(new Path(logicalPath), versionedObject.metadata().getKey());
    assertEquals(0L, versionedObject.metadata().getSize());
    assertFalse(versionedObject.metadata().getPhysicalPath().isPresent());
    assertFalse(versionedObject.metadata().physicalDataCommitted());
    assertEquals(1, versionedObject.version());
    assertNotNull(versionedObject.id());
  }

  @Before
  public void setup() throws IOException, InterruptedException {
    MockitoAnnotations.initMocks(this);

    config = new Configuration(true);
    config.set(LOGICAL_ROOT_PATH, "ks://" + BUCKET);

    when(mockContext.getConfiguration()).thenReturn(config);
    when(mockContext.getCounter(any(Enum.class))).thenReturn(mockCounter);

    when(mockMultiOutputsFactory.newMultipleOutputs(eq(mockContext))).thenReturn(mockMultipleOutputs);

    reducer = new FileSystemCheckFullRestoreDirectoryReducer(mockMultiOutputsFactory);

    reducer.setup(mockContext);
  }

  @Test
  public void testDirectoryIsRecovered() throws IOException, InterruptedException {
    // Prepare
    Text key = new Text(logicalEntry(BUCKET, "dir1/dir2"));
    // Act
    reducer.reduce(key, Collections.emptyList(), mockContext);
    // Verify
    ArgumentCaptor<VersionedObjectWritable> captor =
        ArgumentCaptor.forClass(VersionedObjectWritable.class);
    verify(mockMultipleOutputs, times(1))
        .write(
            eq(METASTORE_OUTPUT_NAME),
            eq(new Text("restoreObject")),
            captor.capture(),
            eq(METASTORE_OUTPUT_SUFFIX));
    verifyMetadata(captor.getValue().getVersionedObject(), logicalEntry(BUCKET, "dir1/dir2"));
  }
}
