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

package com.adobe.s3fs.filesystemcheck.cmdloader;

import com.adobe.s3fs.filesystemcheck.mapreduce.data.VersionedObjectWritable;
import com.adobe.s3fs.metastore.api.MetadataStoreExtended;
import com.adobe.s3fs.metastore.api.ObjectMetadata;
import com.adobe.s3fs.metastore.internal.dynamodb.versioning.VersionedObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

public class MetastoreFsckCmdMapperTest {

  @Mock
  private Mapper<Text, VersionedObjectWritable, NullWritable, NullWritable>.Context mockContext;

  @Mock private MetadataStoreExtended mockMetastore;

  @Mock private Counter mockCounter;

  private Configuration config;

  private MetastoreFsckCmdMapper mapper;

  @Before
  public void setup() throws IOException, InterruptedException {
    MockitoAnnotations.initMocks(this);
    config = new Configuration();
    config.setBoolean("dryRun", false);
    config.set("fs.s3k.cmd.loader.s3.bucket", "Bucket");
    when(mockContext.getConfiguration()).thenReturn(config);

    when(mockContext.getCounter(any(CmdLoaderCounters.class))).thenReturn(mockCounter);
    mapper = new MetastoreFsckCmdMapper((ignoredConfig, ignoredBucket) -> mockMetastore);
    mapper.setup(mockContext);
  }

  @Test(expected = IllegalStateException.class)
  public void testUnknownOption() throws IOException, InterruptedException {
    mapper.map(new Text("unknownOption"), Mockito.mock(VersionedObjectWritable.class), mockContext);
  }

  @Test
  public void testKnownOptionWithParameters() throws IOException, InterruptedException {
    VersionedObjectWritable mockValue = Mockito.mock(VersionedObjectWritable.class);
    ObjectMetadata meta =
        ObjectMetadata.builder()
            .creationTime(100L)
            .isDirectory(false)
            .physicalDataCommitted(true)
            .physicalPath("s3n://bucket/somePath")
            .key(new Path("ks://someKey"))
            .size(1000L)
            .build();
    VersionedObject innerValue =
        VersionedObject.builder().id(UUID.randomUUID()).version(1).metadata(meta).build();
    when(mockValue.getVersionedObject()).thenReturn(innerValue);
    mapper.map(new Text("restoreObject"), mockValue, mockContext);
    // Verify
    Mockito.verify(mockMetastore, Mockito.times(1)).restoreVersionedObject(Mockito.eq(innerValue));
  }
}
