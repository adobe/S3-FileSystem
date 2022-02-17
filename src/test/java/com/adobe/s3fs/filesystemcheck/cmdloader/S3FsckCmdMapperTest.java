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

import com.adobe.s3fs.utils.mapreduce.TextArrayWritable;
import com.amazonaws.services.s3.AmazonS3;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.IOException;

import static org.mockito.Mockito.*;
import static org.mockito.Mockito.times;

public class S3FsckCmdMapperTest {

  private static final String BUCKET = "bucket";

  private Configuration config;

  @Mock private AmazonS3 mockS3Client;

  @Mock private Counter mockCounter;

  @Mock private Mapper<Text, TextArrayWritable, NullWritable, NullWritable>.Context mockContext;

  private S3FsckCmdMapper mapper;

  @Before
  public void setup() throws IOException, InterruptedException {
    MockitoAnnotations.initMocks(this);
    config = new Configuration(true);
    config.setBoolean("dryRun", false);
    when(mockContext.getConfiguration()).thenReturn(config);
    when(mockContext.getCounter(any(CmdLoaderCounters.class))).thenReturn(mockCounter);
    mapper = new S3FsckCmdMapper((retryPolicy, maxConnections) -> mockS3Client);
    mapper.setup(mockContext);
  }

  @Test(expected = IllegalStateException.class)
  public void testOptionWhichIsNotRegistered() throws IOException, InterruptedException {
    mapper.map(new Text("unknownCommand"), new TextArrayWritable(new Text[0]), mockContext);
  }

  @Test
  public void testWithKnownOption() throws IOException, InterruptedException {
    Text[] params = new Text[]{new Text(BUCKET), new Text("key")};
    mapper.map(new Text("deleteObject"), new TextArrayWritable(params), mockContext);
    // Verify
    verify(mockS3Client, times(1)).deleteObject(eq(BUCKET), eq("key"));
  }

  @Test
  public void testWithLongPrefixKey() throws IOException, InterruptedException {
    Text[] params = new Text[]{new Text(BUCKET), new Text("dir1/dir2/dir3/key")};
    mapper.map(new Text("deleteObject"), new TextArrayWritable(params), mockContext);
    // Verify
    verify(mockS3Client, times(1)).deleteObject(eq(BUCKET), eq("dir1/dir2/dir3/key"));
  }
}
