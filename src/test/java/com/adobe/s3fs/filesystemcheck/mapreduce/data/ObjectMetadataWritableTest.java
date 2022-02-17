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

package com.adobe.s3fs.filesystemcheck.mapreduce.data;

import com.adobe.s3fs.metastore.api.ObjectMetadata;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.*;

import static com.adobe.s3fs.filesystem.FileSystemImplementation.UNCOMMITED_PATH_MARKER;

public class ObjectMetadataWritableTest {

  private ByteArrayOutputStream out;
  private DataOutputStream dos;

  @Before
  public void setup() {
    out = new ByteArrayOutputStream();
    dos = new DataOutputStream(out);
  }

  @Test
  public void testSerialization() throws IOException {
    ObjectMetadata metadata =
        ObjectMetadata.builder()
            .key(new Path("ks://bucket/dir1/dir2/file"))
            .isDirectory(false)
            .creationTime(100L)
            .size(1000L)
            .physicalDataCommitted(true)
            .physicalPath("s3n://bucket/somethinng.id=handleID")
            .build();
    ObjectMetadataWritable metadataWritable = new ObjectMetadataWritable(metadata);
    metadataWritable.write(dos);
    // serialize
    DataInputStream dis = new DataInputStream(new ByteArrayInputStream(out.toByteArray()));
    // Verify
    Assert.assertEquals(new Path("ks://bucket/dir1/dir2/file"), new Path(dis.readUTF()));
    Assert.assertFalse(dis.readBoolean());
    Assert.assertEquals(1000L, dis.readLong());
    Assert.assertEquals(100L, dis.readLong());
    Assert.assertTrue(dis.readBoolean());
    Assert.assertEquals("s3n://bucket/somethinng.id=handleID", dis.readUTF());
  }

  @Test
  public void testSerializationWithNoPhyDataCommitted() throws IOException {
    ObjectMetadata metadata =
        ObjectMetadata.builder()
            .key(new Path("ks://bucket/dir1/dir2/file"))
            .isDirectory(true)
            .creationTime(100L)
            .size(0L)
            .physicalDataCommitted(false)
            .build();
    ObjectMetadataWritable metadataWritable = new ObjectMetadataWritable(metadata);
    metadataWritable.write(dos);
    // serialize
    DataInputStream dis = new DataInputStream(new ByteArrayInputStream(out.toByteArray()));
    // Verify
    Assert.assertEquals(new Path("ks://bucket/dir1/dir2/file"), new Path(dis.readUTF()));
    Assert.assertTrue(dis.readBoolean());
    Assert.assertEquals(0L, dis.readLong());
    Assert.assertEquals(100L, dis.readLong());
    Assert.assertFalse(dis.readBoolean());
  }

  @Test
  public void testDeserializationWithSameReference() throws IOException {
    ObjectMetadataWritable metadataWritable = new ObjectMetadataWritable();
    // Create input buffer 1 with phyDataCommitted = false
    Path path1 = new Path("ks://bucket/dir1/dir2/file");
    boolean isDirectory1 = true;
    long size1 = 0L;
    long creationTime1 = 100L;
    boolean phyDataCommitted1 = false;
    dos.writeUTF(path1.toString());
    dos.writeBoolean(isDirectory1);
    dos.writeLong(size1);
    dos.writeLong(creationTime1);
    dos.writeBoolean(phyDataCommitted1);
    // Create input buffer 2 with phyDataCommitted = true
    Path path2 = new Path("ks://bucket/dir1/dir2/file");
    boolean isDirectory2 = false;
    long size2 = 1000L;
    long creationTime2 = 100L;
    boolean phyDataCommitted2 = true;
    String phyPath2 = "s3n://bucket/something.id=handleID";
    dos.writeUTF(path2.toString());
    dos.writeBoolean(isDirectory2);
    dos.writeLong(size2);
    dos.writeLong(creationTime2);
    dos.writeBoolean(phyDataCommitted2);
    dos.writeUTF(phyPath2);
    // Deserialize
    DataInputStream dis = new DataInputStream(new ByteArrayInputStream(out.toByteArray()));
    metadataWritable.readFields(dis);
    // Verify first batch
    Assert.assertEquals(path1, metadataWritable.getMetadata().getKey());
    Assert.assertEquals(isDirectory1, metadataWritable.getMetadata().isDirectory());
    Assert.assertEquals(size1, metadataWritable.getMetadata().getSize());
    Assert.assertEquals(creationTime1, metadataWritable.getMetadata().getCreationTime());
    Assert.assertEquals(phyDataCommitted1, metadataWritable.getMetadata().physicalDataCommitted());
    Assert.assertFalse(metadataWritable.getMetadata().getPhysicalPath().isPresent());
    // Verify second batch
    metadataWritable.readFields(dis);
    Assert.assertEquals(path2, metadataWritable.getMetadata().getKey());
    Assert.assertEquals(isDirectory2, metadataWritable.getMetadata().isDirectory());
    Assert.assertEquals(size2, metadataWritable.getMetadata().getSize());
    Assert.assertEquals(creationTime2, metadataWritable.getMetadata().getCreationTime());
    Assert.assertEquals(phyDataCommitted2, metadataWritable.getMetadata().physicalDataCommitted());
    Assert.assertEquals(phyPath2, metadataWritable.getMetadata().getPhysicalPath().get());
  }

  @Test
  public void testDeserialization() throws IOException {
    ObjectMetadataWritable metadataWritable = new ObjectMetadataWritable();
    // Create input buffer
    Path path = new Path("ks://bucket/dir1/dir2/file");
    boolean isDirectory = false;
    long size = 1000L;
    long creationTime = 100L;
    boolean phyDataCommitted = true;
    String phyPath = "s3n://bucket/something.id=handleID";
    dos.writeUTF(path.toString());
    dos.writeBoolean(isDirectory);
    dos.writeLong(size);
    dos.writeLong(creationTime);
    dos.writeBoolean(phyDataCommitted);
    dos.writeUTF(phyPath);
    // Deserialize
    metadataWritable.readFields(new DataInputStream(new ByteArrayInputStream(out.toByteArray())));
    // Verify
    Assert.assertEquals(path, metadataWritable.getMetadata().getKey());
    Assert.assertEquals(isDirectory, metadataWritable.getMetadata().isDirectory());
    Assert.assertEquals(size, metadataWritable.getMetadata().getSize());
    Assert.assertEquals(creationTime, metadataWritable.getMetadata().getCreationTime());
    Assert.assertEquals(phyDataCommitted, metadataWritable.getMetadata().physicalDataCommitted());
    Assert.assertEquals(phyPath, metadataWritable.getMetadata().getPhysicalPath().get());
  }

  @Test
  public void testDeserializationWithNoPhysicalPath() throws IOException {
    ObjectMetadataWritable metadataWritable = new ObjectMetadataWritable();
    // Create input buffer
    Path path = new Path("ks://bucket/dir1/dir2/file");
    boolean isDirectory = true;
    long size = 0L;
    long creationTime = 100L;
    boolean phyDataCommitted = false;
    String phyPath = "s3n://bucket/something.id=handleID";
    dos.writeUTF(path.toString());
    dos.writeBoolean(isDirectory);
    dos.writeLong(size);
    dos.writeLong(creationTime);
    dos.writeBoolean(phyDataCommitted);
    dos.writeUTF(phyPath); // rogue write
    // Deserialize
    metadataWritable.readFields(new DataInputStream(new ByteArrayInputStream(out.toByteArray())));
    // Verify
    Assert.assertEquals(path, metadataWritable.getMetadata().getKey());
    Assert.assertEquals(isDirectory, metadataWritable.getMetadata().isDirectory());
    Assert.assertEquals(size, metadataWritable.getMetadata().getSize());
    Assert.assertEquals(creationTime, metadataWritable.getMetadata().getCreationTime());
    Assert.assertEquals(phyDataCommitted, metadataWritable.getMetadata().physicalDataCommitted());
    Assert.assertFalse(metadataWritable.getMetadata().getPhysicalPath().isPresent());
  }

  @Test
  public void testDeserializationWithUncommittedPhysicalPath() throws IOException {
    ObjectMetadataWritable metadataWritable = new ObjectMetadataWritable();
    // Create input buffer
    Path path = new Path("ks://bucket/dir1/dir2/file");
    boolean isDirectory = false;
    long size = 0L;
    long creationTime = 100L;
    boolean phyDataCommitted = false;
    dos.writeUTF(path.toString());
    dos.writeBoolean(isDirectory);
    dos.writeLong(size);
    dos.writeLong(creationTime);
    dos.writeBoolean(phyDataCommitted);
    dos.writeUTF(UNCOMMITED_PATH_MARKER);
    // Deserialize
    metadataWritable.readFields(new DataInputStream(new ByteArrayInputStream(out.toByteArray())));
    // Verify
    Assert.assertEquals(path, metadataWritable.getMetadata().getKey());
    Assert.assertEquals(isDirectory, metadataWritable.getMetadata().isDirectory());
    Assert.assertEquals(size, metadataWritable.getMetadata().getSize());
    Assert.assertEquals(creationTime, metadataWritable.getMetadata().getCreationTime());
    Assert.assertEquals(phyDataCommitted, metadataWritable.getMetadata().physicalDataCommitted());
    Assert.assertTrue(metadataWritable.getMetadata().getPhysicalPath().isPresent());
    Assert.assertEquals(UNCOMMITED_PATH_MARKER, metadataWritable.getMetadata().getPhysicalPath().get());
  }
}
