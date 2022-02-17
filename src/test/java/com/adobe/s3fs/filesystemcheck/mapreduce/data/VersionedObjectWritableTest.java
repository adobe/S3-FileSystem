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
import com.adobe.s3fs.metastore.internal.dynamodb.versioning.VersionedObject;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.util.UUID;

public class VersionedObjectWritableTest {

  private ByteArrayOutputStream out;
  private DataOutputStream dos;

  @Before
  public void setup() {
    out = new ByteArrayOutputStream();
    dos = new DataOutputStream(out);
  }

  @Test
  public void testSerialize() throws IOException {
    ObjectMetadata metadata =
        ObjectMetadata.builder()
            .key(new Path("ks://bucket/dir1/dir2/file"))
            .isDirectory(false)
            .creationTime(100L)
            .size(1000L)
            .physicalDataCommitted(true)
            .physicalPath("s3n://bucket/somethinng.id=handleID")
            .build();
    UUID id = UUID.randomUUID();
    VersionedObject versionedObject =
        VersionedObject.builder().metadata(metadata).version(2).id(id).build();
    VersionedObjectWritable versionedObjectWritable = new VersionedObjectWritable(versionedObject);
    versionedObjectWritable.write(dos);
    // Verify
    DataInputStream dis = new DataInputStream(new ByteArrayInputStream(out.toByteArray()));
    Assert.assertEquals(new Path("ks://bucket/dir1/dir2/file"), new Path(dis.readUTF()));
    Assert.assertFalse(dis.readBoolean());
    Assert.assertEquals(1000L, dis.readLong());
    Assert.assertEquals(100L, dis.readLong());
    Assert.assertTrue(dis.readBoolean());
    Assert.assertEquals("s3n://bucket/somethinng.id=handleID", dis.readUTF());
    Assert.assertEquals(2, dis.readInt());
    Assert.assertEquals(id, UUID.fromString(dis.readUTF()));
  }

  @Test
  public void testDeserialize() throws IOException {
    VersionedObjectWritable versionedObjectWritable = new VersionedObjectWritable();
    // Create input buffer
    UUID id = UUID.randomUUID();
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
    dos.writeInt(5);
    dos.writeUTF(id.toString());
    // Deserialize
    DataInputStream dis = new DataInputStream(new ByteArrayInputStream(out.toByteArray()));
    versionedObjectWritable.readFields(dis);
    // Verify
    Assert.assertEquals(path, versionedObjectWritable.getVersionedObject().metadata().getKey());
    Assert.assertEquals(isDirectory, versionedObjectWritable.getVersionedObject().metadata().isDirectory());
    Assert.assertEquals(size, versionedObjectWritable.getVersionedObject().metadata().getSize());
    Assert.assertEquals(creationTime, versionedObjectWritable.getVersionedObject().metadata().getCreationTime());
    Assert.assertEquals(phyDataCommitted, versionedObjectWritable.getVersionedObject().metadata().physicalDataCommitted());
    Assert.assertEquals(phyPath, versionedObjectWritable.getVersionedObject().metadata().getPhysicalPath().get());
    Assert.assertEquals(5, versionedObjectWritable.getVersionedObject().version());
    Assert.assertEquals(id, versionedObjectWritable.getVersionedObject().id());
  }

  @Test
  public void testDeserializeWithSameReference() throws IOException {
    VersionedObjectWritable versionedObjectWritable = new VersionedObjectWritable();
    // Create input buffer with no phy path committed
    UUID id1 = UUID.randomUUID();
    Path path1 = new Path("ks://bucket/dir1/dir2/file1");
    boolean isDirectory1 = true;
    long size1 = 0L;
    long creationTime1 = 100L;
    boolean phyDataCommitted1 = false;
    dos.writeUTF(path1.toString());
    dos.writeBoolean(isDirectory1);
    dos.writeLong(size1);
    dos.writeLong(creationTime1);
    dos.writeBoolean(phyDataCommitted1);
    dos.writeInt(6);
    dos.writeUTF(id1.toString());
    // Create input buffer with phy path committed
    UUID id2 = UUID.randomUUID();
    Path path2 = new Path("ks://bucket/dir1/dir2/file2");
    boolean isDirectory2 = false;
    long size2 = 1000L;
    long creationTime2 = 100L;
    boolean phyDataCommitted2 = true;
    String phyPath2 = "s3n://bucket/something2.id=handleID";
    dos.writeUTF(path2.toString());
    dos.writeBoolean(isDirectory2);
    dos.writeLong(size2);
    dos.writeLong(creationTime2);
    dos.writeBoolean(phyDataCommitted2);
    dos.writeUTF(phyPath2);
    dos.writeInt(5);
    dos.writeUTF(id2.toString());
    // Deserialize
    DataInputStream dis = new DataInputStream(new ByteArrayInputStream(out.toByteArray()));
    versionedObjectWritable.readFields(dis);
    // Verify first batch
    Assert.assertEquals(path1, versionedObjectWritable.getVersionedObject().metadata().getKey());
    Assert.assertEquals(isDirectory1, versionedObjectWritable.getVersionedObject().metadata().isDirectory());
    Assert.assertEquals(size1, versionedObjectWritable.getVersionedObject().metadata().getSize());
    Assert.assertEquals(creationTime1, versionedObjectWritable.getVersionedObject().metadata().getCreationTime());
    Assert.assertEquals(phyDataCommitted1, versionedObjectWritable.getVersionedObject().metadata().physicalDataCommitted());
    Assert.assertFalse(versionedObjectWritable.getVersionedObject().metadata().getPhysicalPath().isPresent());
    Assert.assertEquals(6, versionedObjectWritable.getVersionedObject().version());
    Assert.assertEquals(id1, versionedObjectWritable.getVersionedObject().id());
    // Verify second batch
    versionedObjectWritable.readFields(dis);
    Assert.assertEquals(path2, versionedObjectWritable.getVersionedObject().metadata().getKey());
    Assert.assertEquals(isDirectory2, versionedObjectWritable.getVersionedObject().metadata().isDirectory());
    Assert.assertEquals(size2, versionedObjectWritable.getVersionedObject().metadata().getSize());
    Assert.assertEquals(creationTime2, versionedObjectWritable.getVersionedObject().metadata().getCreationTime());
    Assert.assertEquals(phyDataCommitted2, versionedObjectWritable.getVersionedObject().metadata().physicalDataCommitted());
    Assert.assertEquals(phyPath2, versionedObjectWritable.getVersionedObject().metadata().getPhysicalPath().get());
    Assert.assertEquals(5, versionedObjectWritable.getVersionedObject().version());
    Assert.assertEquals(id2, versionedObjectWritable.getVersionedObject().id());
  }
}
