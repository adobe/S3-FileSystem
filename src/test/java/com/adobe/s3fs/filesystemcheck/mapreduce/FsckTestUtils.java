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

import com.adobe.s3fs.filesystemcheck.mapreduce.data.LogicalObjectWritable;
import com.adobe.s3fs.filesystemcheck.mapreduce.data.SourceType;
import com.adobe.s3fs.metastore.api.ObjectMetadata;
import com.adobe.s3fs.metastore.api.VersionedObjectHandle;
import com.adobe.s3fs.metastore.internal.dynamodb.versioning.VersionedObject;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.junit.Assert;

import java.util.UUID;

final class FsckTestUtils {

  static final String BUCKET = "bucket";
  static final long SIZE = 10L;
  static final long CREATION_TIME = 100L;
  static final int VERSION = 1;

  private FsckTestUtils() {
    throw new IllegalStateException("Non instantiable class!");
  }

  static String operationLogEntry(String bucket, UUID objectHandleId) {
    return String.format("s3://%s/%s.info", bucket, objectHandleId);
  }

  static String operationLogPrefix(UUID objectHandleId) {
    return String.format("%s.info", objectHandleId);
  }

  static String physicalEntry(String bucket, UUID objectHandleId) {
    return String.format("s3://%s/%s.id=%s", bucket, UUID.randomUUID(), objectHandleId);
  }

  static String logicalEntry(String bucket, String prefix) {
    return String.format("ks://%s/%s", bucket, prefix);
  }

  static UUID toUUID(Text text) {
    return UUID.fromString(text.toString());
  }

  static Text toText(UUID uuid) {
    return new Text(uuid.toString());
  }

  static void verifyWritable(
      LogicalObjectWritable writable,
      String expectedPhyPath,
      String expectedLogicalPath,
      SourceType expectedType,
      long expectedSize,
      long expectedCreationTime,
      int expectedVersion,
      boolean expectedIsDir,
      boolean pendingState) {
    Assert.assertEquals(expectedPhyPath, writable.getPhysicalPath());
    Assert.assertEquals(expectedLogicalPath, writable.getLogicalPath());
    Assert.assertEquals(expectedType, writable.getSourceType());
    Assert.assertEquals(expectedIsDir, writable.isDirectory());
    Assert.assertEquals(expectedSize, writable.getSize());
    Assert.assertEquals(expectedCreationTime, writable.getCreationTime());
    Assert.assertEquals(expectedVersion, writable.getVersion());
    Assert.assertEquals(pendingState, writable.isPendingState());
  }

  static VersionedObjectHandle buildVersionedObject(
      String phyPath, String logicalPath, UUID uuid, boolean physicalDataCommitted) {
    ObjectMetadata metadata =
        ObjectMetadata.builder()
            .key(new Path(logicalPath))
            .isDirectory(false)
            .creationTime(CREATION_TIME)
            .size(SIZE)
            .physicalPath(phyPath)
            .physicalDataCommitted(physicalDataCommitted)
            .build();
    return VersionedObject.builder().metadata(metadata).id(uuid).version(VERSION).build();
  }

  static LogicalObjectWritable metaWritable(String phyPath, String logicalPath, int version) {
    return new LogicalObjectWritable.Builder()
        .withSourceType(SourceType.FROM_METASTORE)
        .withPhysicalPath(phyPath)
        .withLogicalPath(logicalPath)
        .withSize(SIZE)
        .withCreationTime(CREATION_TIME)
        .withVersion(version)
        .isDirectory(false)
        .isPendingState(false) // don't care
        .build();
  }

  static LogicalObjectWritable opLogWritable(
      String logicalPath, String phyPath, boolean isPending, int version) {
    return new LogicalObjectWritable.Builder()
        .withSourceType(SourceType.FROM_OPLOG)
        .withLogicalPath(logicalPath)
        .withSize(SIZE)
        .withCreationTime(CREATION_TIME)
        .withVersion(version)
        .isDirectory(false)
        .isPendingState(isPending)
        .withPhysicalPath(phyPath)
        .build();
  }

  static LogicalObjectWritable opLogWritable(
      String logicalPath, String phyPath, boolean isPending) {
    return opLogWritable(logicalPath, phyPath, isPending, VERSION);
  }

  static LogicalObjectWritable s3Writable(String phyPath) {
    return new LogicalObjectWritable.Builder()
        .withSourceType(SourceType.FROM_S3)
        .withLogicalPath("")
        .withSize(SIZE)
        .withCreationTime(CREATION_TIME)
        .withVersion(VERSION)
        .isDirectory(false)
        .isPendingState(false)
        .withPhysicalPath(phyPath)
        .build();
  }
}
