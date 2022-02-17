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

package com.adobe.s3fs.operationlog;

import com.adobe.s3fs.metastore.api.ObjectMetadata;
import com.adobe.s3fs.metastore.api.OperationLogEntryState;
import com.adobe.s3fs.metastore.api.ObjectOperationType;
import com.adobe.s3fs.metastore.api.VersionedObjectHandle;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.util.UUID;

public final class ObjectMetadataSerialization {

  private ObjectMetadataSerialization() {
    // no op
  }

  public static final byte VERSION = 1;
  public static final byte VERSION_2 = 2;

  public static void serializeTo(ObjectMetadata objectMetadata, OutputStream destination) throws IOException {
    LogicalFileMetadata logicalFileMetadata = new LogicalFileMetadata();

    logicalFileMetadata.setLogicalPath(objectMetadata.getKey().toString());
    logicalFileMetadata.setSize(objectMetadata.getSize());
    logicalFileMetadata.setCreationTime(objectMetadata.getCreationTime());
    logicalFileMetadata.setPhysicalDataCommitted(objectMetadata.physicalDataCommitted());

    ObjectOutputStream objectOutputStream = new ObjectOutputStream(destination);

    objectOutputStream.write(VERSION); // in case we need to evolve the schema
    logicalFileMetadata.writeExternal(objectOutputStream);
    objectOutputStream.flush();
  }

  public static void serializeObjectMetadataToV2(ObjectMetadata objectMetadata,
                                                 UUID id,
                                                 int version,
                                                 OperationLogEntryState state,
                                                 ObjectOperationType type,
                                                 OutputStream destination) throws IOException {
    LogicalFileMetadataV2 logicalFileMetadataV3 = new LogicalFileMetadataV2();

    logicalFileMetadataV3.setPhysicalPath(objectMetadata.getPhysicalPath().get()); // NOSONAR
    logicalFileMetadataV3.setLogicalPath(objectMetadata.getKey().toString());
    logicalFileMetadataV3.setSize(objectMetadata.getSize());
    logicalFileMetadataV3.setCreationTime(objectMetadata.getCreationTime());
    logicalFileMetadataV3.setPhysicalDataCommitted(objectMetadata.physicalDataCommitted());
    logicalFileMetadataV3.setVersion(version);
    logicalFileMetadataV3.setId(id.toString());
    logicalFileMetadataV3.setState(state);
    logicalFileMetadataV3.setType(type);

    ObjectOutputStream objectOutputStream = new ObjectOutputStream(destination);
    objectOutputStream.write(VERSION_2);
    logicalFileMetadataV3.writeExternal(objectOutputStream);
    objectOutputStream.flush();
  }

  public static void serializeToV2(VersionedObjectHandle objectHandle,
                                   OperationLogEntryState state,
                                   ObjectOperationType type,
                                   OutputStream destination) throws IOException {
    LogicalFileMetadataV2 logicalFileMetadataV3 = new LogicalFileMetadataV2();

    logicalFileMetadataV3.setPhysicalPath(objectHandle.metadata().getPhysicalPath().get());
    logicalFileMetadataV3.setLogicalPath(objectHandle.metadata().getKey().toString());
    logicalFileMetadataV3.setSize(objectHandle.metadata().getSize());
    logicalFileMetadataV3.setCreationTime(objectHandle.metadata().getCreationTime());
    logicalFileMetadataV3.setPhysicalDataCommitted(objectHandle.metadata().physicalDataCommitted());
    logicalFileMetadataV3.setVersion(objectHandle.version());
    logicalFileMetadataV3.setId(objectHandle.id().toString());
    logicalFileMetadataV3.setState(state);
    logicalFileMetadataV3.setType(type);

    ObjectOutputStream objectOutputStream = new ObjectOutputStream(destination);
    objectOutputStream.write(VERSION_2);
    logicalFileMetadataV3.writeExternal(objectOutputStream);
    objectOutputStream.flush();
  }

  public static LogicalFileMetadataV2 deserializeFromV2(InputStream inputStream) throws IOException {
    ObjectInputStream objectInputStream = new ObjectInputStream(inputStream);

    int ignoredVersion = objectInputStream.read();

    LogicalFileMetadataV2 logicalFileMetadata = new LogicalFileMetadataV2();
    logicalFileMetadata.readExternal(objectInputStream);

    return logicalFileMetadata;
  }

  public static LogicalFileMetadata deserializeFrom(InputStream inputStream) throws IOException {
    ObjectInputStream objectInputStream = new ObjectInputStream(inputStream);

    int ignoredVersion = objectInputStream.read();

    LogicalFileMetadata logicalFileMetadata = new LogicalFileMetadata();
    logicalFileMetadata.readExternal(objectInputStream);

    return logicalFileMetadata;
  }
}
