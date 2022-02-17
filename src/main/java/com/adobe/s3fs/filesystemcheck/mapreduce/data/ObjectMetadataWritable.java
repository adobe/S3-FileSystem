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
import com.google.common.base.Preconditions;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Optional;

import static com.adobe.s3fs.filesystem.FileSystemImplementation.UNCOMMITED_PATH_MARKER;

public class ObjectMetadataWritable implements Writable {

  private ObjectMetadata metadata;

  // Needed by Hadoop framework
  public ObjectMetadataWritable() {
    this.metadata = null;
  }

  public ObjectMetadataWritable(ObjectMetadata metadata) {
    this.metadata = Preconditions.checkNotNull(metadata);
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    dataOutput.writeUTF(metadata.getKey().toString());
    dataOutput.writeBoolean(metadata.isDirectory());
    dataOutput.writeLong(metadata.getSize());
    dataOutput.writeLong(metadata.getCreationTime());
    dataOutput.writeBoolean(metadata.physicalDataCommitted());
    if (metadata.physicalDataCommitted()) {
      dataOutput.writeUTF(metadata.getPhysicalPath().get()); // NOSONAR
    }
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    String key = dataInput.readUTF();
    boolean isDirectory = dataInput.readBoolean();
    long size = dataInput.readLong();
    long creationTime = dataInput.readLong();
    boolean phyDataCommitted = dataInput.readBoolean();
    Optional<String> phyPath = Optional.empty();
    if (phyDataCommitted) {
      phyPath = Optional.of(dataInput.readUTF());
    } else {
      if (!isDirectory) { // phyDataCommitted == false and it's a file
        phyPath = Optional.of(UNCOMMITED_PATH_MARKER);
      }
    }
    metadata =
        ObjectMetadata.builder()
            .key(new Path(key))
            .isDirectory(isDirectory)
            .size(size)
            .creationTime(creationTime)
            .physicalDataCommitted(phyDataCommitted)
            .physicalPath(phyPath)
            .build();
  }

  ObjectMetadata getMetadata() {
    return metadata;
  }

  @Override
  public String toString() {
    return "{" + metadata + '}';
  }
}
