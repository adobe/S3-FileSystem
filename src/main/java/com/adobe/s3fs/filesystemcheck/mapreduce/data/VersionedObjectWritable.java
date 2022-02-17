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

import com.adobe.s3fs.metastore.internal.dynamodb.versioning.VersionedObject;
import com.google.common.base.Preconditions;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.UUID;

public class VersionedObjectWritable implements Writable {

  private VersionedObject versionedObject;

  // Needed by hadoop framework
  public VersionedObjectWritable() {
    this.versionedObject = null;
  }

  public VersionedObjectWritable(VersionedObject versionedObject) {
    this.versionedObject = Preconditions.checkNotNull(versionedObject);
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    ObjectMetadataWritable metadata = new ObjectMetadataWritable(versionedObject.metadata());
    metadata.write(dataOutput);
    dataOutput.writeInt(versionedObject.version());
    dataOutput.writeUTF(versionedObject.id().toString());
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    ObjectMetadataWritable metadataWritable = new ObjectMetadataWritable();
    metadataWritable.readFields(dataInput);
    int version = dataInput.readInt();
    UUID uuid = UUID.fromString(dataInput.readUTF());
    versionedObject =
        VersionedObject.builder()
            .metadata(metadataWritable.getMetadata())
            .version(version)
            .id(uuid)
            .build();
  }

  public VersionedObject getVersionedObject() {
    return versionedObject;
  }

  @Override
  public String toString() {
    return "{" + versionedObject + '}';
  }

  public void clear() {
    this.versionedObject = null;
  }
}
