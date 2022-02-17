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

package com.adobe.s3fs.metastore.internal.dynamodb.versioning;

import com.adobe.s3fs.metastore.api.ObjectMetadata;
import com.adobe.s3fs.metastore.api.VersionedObjectHandle;

import com.google.common.base.Preconditions;
import org.apache.hadoop.fs.Path;
import org.immutables.value.Value;

import java.util.UUID;

@Value.Immutable
@Value.Style(strictBuilder = true, typeImmutable = "*")
public abstract class AbstractVersionedObject implements VersionedObjectHandle {

  public VersionedObject moveTo(Path destination) {
    ObjectMetadata newMetadata = metadata().withKey(destination);
    return VersionedObject.builder()
        .metadata(newMetadata)
        .version(version() + 1)
        .id(id())
        .build();
  }

  public VersionedObject updated(ObjectMetadata newMetadata) {
    return VersionedObject.builder()
        .metadata(newMetadata)
        .version(version() + 1)
        .id(id())
        .build();
  }

  @Value.Check
  protected void validate() {
    Preconditions.checkState(version() > 0, "Version must greater than 0");
  }
}
