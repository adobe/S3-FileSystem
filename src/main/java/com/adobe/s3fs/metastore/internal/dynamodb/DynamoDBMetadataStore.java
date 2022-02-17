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

package com.adobe.s3fs.metastore.internal.dynamodb;

import com.adobe.s3fs.metastore.api.MetadataStoreExtended;
import com.adobe.s3fs.metastore.api.ObjectHandle;
import com.adobe.s3fs.metastore.api.ObjectMetadata;
import com.adobe.s3fs.metastore.api.VersionedObjectHandle;
import com.adobe.s3fs.metastore.internal.dynamodb.operations.MetadataOperations;
import com.adobe.s3fs.metastore.internal.dynamodb.operations.MetadataTreeOperations;
import com.adobe.s3fs.metastore.internal.dynamodb.versioning.VersionedObject;
import com.google.common.base.Preconditions;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Iterables;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;

public class DynamoDBMetadataStore implements MetadataStoreExtended {

  private final MetadataOperations metadataOps;
  private final MetadataTreeOperations metadataTreeOps;

  private static final UUID ROOT_UUID = UUID.randomUUID();

  public DynamoDBMetadataStore(MetadataOperations metadataOps, MetadataTreeOperations metadataTreeOps) {
    this.metadataOps = Preconditions.checkNotNull(metadataOps);
    this.metadataTreeOps = Preconditions.checkNotNull(metadataTreeOps);
  }

  @Override
  public Iterable<? extends VersionedObjectHandle> recursiveList(Path key) {
    ObjectHandle root = getObject(key).orElseThrow(IllegalArgumentException::new);

    return FluentIterable.from(listChildObjects(root))
        .transformAndConcat(this::recursiveListHelper);
  }

  private Iterable<? extends VersionedObjectHandle> recursiveListHelper(VersionedObjectHandle object) {
    if (!object.metadata().isDirectory()) {
      return FluentIterable.of(object);
    }

    FluentIterable<? extends VersionedObjectHandle> children = FluentIterable.from(listChildObjects(object))
        .transformAndConcat(this::recursiveListHelper);
    return Iterables.concat(FluentIterable.of(object), children);
  }

  @Override
  public Iterable<? extends VersionedObjectHandle> scanPartition(Path key, int partitionIndex, int partitionCount) {
    return metadataOps.scan(key, partitionIndex, partitionCount);
  }

  @Override
  public ObjectHandle createObject(ObjectMetadata objectMetadata) {
    VersionedObject versionedObject = VersionedObject.builder()
        .metadata(objectMetadata)
        .id(UUID.randomUUID())
        .version(1)
        .build();
    if (!metadataOps.store(versionedObject)) {
      throw new RuntimeException("Error creating " + objectMetadata.getKey());
    }

    return versionedObject;
  }

  @Override
  public VersionedObjectHandle restoreVersionedObject(VersionedObjectHandle versionedObject) {
    if (!metadataOps.restoreVersionedObject((VersionedObject) versionedObject)) {
      throw new RuntimeException("Error creating " + versionedObject.metadata().getKey()); // NOSONAR
    }

    return versionedObject;
  }

  @Override
  public ObjectHandle updateObject(ObjectHandle currentHandle, ObjectMetadata newMetadata) {
    Preconditions.checkArgument(currentHandle instanceof VersionedObject);
    VersionedObject versionedObject = (VersionedObject) currentHandle;

    return metadataOps.update(versionedObject, newMetadata);
  }

  @Override
  public Optional<? extends ObjectHandle> getObject(Path key) {
    if (key.isRoot()) {
      ObjectMetadata rootMetadata = ObjectMetadata.builder().key(key).isDirectory(true).size(0L).creationTime(0L).build();
      VersionedObject rootObject = VersionedObject.builder()
          .metadata(rootMetadata)
          .id(ROOT_UUID)
          .version(1)
          .build();
      return Optional.of(rootObject);
    }
    return metadataOps.getObject(key);
  }

  @Override
  public boolean renameObject(ObjectHandle sourceObject, Path destinationKey) {
    Preconditions.checkArgument(sourceObject instanceof VersionedObject);
    VersionedObject source = (VersionedObject) sourceObject;

    if (!sourceObject.metadata().isDirectory()) {
      // single files can be renamed transactionally
      return metadataOps.renameFile(source, destinationKey);
    } else if (!metadataTreeOps.renameObjectTree(source, destinationKey)) {
      throw new RuntimeException(
          "Error renaming " + sourceObject.metadata().getKey() + " to " + destinationKey);
    }

    return true;
  }

  public boolean deleteObject(ObjectHandle object, Function<ObjectHandle, Boolean> callback) {
    Preconditions.checkArgument(object instanceof VersionedObject);
    VersionedObject versionedObject = (VersionedObject) object;

    boolean successful = false;
    if (object.metadata().isDirectory()) {
      successful = metadataTreeOps.deleteObjectTree(versionedObject, callback);
    } else {
      successful = metadataOps.delete(versionedObject) && callback.apply(versionedObject);
    }

    if (!successful) {
      throw new RuntimeException("Error deleting " + object.metadata().getKey());
    }

    return true;
  }

  @Override
  public boolean deleteSingleObject(VersionedObjectHandle object, Function<ObjectHandle, Boolean> callback) {
    return metadataOps.delete((VersionedObject) object) && callback.apply(object);
  }

  @Override
  public ContentSummary computeContentSummary(ObjectHandle objectHandle) {
    Preconditions.checkArgument(objectHandle instanceof VersionedObject);
    return metadataTreeOps.computeContentSummary((VersionedObject) objectHandle);
  }

  @Override
  public Iterable<? extends VersionedObjectHandle> listChildObjects(ObjectHandle object) {
    Preconditions.checkArgument(object instanceof VersionedObject);
    return metadataOps.list((VersionedObject) object);
  }

  @Override
  public void close() throws IOException {
    metadataOps.close();
  }

}
