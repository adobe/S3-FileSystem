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

package com.adobe.s3fs.utils;

import com.adobe.s3fs.metastore.api.MetadataOperationLog;
import com.adobe.s3fs.metastore.api.ObjectMetadata;
import com.adobe.s3fs.metastore.api.VersionedObjectHandle;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;

public class InMemoryMetadataOperationLog implements MetadataOperationLog {

  private final ConcurrentLinkedQueue<ObjectMetadata> createdObjects = new ConcurrentLinkedQueue<>();
  private final ConcurrentLinkedQueue<ObjectMetadata> deletedObjects = new ConcurrentLinkedQueue<>();
  private final ConcurrentLinkedQueue<AbstractMap.SimpleImmutableEntry<ObjectMetadata, ObjectMetadata>> renamedObjects =
      new ConcurrentLinkedQueue<>();
  private final ConcurrentLinkedQueue<AbstractMap.SimpleImmutableEntry<ObjectMetadata, ObjectMetadata>> updatedObjects =
          new ConcurrentLinkedQueue<>();

  @Override
  public Optional<LogEntryHandle> logCreateOperation(VersionedObjectHandle object) {
    createdObjects.add(object.metadata());
    return Optional.of(new LogEntryHandle() {
      @Override
      public boolean rollback() {
        return createdObjects.remove(object.metadata());
      }

      @Override
      public boolean commit() {
        return true;
      }
    });
  }

  @Override
  public CompletableFuture<Optional<LogEntryHandle>> logCreateOperationAsync(VersionedObjectHandle object) {
    return CompletableFuture.completedFuture(logCreateOperation(object));
  }

  @Override
  public Optional<LogEntryHandle> logUpdateOperation(VersionedObjectHandle sourceObject, VersionedObjectHandle destinationObject) {
    if (sourceObject.metadata().getKey().equals(destinationObject.metadata().getKey())) {
      updatedObjects.add(new AbstractMap.SimpleImmutableEntry<>(sourceObject.metadata(), destinationObject.metadata()));
      return Optional.of(new LogEntryHandle() {
        @Override
        public boolean rollback() {
          return updatedObjects.remove(new AbstractMap.SimpleImmutableEntry<>(sourceObject.metadata(), destinationObject.metadata()));
        }

        @Override
        public boolean commit() {
          return true;
        }
      });
    }

    // rename case
    renamedObjects.add(new AbstractMap.SimpleImmutableEntry<>(sourceObject.metadata(), destinationObject.metadata()));
    return Optional.of(new LogEntryHandle() {
      @Override
      public boolean rollback() {
        return renamedObjects.remove(new AbstractMap.SimpleImmutableEntry<>(sourceObject.metadata(), destinationObject.metadata()));
      }

      @Override
      public boolean commit() {
        return true;
      }
    });
  }

  @Override
  public CompletableFuture<Optional<LogEntryHandle>> logUpdateOperationAsync(VersionedObjectHandle sourceObject, VersionedObjectHandle destinationObject) {
    return CompletableFuture.completedFuture(logUpdateOperation(sourceObject, destinationObject));
  }

  @Override
  public Optional<LogEntryHandle> logDeleteOperation(VersionedObjectHandle object) {
    deletedObjects.add(object.metadata());
    return Optional.of(new LogEntryHandle() {
      @Override
      public boolean rollback() {
        return deletedObjects.remove(object.metadata());
      }

      @Override
      public boolean commit() {
        return false;
      }
    });
  }

  @Override
  public CompletableFuture<Optional<LogEntryHandle>> logDeleteOperationAsync(VersionedObjectHandle object) {
    return CompletableFuture.completedFuture(logDeleteOperation(object));
  }

  @Override
  public void close() throws IOException {

  }

  public ConcurrentLinkedQueue<ObjectMetadata> getCreatedObjects() {
    return createdObjects;
  }

  public ConcurrentLinkedQueue<AbstractMap.SimpleImmutableEntry<ObjectMetadata, ObjectMetadata>> getRenamedObjects() {
    return renamedObjects;
  }

  public ConcurrentLinkedQueue<ObjectMetadata> getDeletedObjects() {
    return deletedObjects;
  }
}
