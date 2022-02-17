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

package com.adobe.s3fs.metastore.internal.dynamodb.operations;

import com.adobe.s3fs.metastore.api.MetadataOperationLog;
import com.adobe.s3fs.metastore.api.ObjectLevelMetrics;
import com.adobe.s3fs.metastore.api.ObjectMetadata;
import com.adobe.s3fs.metastore.internal.dynamodb.storage.ObjectMetadataStorage;
import com.adobe.s3fs.metastore.internal.dynamodb.versioning.VersionedObject;
import com.google.common.base.Preconditions;
import org.apache.hadoop.fs.Path;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public class MetadataOperations implements Closeable {

  private final ObjectMetadataStorage objectStorage;
  private final MetadataOperationLog operationLog;
  private final ObjectLevelMetrics objectLevelMetrics;

  public MetadataOperations(
      ObjectMetadataStorage objectMetadataStorage,
      MetadataOperationLog operationLog,
      ObjectLevelMetrics objectLevelMetrics) {
    this.objectStorage = Preconditions.checkNotNull(objectMetadataStorage);
    this.operationLog = Preconditions.checkNotNull(operationLog);
    this.objectLevelMetrics = Preconditions.checkNotNull(objectLevelMetrics);
  }

  public Optional<VersionedObject> getObject(Path key) {
    return objectStorage.getObject(key);
  }

  public CompletableFuture<Optional<VersionedObject>> getObjectAsync(Path key) {
    return objectStorage.getObjectAsync(key);
  }

  public boolean store(VersionedObject object) {
    if (object.metadata().isDirectory()) {
      boolean returnStatus = objectStorage.storeSingleObject(object);
      if (!returnStatus) {
        objectLevelMetrics.incrFailedDynamoDB();
      }
      return returnStatus;
    }
    Optional<MetadataOperationLog.LogEntryHandle> handle = operationLog.logCreateOperation(object);
    if (!handle.isPresent()) {
      objectLevelMetrics.incrFailedPendingOpLog();
      return false;
    }
    if (!objectStorage.storeSingleObject(object)) {
      objectLevelMetrics.incrFailedDynamoDB();
      rollbackHandle(handle.get());
      return false;
    }
    commitHandle(handle.get());
    return true;
  }

  public boolean restoreVersionedObject(VersionedObject objectToRestore) {
    if (!objectStorage.storeSingleObject(objectToRestore)) {
      objectLevelMetrics.incrFailedDynamoDB();
      return false;
    }
    return true;
  }

  public CompletableFuture<Boolean> storeAsync(VersionedObject object) {
    if (object.metadata().isDirectory()) {
      return objectStorage.storeSingleObjectAsync(object).thenApply(
          returnStatus -> {
            if (!returnStatus) {
              objectLevelMetrics.incrFailedDynamoDB();
            }
            return returnStatus;
          }
      );
    }

    return operationLog.logCreateOperationAsync(object)
        .thenCompose(handle -> {
          if (!handle.isPresent()) {
            objectLevelMetrics.incrFailedPendingOpLog();
            return CompletableFuture.completedFuture(Boolean.FALSE);
          }
          return objectStorage.storeSingleObjectAsync(object)
                  .thenApply(stored -> {
                    if (stored) {
                      commitHandle(handle.get());
                      return Boolean.TRUE;
                    }
                    objectLevelMetrics.incrFailedDynamoDB();
                    rollbackHandle(handle.get());
                    return Boolean.FALSE;
                  });
        });
  }

  public VersionedObject update(VersionedObject object, ObjectMetadata newMetadata) {
    VersionedObject newObject = object.updated(newMetadata);

    if (object.metadata().isDirectory()) {
      if (!objectStorage.updateSingleObject(newObject)) {
        objectLevelMetrics.incrFailedDynamoDB();
        throw new RuntimeException("Failed to update object " + object);
      }
      return newObject;
    }

    Optional<MetadataOperationLog.LogEntryHandle> handle = operationLog.logUpdateOperation(object, newObject);
    if (!handle.isPresent()) {
      objectLevelMetrics.incrFailedPendingOpLog();
      throw new RuntimeException("Failed to update object " + object);
    }

    if (!objectStorage.updateSingleObject(newObject)) {
      objectLevelMetrics.incrFailedDynamoDB();
      rollbackHandle(handle.get());
      throw new RuntimeException("Failed to update object " + object);
    }
    commitHandle(handle.get());
    return newObject;
  }

  public boolean delete(VersionedObject object) {
    if (object.metadata().isDirectory()) {
      boolean returnStatus = objectStorage.deleteSingleObject(object);
      if (!returnStatus) {
        objectLevelMetrics.incrFailedDynamoDB();
      }
      return returnStatus;
    }
    Optional<MetadataOperationLog.LogEntryHandle> handle = operationLog.logDeleteOperation(object);
    if (!handle.isPresent()) {
      objectLevelMetrics.incrFailedPendingOpLog();
      return false;
    }
    if (!objectStorage.deleteSingleObject(object)) {
      objectLevelMetrics.incrFailedDynamoDB();
      rollbackHandle(handle.get());
      return false;
    }
    commitHandle(handle.get());
    return true;
  }

  public CompletableFuture<Boolean> deleteAsync(VersionedObject object) {
    if (object.metadata().isDirectory()) {
      return objectStorage
          .deleteSingleObjectAsync(object)
          .thenApply(
              returnStatus -> {
                if (!returnStatus) {
                  objectLevelMetrics.incrFailedDynamoDB();
                }
                return returnStatus;
              });
    }

    return operationLog.logDeleteOperationAsync(object)
            .thenCompose(handle -> {
              if (!handle.isPresent()) {
                objectLevelMetrics.incrFailedPendingOpLog();
                return CompletableFuture.completedFuture(Boolean.FALSE);
              }
              return objectStorage.deleteSingleObjectAsync(object)
                      .thenApply(deleted -> {
                        if (deleted) {
                          commitHandle(handle.get());
                          return Boolean.TRUE;
                        }
                        objectLevelMetrics.incrFailedDynamoDB();
                        rollbackHandle(handle.get());
                        return Boolean.FALSE;
                      });
            });
  }

  public boolean renameFile(VersionedObject source, Path destination) {
    if (source.metadata().isDirectory()) {
      throw new IllegalArgumentException();
    }

    VersionedObject destinationObject = source.moveTo(destination);

    Optional<MetadataOperationLog.LogEntryHandle> handle = operationLog.logUpdateOperation(source, destinationObject);
    if (!handle.isPresent()) {
      objectLevelMetrics.incrFailedPendingOpLog();
      return false;
    }

    ObjectMetadataStorage.Transaction transaction = objectStorage.createTransaction();
    transaction.addItemToDelete(source);
    transaction.addItemToStore(destinationObject);

    if (!transaction.commit()) {
      objectLevelMetrics.incrFailedDynamoDB();
      rollbackHandle(handle.get());
      return false;
    }

    commitHandle(handle.get());
    return true;
  }

  public CompletableFuture<Boolean> renameFileAsync(VersionedObject source, Path destination) {
    if (source.metadata().isDirectory()) {
      throw new IllegalArgumentException();
    }

    VersionedObject destinationObject = source.moveTo(destination);

    return operationLog.logUpdateOperationAsync(source, destinationObject)
        .thenCompose(handle -> {
          if (!handle.isPresent()) {
            objectLevelMetrics.incrFailedPendingOpLog();
            return CompletableFuture.completedFuture(Boolean.FALSE);
          }
          ObjectMetadataStorage.Transaction transaction = objectStorage.createTransaction();
          transaction.addItemToDelete(source);
          transaction.addItemToStore(destinationObject);
          return transaction.commitAsync().thenApply(successful -> {
            if (!successful) {
              rollbackHandle(handle.get());
              objectLevelMetrics.incrFailedDynamoDB();
              return Boolean.FALSE;
            }
            commitHandle(handle.get());
            return Boolean.TRUE;
          });
        });
  }

  public Iterable<VersionedObject> list(VersionedObject object) {
    return objectStorage.list(object);
  }

  public CompletableFuture<Iterable<VersionedObject>> listAsync(VersionedObject object) {
    return objectStorage.listAsync(object);
  }

  public Iterable<VersionedObject> scan(Path key, int partitionIndex, int partitionCount) {
    return objectStorage.scan(key, partitionIndex, partitionCount);
  }

  private void rollbackHandle(MetadataOperationLog.LogEntryHandle handle) {
    if (handle.rollback()) {
      objectLevelMetrics.incrOpLogSuccessfulRollback();
    } else {
      objectLevelMetrics.incrOpLogFailedRollback();
    }
  }

  private void commitHandle(MetadataOperationLog.LogEntryHandle handle) {
    if (!handle.commit()) {
      objectLevelMetrics.incrFailedCommitOpLog();
    }
  }

  @Override
  public void close() throws IOException {
    operationLog.close();
  }
}
