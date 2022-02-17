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

package com.adobe.s3fs.metastore.internal.dynamodb.storage;

import com.adobe.s3fs.metastore.api.ObjectMetadata;
import com.adobe.s3fs.metastore.internal.dynamodb.versioning.VersionedObject;

import org.apache.hadoop.fs.Path;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * Defines the contract for basic CRUD operations of {@link ObjectMetadata} instances.
 */
public interface ObjectMetadataStorage {

  /**
   * Creates a transaction.
   * @return
   */
  Transaction createTransaction();

  Optional<VersionedObject> getObject(Path key);

  CompletableFuture<Optional<VersionedObject>> getObjectAsync(Path key);

  boolean storeSingleObject(VersionedObject object);

  boolean updateSingleObject(VersionedObject object);

  CompletableFuture<Boolean> updateSingleObjectAsync(VersionedObject object);

  CompletableFuture<Boolean> storeSingleObjectAsync(VersionedObject object);

  boolean deleteSingleObject(VersionedObject object);

  CompletableFuture<Boolean> deleteSingleObjectAsync(VersionedObject object);

  Iterable<VersionedObject> list(VersionedObject object);

  CompletableFuture<Iterable<VersionedObject>> listAsync(VersionedObject object);

  Iterable<VersionedObject> scan(Path key, int partitionIndex, int partitionCount);

  /**
   * Defines an interface that can be used to store and remove multiple {@link ObjectMetadata} instances as an atomic operation.
   */
  interface Transaction {

    /**
     * Adds an object to be stored
     * @param object
     */
    void addItemToStore(VersionedObject object);

    /**
     * Adds an object to be stored.
     * @param object
     */
    void addItemToDelete(VersionedObject object);

    /**
     * Commits the transaction
     * @return True if the commit is successful, false otherwise.
     */
    boolean commit();

    /**
     * Commits the transaction asynchronously.
     * @return
     */
    CompletableFuture<Boolean> commitAsync();
  }
}
