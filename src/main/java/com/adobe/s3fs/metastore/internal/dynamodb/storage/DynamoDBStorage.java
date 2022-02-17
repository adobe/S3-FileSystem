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

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * Defines the contract for basic CRUD operations over a DynamoDB table.
 */
public interface DynamoDBStorage {

  /**
   * Defines a transaction against the DynamoDB storage. You can add multiple items to be deleted or created as an atomic operation.
   */
  interface Transaction {

    /**
     * Adds the given item to be stored in DynamoDB.
     * @param item
     * @param enforceItemNotPresent Enforces that the given item must not already exist in DynamoDB.
     */
    void addItemToPut(DynamoDBItem item, boolean enforceItemNotPresent);

    /**
     * Adds the given item to be removed from DynamoDB.
     * @param item
     */
    void addItemToDelete(DynamoDBItem item);

    /**
     * Commits the transaction as an atomic operation.
     * if the transaction cannot be committed due to conflicts with other transactions false is returned.
     * If the commit fails due to I/O or rate limiting errors false is returned.
     */
    boolean commit();

    /**
     * Commits the transaction asynchronously.
     * See {{@link Transaction#commit()}}
     * @return
     */
    CompletableFuture<Boolean> commitAsync();
  }

  /**
   * Creates a new transaction.
   * @return
   */
  Transaction createTransaction();

  /**
   * Stores the given item. If the item already exists it will be overwritten.
   * @param item
   */
  void putItem(DynamoDBItem item);

  CompletableFuture<Void> putItemAsync(DynamoDBItem item);

  /**
   * Updates an existing item. If an item already exists at the same hash and sort key, but the id is different
   * an error will be thrown.
   * @param item
   */
  void updateItem(DynamoDBItem item);

  /**
   * Asynchronous version of {@link DynamoDBStorage#updateItem(DynamoDBItem)}.
   * @param item
   * @return
   */
  CompletableFuture<Void> updateItemAsync(DynamoDBItem item);

  /**
   * @param hashKey
   * @param sortKey
   * @return Return the DynamoDB item associated to the hash and sort key. If there si no such item {@link Optional#empty()} is returned.
   */
  Optional<DynamoDBItem> getItem(String hashKey, String sortKey);

  CompletableFuture<Optional<DynamoDBItem>> getItemAsync(String hashKey, String sortKey);

  /**
   * Delete the item associated with the given hashKey and sortKey.
   * @param hashKey
   * @param sortKey
   */
  void deleteItem(String hashKey, String sortKey);

  CompletableFuture<Void> deleteItemAsync(String hashKey, String sortKey);

  /**
   * @param hashKey
   * @return All DynamoDB items associated with the sort key.
   */
  Iterable<DynamoDBItem> list(String hashKey);

  CompletableFuture<Iterable<DynamoDBItem>> listAsync(String hashKey);

  /**
   *
   * @return All DynamoDB items from the table that belong to the given partition index.
   * For example in partitionIndex is 1 and totalPartitions is 4 the method will return a quarter of all DynamoDB items in the table.
   */
  Iterable<DynamoDBItem> scan(int partitionIndex, int totalPartitions);
}
