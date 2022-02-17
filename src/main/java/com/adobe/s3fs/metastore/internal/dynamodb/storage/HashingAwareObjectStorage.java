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
import com.adobe.s3fs.metastore.internal.dynamodb.hashing.KeyOperations;
import com.adobe.s3fs.metastore.internal.dynamodb.utils.DynamoUtils;
import com.adobe.s3fs.metastore.internal.dynamodb.versioning.VersionedObject;
import com.adobe.s3fs.utils.collections.RoundRobinIterable;

import com.google.common.base.Preconditions;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Lists;

import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;

public class HashingAwareObjectStorage implements ObjectMetadataStorage {

  private final KeyOperations keyOperations;
  private final DynamoDBStorage storage;

  private static final Logger LOG = LoggerFactory.getLogger(HashingAwareObjectStorage.class);

  public HashingAwareObjectStorage(KeyOperations keyOperations, DynamoDBStorage storage) {
    this.keyOperations = Preconditions.checkNotNull(keyOperations);
    this.storage = Preconditions.checkNotNull(storage);
  }

  @Override
  public Transaction createTransaction() {
    return new TransactionImpl(storage.createTransaction());
  }

  @Override
  public Optional<VersionedObject> getObject(Path key) {
    String hashKey = keyOperations.logicalKeyToHashKey(key);
    String sortKey = keyOperations.logicalKeyToSortKey(key);
    return storage.getItem(hashKey, sortKey)
        .map(this::dynamoItemToObject);
  }

  @Override
  public CompletableFuture<Optional<VersionedObject>> getObjectAsync(Path key) {
    String hashKey = keyOperations.logicalKeyToHashKey(key);
    String sortKey = keyOperations.logicalKeyToSortKey(key);
    return storage.getItemAsync(hashKey, sortKey)
        .thenApply(it -> it.map(this::dynamoItemToObject));
  }

  @Override
  public boolean storeSingleObject(VersionedObject object) {
    try {
      DynamoDBItem dynamoDBItem = objectMetadataToDynamoItem(object);
      storage.putItem(dynamoDBItem);

      return true;
    } catch (Exception e) {
      LOG.error("Error creating " + object.metadata().getKey(), e);
      return false;
    }
  }

  @Override
  public CompletableFuture<Boolean> storeSingleObjectAsync(VersionedObject object) {
    DynamoDBItem dynamoDBItem = objectMetadataToDynamoItem(object);

    return storage.putItemAsync(dynamoDBItem)
        .thenApply(it -> Boolean.TRUE)
        .exceptionally(e -> {
          LOG.error("Error creating " + object.metadata().getKey(), e);
          return Boolean.FALSE;
        });
  }

  @Override
  public boolean updateSingleObject(VersionedObject object) {
    DynamoDBItem item = objectMetadataToDynamoItem(object);

    try {
      storage.updateItem(item);
      return true;
    } catch (Exception e) {
      LOG.error("Error updating " + object, e);
      return false;
    }
  }

  @Override
  public CompletableFuture<Boolean> updateSingleObjectAsync(VersionedObject object) {
    DynamoDBItem dynamoDBItem = objectMetadataToDynamoItem(object);

    return storage.updateItemAsync(dynamoDBItem)
        .thenApply(it -> Boolean.TRUE)
        .exceptionally(e -> {
          LOG.error("Error updating " + object, e);
          return Boolean.FALSE;
        });
  }

  @Override
  public boolean deleteSingleObject(VersionedObject object) {
    try {
      storage.deleteItem(keyOperations.logicalKeyToHashKey(object.metadata().getKey()),
                         keyOperations.logicalKeyToSortKey(object.metadata().getKey()));
      return true;
    } catch (Exception e) {
      LOG.error("Error deleting " + object.metadata().getKey(), e);
      return false;
    }
  }

  @Override
  public CompletableFuture<Boolean> deleteSingleObjectAsync(VersionedObject object) {
    String hashKey = keyOperations.logicalKeyToHashKey(object.metadata().getKey());
    String sortKey = keyOperations.logicalKeyToSortKey(object.metadata().getKey());

    return storage.deleteItemAsync(hashKey, sortKey)
        .thenApply(it -> Boolean.TRUE)
        .exceptionally(e -> {
          LOG.error("Error deleting " + object.metadata().getKey(), e);
          return Boolean.FALSE;
        });
  }

  @Override
  public Iterable<VersionedObject> list(VersionedObject object) {
    return FluentIterable.from(keyOperations.logicalKeyToAllPossibleHashKeys(object.metadata().getKey()))
        .transformAndConcat(it -> storage.list(it))
        .transform(this::dynamoItemToObject);
  }

  @Override
  public CompletableFuture<Iterable<VersionedObject>> listAsync(VersionedObject object) {
    List<String> allHashKeys = Lists.newArrayList(keyOperations.logicalKeyToAllPossibleHashKeys(object.metadata().getKey()));

    List<CompletableFuture<?>> partitionListings = new ArrayList<>(allHashKeys.size());

    ConcurrentLinkedQueue<Iterable<VersionedObject>> sync = new ConcurrentLinkedQueue<>();

    for (String hashKey : allHashKeys) {
      partitionListings.add(listSinglePartitionAsync(hashKey)
                                .thenApply(sync::add));
    }

    return CompletableFuture.allOf((CompletableFuture[]) partitionListings.toArray(new CompletableFuture[0]))
        .thenApply(it -> new RoundRobinIterable<>(sync));
  }

  private CompletableFuture<Iterable<VersionedObject>> listSinglePartitionAsync(String hashKey) {
    return storage.listAsync(hashKey)
        .thenApply(it -> FluentIterable.from(it).transform(this::dynamoItemToObject));
  }

  @Override
  public Iterable<VersionedObject> scan(Path key, int partitionIndex, int partitionCount) {
    Preconditions.checkArgument(partitionIndex >= 0);
    Preconditions.checkArgument(partitionCount > 0);
    Preconditions.checkArgument(partitionIndex < partitionCount);

    String keyString = key.toString();
    return  FluentIterable.from(storage.scan(partitionIndex, partitionCount))
        .transform(this::dynamoItemToObject)
        .filter(it -> {
          String scannedKeyString = it.metadata().getKey().toString();
          return scannedKeyString.startsWith(keyString)
              && scannedKeyString.length() > keyString.length();
        });
  }

  private VersionedObject dynamoItemToObject(DynamoDBItem item) {
    ObjectMetadata metadata = ObjectMetadata.builder()
        .key(keyOperations.hashAndSortKeyToLogicalKey(item.getHashKey(), item.getSortKey())
                 .orElseThrow(() -> new RuntimeException("Invalid hash and sort " + item.getHashKey() + " " + item.getSortKey())))
        .isDirectory(item.isDirectory())
        .creationTime(item.getCreationTime())
        .size(item.getSize())
        .physicalPath(item.getPhysicalPath())
        .physicalDataCommitted(item.physicalDataCommitted())
        .build();
    return VersionedObject.builder()
        .metadata(metadata)
        .id(item.id())
        .version(item.version())
        .build();
  }

  private DynamoDBItem objectMetadataToDynamoItem(VersionedObject object) {
    Path key = object.metadata().getKey();
    String hashKey = keyOperations.logicalKeyToHashKey(key);
    String sortKey = keyOperations.logicalKeyToSortKey(key);

    return DynamoUtils.objectToDynamoItem(object, hashKey, sortKey);
  }

  private class TransactionImpl implements Transaction {

    private final DynamoDBStorage.Transaction dynamoTransaction;

    public TransactionImpl(DynamoDBStorage.Transaction dynamoTransaction) {
      this.dynamoTransaction = dynamoTransaction;
    }

    @Override
    public void addItemToDelete(VersionedObject object) {
      LOG.info("Adding object to delete in transaction: {}", object.metadata().getKey());

      dynamoTransaction.addItemToDelete(objectMetadataToDynamoItem(object));
    }

    @Override
    public void addItemToStore(VersionedObject object) {
      LOG.info("Adding object to store in transaction: {}", object.metadata().getKey());

      dynamoTransaction.addItemToPut(objectMetadataToDynamoItem(object), true);
    }

    @Override
    public boolean commit() {
      return dynamoTransaction.commit();
    }

    @Override
    public CompletableFuture<Boolean> commitAsync() {
      return dynamoTransaction.commitAsync();
    }
  }
}
