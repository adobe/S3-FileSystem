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

import com.adobe.s3fs.common.runtime.FileSystemRuntime;
import com.adobe.s3fs.utils.collections.EagerIterable;
import com.adobe.s3fs.utils.exceptions.UncheckedException;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.document.ItemUtils;
import com.amazonaws.services.dynamodbv2.model.*;
import com.google.common.base.Preconditions;
import com.google.common.collect.FluentIterable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CompletableFuture;

public class AmazonDynamoDBStorage implements DynamoDBStorage {

  public static final String HASH_KEY = "path";
  public static final String SORT_KEY = "children";
  public static final String IS_DIR = "isdir";
  public static final String SIZE= "size";
  public static final String CREATION_TIME = "ctime";
  public static final String PHYSICAL_PATH = "physpath";
  public static final String PHYSICAL_DATA_COMMITTED = "physcommitted";
  public static final String VERSION = "ver";
  public static final String ID = "id";

  public static final String HASH_KEY_ATT_NAME = "#p";
  public static final Map<String, String> CREATE_ITEM_IF_NOT_EXISTS_ATT_NAMES = new HashMap<String, String>() {{
    put(HASH_KEY_ATT_NAME, HASH_KEY);
  }};

  private final AmazonDynamoDB dynamoDB;
  private final String tableName;
  private final FileSystemRuntime runtime;

  private static final Logger LOG = LoggerFactory.getLogger(AmazonDynamoDBStorage.class);

  public AmazonDynamoDBStorage(AmazonDynamoDB dynamoDB,
                               String tableName,
                               FileSystemRuntime runtime) {
    this.dynamoDB = Preconditions.checkNotNull(dynamoDB);
    this.tableName = Preconditions.checkNotNull(tableName);
    this.runtime = Preconditions.checkNotNull(runtime);
  }

  @Override
  public void putItem(DynamoDBItem item) {
    Map<String, AttributeValue> dynamoItem = toRawDynamoDBItem(item);
    dynamoDB.putItem(new PutItemRequest(tableName, dynamoItem));
  }

  @Override
  public CompletableFuture<Void> putItemAsync(DynamoDBItem item) {
    return runtime.async(() -> {
      putItem(item);
      return null;
    });
  }

  @Override
  public CompletableFuture<Void> updateItemAsync(DynamoDBItem item) {
    return runtime.async(() -> {
      updateItem(item);
      return null;
    });
  }

  @Override
  public void updateItem(DynamoDBItem item) {
    UpdateItemRequest request = new UpdateItemRequest()
        .withTableName(tableName)
        .withKey(toRawDynamoDBKey(item))
        .addExpectedEntry(HASH_KEY,
            new ExpectedAttributeValue()
                .withValue(ItemUtils.toAttributeValue(item.getHashKey()))
                .withComparisonOperator(ComparisonOperator.EQ))
        .addExpectedEntry(SORT_KEY,
            new ExpectedAttributeValue()
                .withValue(ItemUtils.toAttributeValue(item.getSortKey()))
                .withComparisonOperator(ComparisonOperator.EQ))
        .addExpectedEntry(ID,
            new ExpectedAttributeValue()
                .withValue(ItemUtils.toAttributeValue(item.id().toString()))
                .withComparisonOperator(ComparisonOperator.EQ))
        .addExpectedEntry(VERSION,
            new ExpectedAttributeValue()
                .withValue(ItemUtils.toAttributeValue(item.version() - 1))
                .withComparisonOperator(ComparisonOperator.EQ))
        .withAttributeUpdates(toRawDynamoDBItemUpdate(item));

    try {
      dynamoDB.updateItem(request);
    } catch (ConditionalCheckFailedException e) {
      try {
        LOG.error("Received conditional check failed error. Will check for idempotency false positive");
        LOG.error("Current storage state {}. Item new state to be updated {}", getItem(item.getHashKey(), item.getSortKey()), item);
      } catch (Throwable t) {
        LOG.error("Error checking for update false positive", t);
      }
      throw new UncheckedException(e);
    }
  }

  @Override
  public Optional<DynamoDBItem> getItem(String hashKey, String sortKey) {
    GetItemRequest getItemRequest = new GetItemRequest(tableName, toRawDynamoDBKey(hashKey, sortKey))
        .withConsistentRead(true)
        .withAttributesToGet(HASH_KEY, SORT_KEY, IS_DIR, CREATION_TIME, SIZE,
                             PHYSICAL_PATH, PHYSICAL_DATA_COMMITTED, VERSION, ID);

    GetItemResult getItemResult = dynamoDB.getItem(getItemRequest);
    if (getItemResult == null || getItemResult.getItem() == null) {
      return Optional.empty();
    }

    return Optional.of(rawItemToDynamoDBItem(getItemResult.getItem()));
  }

  @Override
  public CompletableFuture<Optional<DynamoDBItem>> getItemAsync(String hashKey, String sortKey) {
    return runtime.async(() -> getItem(hashKey, sortKey));
  }

  @Override
  public void deleteItem(String hashKey, String sortKey) {
    dynamoDB.deleteItem(new DeleteItemRequest(tableName, toRawDynamoDBKey(hashKey, sortKey)));
  }

  @Override
  public CompletableFuture<Void> deleteItemAsync(String hashKey, String sortKey) {
    return runtime.async(() -> {
      deleteItem(hashKey, sortKey);
      return null;
    });
  }

  @Override
  public Iterable<DynamoDBItem> list(String hashKey) {
    Condition hashCondition = new Condition()
        .withAttributeValueList(ItemUtils.toAttributeValue(hashKey))
        .withComparisonOperator(ComparisonOperator.EQ);
    Map<String, Condition> hashConditionMap = new HashMap<>();
    hashConditionMap.put(HASH_KEY, hashCondition);

    QueryRequest queryRequest = new QueryRequest(tableName)
        .withKeyConditions(hashConditionMap)
        .withConsistentRead(true)
        .withAttributesToGet(HASH_KEY, SORT_KEY, IS_DIR, CREATION_TIME, SIZE, PHYSICAL_PATH, PHYSICAL_DATA_COMMITTED,
                             VERSION, ID);

    return FluentIterable.from(new EagerIterable<>(() -> new QueryIterator(queryRequest)))
        .transform(this::rawItemToDynamoDBItem);
  }

  @Override
  public CompletableFuture<Iterable<DynamoDBItem>> listAsync(String hashKey) {
    return runtime.async(() -> list(hashKey));
  }

  @Override
  public Iterable<DynamoDBItem> scan(int partitionIndex, int partitionCount) {
    Preconditions.checkArgument(partitionIndex >= 0);
    Preconditions.checkArgument(partitionCount > 0);
    Preconditions.checkArgument(partitionIndex < partitionCount);

    ScanRequest scanRequest = new ScanRequest(tableName)
        .withConsistentRead(true)
        .withSegment(partitionIndex)
        .withTotalSegments(partitionCount)
        .withAttributesToGet(HASH_KEY, SORT_KEY, IS_DIR, CREATION_TIME, SIZE, PHYSICAL_PATH, PHYSICAL_DATA_COMMITTED,
                             VERSION, ID);

    return FluentIterable.from(new EagerIterable<>(() -> new ScanIterator(scanRequest)))
        .transform(this::rawItemToDynamoDBItem);

  }

  private static Map<String, AttributeValue> toRawDynamoDBKey(DynamoDBItem item) {
    return toRawDynamoDBKey(item.getHashKey(), item.getSortKey());
  }

  private static Map<String, AttributeValue> toRawDynamoDBKey(String hashKey, String sortKey) {
    Map<String, AttributeValue> key = new HashMap<>();

    key.put(HASH_KEY, ItemUtils.toAttributeValue(hashKey));
    key.put(SORT_KEY, ItemUtils.toAttributeValue(sortKey));

    return key;
  }

  private static Map<String, AttributeValue> toRawDynamoDBItem(DynamoDBItem item) {
    Map<String ,AttributeValue> dynamoItem = new HashMap<>();

    dynamoItem.put(HASH_KEY, ItemUtils.toAttributeValue(item.getHashKey()));
    dynamoItem.put(SORT_KEY, ItemUtils.toAttributeValue(item.getSortKey()));
    dynamoItem.put(IS_DIR, ItemUtils.toAttributeValue(item.isDirectory()));
    dynamoItem.put(CREATION_TIME, ItemUtils.toAttributeValue(item.getCreationTime()));
    dynamoItem.put(VERSION, ItemUtils.toAttributeValue(item.version()));
    dynamoItem.put(ID, ItemUtils.toAttributeValue(item.id().toString()));

    if (!item.isDirectory()) {
      dynamoItem.put(SIZE, ItemUtils.toAttributeValue(item.getSize()));
      dynamoItem.put(PHYSICAL_DATA_COMMITTED, ItemUtils.toAttributeValue(item.physicalDataCommitted()));
      dynamoItem.put(PHYSICAL_PATH, ItemUtils.toAttributeValue(item.getPhysicalPath().get()));
    }

    return dynamoItem;
  }

  private static Map<String, AttributeValueUpdate> toRawDynamoDBItemUpdate(DynamoDBItem item) {
    Map<String ,AttributeValueUpdate> dynamoItem = new HashMap<>();

    dynamoItem.put(CREATION_TIME,
                   new AttributeValueUpdate(ItemUtils.toAttributeValue(item.getCreationTime()), AttributeAction.PUT));
    dynamoItem.put(VERSION,
                   new AttributeValueUpdate(ItemUtils.toAttributeValue(item.version()), AttributeAction.PUT));
    dynamoItem.put(ID,
                   new AttributeValueUpdate(ItemUtils.toAttributeValue(item.id().toString()), AttributeAction.PUT));

    if (!item.isDirectory()) {
      dynamoItem.put(SIZE,
                     new AttributeValueUpdate(ItemUtils.toAttributeValue(item.getSize()), AttributeAction.PUT));
      dynamoItem.put(PHYSICAL_DATA_COMMITTED,
                     new AttributeValueUpdate(ItemUtils.toAttributeValue(item.physicalDataCommitted()), AttributeAction.PUT));
      dynamoItem.put(PHYSICAL_PATH,
                     new AttributeValueUpdate(ItemUtils.toAttributeValue(item.getPhysicalPath().get()), AttributeAction.PUT));
    }

    return dynamoItem;
  }

  @Override
  public Transaction createTransaction() {
    return new TransactionImpl();
  }

  private DynamoDBItem rawItemToDynamoDBItem(Map<String, AttributeValue> rawItem) {
    boolean isDir = rawItem.get(IS_DIR).getBOOL();

    DynamoDBItem.Builder itemBuilder = DynamoDBItem.builder()
        .hashKey(rawItem.get(HASH_KEY).getS())
        .sortKey(rawItem.get(SORT_KEY).getS())
        .isDirectory(isDir)
        .creationTime(Long.valueOf(rawItem.get(CREATION_TIME).getN()))
        .id(UUID.fromString(rawItem.get(ID).getS()))
        .version(Integer.valueOf(rawItem.get(VERSION).getN()));

    if (isDir) {
      itemBuilder.size(0)
          .physicalDataCommitted(false)
          .physicalPath(Optional.empty());
    } else {
      if (rawItem.get(PHYSICAL_DATA_COMMITTED) == null) {
        // being backwards compatible here, this attribute may not be present
        itemBuilder.size(Long.valueOf(rawItem.get(SIZE).getN()))
                .physicalDataCommitted(true)
                .physicalPath(rawItem.get(PHYSICAL_PATH).getS());
      } else {
        itemBuilder.size(Long.valueOf(rawItem.get(SIZE).getN()))
                .physicalDataCommitted(rawItem.get(PHYSICAL_DATA_COMMITTED).getBOOL())
                .physicalPath(rawItem.get(PHYSICAL_PATH).getS());
      }
    }

    return itemBuilder.build();
  }

  private static class Page {
    private final Map<String, AttributeValue> lastKey;
    private final Iterator<Map<String, AttributeValue>> iterator;

    public Page(Map<String, AttributeValue> lastKey, Iterator<Map<String, AttributeValue>> iterator) {
      this.lastKey = lastKey;
      this.iterator = iterator;
    }

    public Map<String, AttributeValue> getLastKey() {
      return lastKey;
    }

    public Iterator<Map<String, AttributeValue>> getIterator() {
      return iterator;
    }

    public static final Page EMPTY = new Page(null, Collections.<Map<String, AttributeValue>>emptyList().iterator());
  }

  private class ScanIterator extends RawDynamoDBItemIterator<ScanRequest> {

    public ScanIterator(ScanRequest request) {
      super(request, scanPage(dynamoDB, request));
    }

    @Override
    protected Page nextPage(Page currentPage, ScanRequest scanRequest) {
      return scanPage(dynamoDB, scanRequest.withExclusiveStartKey(currentPage.getLastKey()));
    }
  }

  private class QueryIterator extends RawDynamoDBItemIterator<QueryRequest> {

    public QueryIterator(QueryRequest request) {
      super(request, queryPage(dynamoDB, request));
    }

    @Override
    protected Page nextPage(Page currentPage, QueryRequest request) {
      return queryPage(dynamoDB, request.withExclusiveStartKey(currentPage.getLastKey()));
    }
  }

  private static Page queryPage(AmazonDynamoDB dynamoDB, QueryRequest request) {
    QueryResult result = dynamoDB.query(request);
    if (result == null || result.getItems() == null) {
      return Page.EMPTY;
    }
    return new Page(result.getLastEvaluatedKey(), result.getItems().iterator());
  }

  private static Page scanPage(AmazonDynamoDB dynamoDB, ScanRequest request) {
    ScanResult result = dynamoDB.scan(request);
    if (result == null || result.getItems() == null) {
      return Page.EMPTY;
    }
    return new Page(result.getLastEvaluatedKey(), result.getItems().iterator());
  }

  private abstract class RawDynamoDBItemIterator<TRequest> implements Iterator<Map<String, AttributeValue>> {

    private final TRequest request;
    private Page currentPage = null;

    public RawDynamoDBItemIterator(TRequest request, Page currentPage) {
      this.request = request;
      this.currentPage = currentPage;
    }

    @Override
    public boolean hasNext() {
      if (!currentPage.getIterator().hasNext()) {
        if (currentPage.getLastKey() != null) {
          currentPage = nextPage(currentPage, request);
        }
      }

      return currentPage.getIterator().hasNext();
    }

    @Override
    public Map<String, AttributeValue> next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      return currentPage.getIterator().next();
    }

    protected abstract Page nextPage(Page currentPage, TRequest request);

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }

  private class TransactionImpl implements Transaction {

    private final TransactWriteItemsRequest transactWriteItemsRequest = new TransactWriteItemsRequest()
        .withClientRequestToken(UUID.randomUUID().toString());

    @Override
    public void addItemToPut(DynamoDBItem item, boolean enforceItemNotPresent) {
      Put put = new Put().withTableName(tableName).withItem(toRawDynamoDBItem(item));
      if (enforceItemNotPresent) {
        put.withExpressionAttributeNames(CREATE_ITEM_IF_NOT_EXISTS_ATT_NAMES);
        put.withConditionExpression(String.format("attribute_not_exists(#p) and attribute_not_exists(%s)", SORT_KEY));
      }
      transactWriteItemsRequest.withTransactItems(new TransactWriteItem().withPut(put));
    }

    @Override
    public void addItemToDelete(DynamoDBItem item) {
      transactWriteItemsRequest.withTransactItems(
          new TransactWriteItem().withDelete(new Delete().withTableName(tableName).withKey(toRawDynamoDBKey(item))));
    }

    @Override
    public CompletableFuture<Boolean> commitAsync() {
      return runtime.async(this::commit);
    }

    @Override
    public boolean commit() {
      try {
        dynamoDB.transactWriteItems(transactWriteItemsRequest);
      } catch (TransactionConflictException | TransactionCanceledException | ConditionalCheckFailedException e) {
        LOG.error("Transaction conflict occurred on request: {}", transactWriteItemsRequest);
        LOG.error("The conflict is caused by:", e);
        return false;
      } catch (Exception re) {
        LOG.error("Transaction failed with error", re);
        return false;
      }
      return true;
    }
  }
}
