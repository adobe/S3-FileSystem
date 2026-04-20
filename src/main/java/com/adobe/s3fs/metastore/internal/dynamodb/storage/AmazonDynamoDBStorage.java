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
import com.google.common.base.Preconditions;
import com.google.common.collect.FluentIterable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.util.*;
import java.util.concurrent.CompletableFuture;

public class AmazonDynamoDBStorage implements DynamoDBStorage {

  public static final String HASH_KEY = "path";
  public static final String SORT_KEY = "children";
  public static final String IS_DIR = "isdir";
  public static final String SIZE = "size";
  public static final String CREATION_TIME = "ctime";
  public static final String PHYSICAL_PATH = "physpath";
  public static final String PHYSICAL_DATA_COMMITTED = "physcommitted";
  public static final String VERSION = "ver";
  public static final String ID = "id";

  // Projection expression - use placeholders for reserved keywords
  // DynamoDB reserved keywords: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ReservedWords.html
  private static final String PROJECTION_EXPRESSION =
      "#p,#c,#isdir,#ctime,#size,#physpath,#physcommitted,#ver,#id";

  // Expression Attribute Names - maps placeholders to actual attribute names
  private static final Map<String, String> EXPRESSION_ATTRIBUTE_NAMES =
      Collections.unmodifiableMap(new HashMap<String, String>() {{
        put("#p", HASH_KEY);
        put("#c", SORT_KEY);
        put("#isdir", IS_DIR);
        put("#ctime", CREATION_TIME);
        put("#size", SIZE);             // "size" is a reserved keyword
        put("#physpath", PHYSICAL_PATH);
        put("#physcommitted", PHYSICAL_DATA_COMMITTED);
        put("#ver", VERSION);
        put("#id", ID);
      }});

  public static final String HASH_KEY_ATT_NAME = "#p";
  public static final Map<String, String> CREATE_ITEM_IF_NOT_EXISTS_ATT_NAMES = new HashMap<String, String>() {{
    put(HASH_KEY_ATT_NAME, HASH_KEY);
  }};

  private final DynamoDbClient dynamoDB;
  private final String tableName;
  private final FileSystemRuntime runtime;

  private static final Logger LOG = LoggerFactory.getLogger(AmazonDynamoDBStorage.class);

  public AmazonDynamoDBStorage(DynamoDbClient dynamoDB,
                               String tableName,
                               FileSystemRuntime runtime) {
    this.dynamoDB = Preconditions.checkNotNull(dynamoDB);
    this.tableName = Preconditions.checkNotNull(tableName);
    this.runtime = Preconditions.checkNotNull(runtime);
  }

  @Override
  public void putItem(DynamoDBItem item) {
    Map<String, AttributeValue> dynamoItem = toRawDynamoDBItem(item);
    PutItemRequest request = PutItemRequest.builder()
        .tableName(tableName)
        .item(dynamoItem)
        .build();
    dynamoDB.putItem(request);
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
    Map<String, ExpectedAttributeValue> expected = new HashMap<>();

    expected.put(HASH_KEY, ExpectedAttributeValue.builder()
        .value(ItemUtils.toAttributeValue(item.getHashKey()))
        .comparisonOperator(ComparisonOperator.EQ)
        .build());
    expected.put(SORT_KEY, ExpectedAttributeValue.builder()
        .value(ItemUtils.toAttributeValue(item.getSortKey()))
        .comparisonOperator(ComparisonOperator.EQ)
        .build());
    expected.put(ID, ExpectedAttributeValue.builder()
        .value(ItemUtils.toAttributeValue(item.id().toString()))
        .comparisonOperator(ComparisonOperator.EQ)
        .build());
    expected.put(VERSION, ExpectedAttributeValue.builder()
        .value(ItemUtils.toAttributeValue(item.version() - 1))
        .comparisonOperator(ComparisonOperator.EQ)
        .build());

    UpdateItemRequest request = UpdateItemRequest.builder()
        .tableName(tableName)
        .key(toRawDynamoDBKey(item))
        .expected(expected)
        .attributeUpdates(toRawDynamoDBItemUpdate(item))
        .build();

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
    GetItemRequest request = GetItemRequest.builder()
        .tableName(tableName)
        .key(toRawDynamoDBKey(hashKey, sortKey))
        .consistentRead(true)
        .projectionExpression(PROJECTION_EXPRESSION)
        .expressionAttributeNames(EXPRESSION_ATTRIBUTE_NAMES)
        .build();

    GetItemResponse response = dynamoDB.getItem(request);
    if (!response.hasItem() || response.item().isEmpty()) {
      return Optional.empty();
    }

    return Optional.of(rawItemToDynamoDBItem(response.item()));
  }

  @Override
  public CompletableFuture<Optional<DynamoDBItem>> getItemAsync(String hashKey, String sortKey) {
    return runtime.async(() -> getItem(hashKey, sortKey));
  }

  @Override
  public void deleteItem(String hashKey, String sortKey) {
    DeleteItemRequest request = DeleteItemRequest.builder()
        .tableName(tableName)
        .key(toRawDynamoDBKey(hashKey, sortKey))
        .build();

    dynamoDB.deleteItem(request);
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
    Condition condition = Condition.builder()
        .attributeValueList(ItemUtils.toAttributeValue(hashKey))
        .comparisonOperator(ComparisonOperator.EQ)
        .build();

    QueryRequest queryRequest = QueryRequest.builder()
        .tableName(tableName)
        .keyConditions(Collections.singletonMap(HASH_KEY, condition))
        .consistentRead(true)
        .projectionExpression(PROJECTION_EXPRESSION)
        .expressionAttributeNames(EXPRESSION_ATTRIBUTE_NAMES)
        .build();

    return FluentIterable.from(new EagerIterable<>(() -> new PaginatedDynamoIterator<>(queryRequest, this::loadQueryPage)));
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

    ScanRequest scanRequest = ScanRequest.builder()
        .tableName(tableName)
        .consistentRead(true)
        .segment(partitionIndex)
        .totalSegments(partitionCount)
        .projectionExpression(PROJECTION_EXPRESSION)
        .expressionAttributeNames(EXPRESSION_ATTRIBUTE_NAMES)
        .build();

    return () -> new PaginatedDynamoIterator<>(scanRequest, this::loadScanPage);
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
    Map<String, AttributeValue> dynamoItem = new HashMap<>();

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
    Map<String, AttributeValueUpdate> dynamoItem = new HashMap<>();

    dynamoItem.put(CREATION_TIME, AttributeValueUpdate.builder()
        .value(ItemUtils.toAttributeValue(item.getCreationTime()))
        .action(AttributeAction.PUT)
        .build());
    dynamoItem.put(VERSION, AttributeValueUpdate.builder()
        .value(ItemUtils.toAttributeValue(item.version()))
        .action(AttributeAction.PUT)
        .build());
    dynamoItem.put(ID, AttributeValueUpdate.builder()
        .value(ItemUtils.toAttributeValue(item.id().toString()))
        .action(AttributeAction.PUT)
        .build());

    if (!item.isDirectory()) {
      dynamoItem.put(SIZE, AttributeValueUpdate.builder()
          .value(ItemUtils.toAttributeValue(item.getSize()))
          .action(AttributeAction.PUT)
          .build());
      dynamoItem.put(PHYSICAL_DATA_COMMITTED, AttributeValueUpdate.builder()
          .value(ItemUtils.toAttributeValue(item.physicalDataCommitted()))
          .action(AttributeAction.PUT)
          .build());
      dynamoItem.put(PHYSICAL_PATH, AttributeValueUpdate.builder()
          .value(ItemUtils.toAttributeValue(item.getPhysicalPath().get()))
          .action(AttributeAction.PUT)
          .build());
    }

    return dynamoItem;
  }

  @Override
  public Transaction createTransaction() {
    return new TransactionImpl();
  }

  private DynamoDBItem rawItemToDynamoDBItem(Map<String, AttributeValue> rawItem) {
    boolean isDir = rawItem.get(IS_DIR).bool();


    DynamoDBItem.Builder itemBuilder = DynamoDBItem.builder()
        .hashKey(rawItem.get(HASH_KEY).s())
        .sortKey(rawItem.get(SORT_KEY).s())
        .isDirectory(isDir)
        .creationTime(Long.parseLong(rawItem.get(CREATION_TIME).n()))
        .id(UUID.fromString(rawItem.get(ID).s()))
        .version(Integer.parseInt(rawItem.get(VERSION).n()));

    if (isDir) {
      itemBuilder.size(0)
          .physicalDataCommitted(false)
          .physicalPath(Optional.empty());
    } else {
      if (rawItem.get(PHYSICAL_DATA_COMMITTED) == null) {
        // being backwards compatible here, this attribute may not be present
        itemBuilder.size(Long.parseLong(rawItem.get(SIZE).n()))
                .physicalDataCommitted(true)
                .physicalPath(rawItem.get(PHYSICAL_PATH).s());
      } else {
        itemBuilder.size(Long.parseLong(rawItem.get(SIZE).n()))
                .physicalDataCommitted(rawItem.get(PHYSICAL_DATA_COMMITTED).bool())
                .physicalPath(rawItem.get(PHYSICAL_PATH).s());
      }
    }

    return itemBuilder.build();
  }

  @Override
  public void close() {
    try (DynamoDbClient dynamoDbCopy = dynamoDB) {
      // let try-with-resources close it
    }
  }

  /**
   * Encapsulates a page of DynamoDB results and pagination state.
   */
  private static class Page {
    final Iterator<Map<String, AttributeValue>> items;
    final Map<String, AttributeValue> lastEvaluatedKey;

    Page(Iterator<Map<String, AttributeValue>> items, Map<String, AttributeValue> lastEvaluatedKey) {
      this.items = items;
      this.lastEvaluatedKey = lastEvaluatedKey;
    }

    boolean hasMorePages() {
      return lastEvaluatedKey != null;
    }
  }

  /**
   * Functional interface for loading a page of results from DynamoDB.
   * @param <TRequest> The type of DynamoDB request (QueryRequest or ScanRequest)
   */
  @FunctionalInterface
  private interface PageLoader<TRequest> {
    Page loadPage(TRequest request, Map<String, AttributeValue> exclusiveStartKey);
  }

  /**
   * Generic paginated iterator for DynamoDB operations like query and scan.
   * @param <TRequest> The type of DynamoDB request (QueryRequest or ScanRequest)
   */
  private class PaginatedDynamoIterator<TRequest> implements Iterator<DynamoDBItem> {
    private final TRequest request;
    private final PageLoader<TRequest> pageLoader;
    private Page currentPage;

    PaginatedDynamoIterator(TRequest request, PageLoader<TRequest> pageLoader) {
      this.request = request;
      this.pageLoader = pageLoader;
      // no start key initially
      currentPage = loadNextPage(null);
    }

    private Page loadNextPage(Map<String, AttributeValue> exclusiveStartKey) {
      return pageLoader.loadPage(request, exclusiveStartKey);
    }

    @Override
    public boolean hasNext() {
      // Keep loading pages until we find items or run out of pages.
      // DynamoDB can return empty pages with lastEvaluatedKey when filter expressions exclude all items.
      while (!currentPage.items.hasNext() && currentPage.hasMorePages()) {
        currentPage = loadNextPage(currentPage.lastEvaluatedKey);
      }

      return currentPage.items.hasNext();
    }

    @Override
    public DynamoDBItem next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      return rawItemToDynamoDBItem(currentPage.items.next());
    }
  }

  /**
   * Loads a page of results from a DynamoDB Query operation.
   */
  private Page loadQueryPage(QueryRequest request, Map<String, AttributeValue> exclusiveStartKey) {
    QueryRequest.Builder builder = request.toBuilder();
    if (exclusiveStartKey != null) {
      builder.exclusiveStartKey(exclusiveStartKey);
    }

    QueryResponse response = dynamoDB.query(builder.build());
    Map<String, AttributeValue> lastKey = response.hasLastEvaluatedKey() && !response.lastEvaluatedKey().isEmpty()
      ? response.lastEvaluatedKey()
      : null;

    return new Page(response.items().iterator(), lastKey);
  }

  /**
   * Loads a page of results from a DynamoDB Scan operation.
   */
  private Page loadScanPage(ScanRequest request, Map<String, AttributeValue> exclusiveStartKey) {
    ScanRequest.Builder builder = request.toBuilder();
    if (exclusiveStartKey != null) {
      builder.exclusiveStartKey(exclusiveStartKey);
    }

    ScanResponse response = dynamoDB.scan(builder.build());
    Map<String, AttributeValue> lastKey = response.hasLastEvaluatedKey() && !response.lastEvaluatedKey().isEmpty()
      ? response.lastEvaluatedKey()
      : null;

    return new Page(response.items().iterator(), lastKey);
  }

  private class TransactionImpl implements Transaction {
    private final List<TransactWriteItem> transactItems = new ArrayList<>();
    private final String clientRequestToken = UUID.randomUUID().toString();

    @Override
    public void addItemToPut(DynamoDBItem item, boolean enforceItemNotPresent) {
      Put.Builder putBuilder = Put.builder().tableName(tableName).item(toRawDynamoDBItem(item));

      if (enforceItemNotPresent) {
        putBuilder.expressionAttributeNames(CREATE_ITEM_IF_NOT_EXISTS_ATT_NAMES);
        putBuilder.conditionExpression(String.format("attribute_not_exists(#p) and attribute_not_exists(%s)", SORT_KEY));
      }

      transactItems.add(TransactWriteItem.builder().put(putBuilder.build()).build());
    }

    @Override
    public void addItemToDelete(DynamoDBItem item) {
      Delete delete = Delete.builder()
          .tableName(tableName)
          .key(toRawDynamoDBKey(item))
          .build();
      transactItems.add(TransactWriteItem.builder().delete(delete).build());
    }

    @Override
    public CompletableFuture<Boolean> commitAsync() {
      return runtime.async(this::commit);
    }

    @Override
    public boolean commit() {
      try {
        TransactWriteItemsRequest request = TransactWriteItemsRequest.builder()
            .transactItems(transactItems)
            .clientRequestToken(clientRequestToken)
            .build();
        dynamoDB.transactWriteItems(request);
      } catch (TransactionConflictException | TransactionCanceledException | ConditionalCheckFailedException e) {
        LOG.error("Transaction conflict occurred on request: {}", transactItems);
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
