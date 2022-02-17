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
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.document.ItemUtils;
import com.amazonaws.services.dynamodbv2.model.*;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import junit.framework.AssertionFailedError;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.*;
import java.util.stream.Collectors;

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;

public class AmazonDynamoDBStorageTest {

  private AmazonDynamoDBStorage amazonDynamoDBStorage;

  @Mock
  private AmazonDynamoDB mockDynamoDBClient;

  @Mock
  private FileSystemRuntime mockRuntime;

  ArgumentCaptor<PutItemRequest> putItemRequestCaptor;

  ArgumentCaptor<TransactWriteItemsRequest> transactWriteItemsCaptor;

  private static final Set<String> EXPECTED_ATTRIBUTES_TO_GET = Sets.newHashSet(AmazonDynamoDBStorage.HASH_KEY,
                                                                               AmazonDynamoDBStorage.SORT_KEY,
                                                                               AmazonDynamoDBStorage.IS_DIR,
                                                                               AmazonDynamoDBStorage.CREATION_TIME,
                                                                               AmazonDynamoDBStorage.SIZE,
                                                                               AmazonDynamoDBStorage.PHYSICAL_PATH,
                                                                               AmazonDynamoDBStorage.PHYSICAL_DATA_COMMITTED,
                                                                               AmazonDynamoDBStorage.VERSION,
                                                                               AmazonDynamoDBStorage.ID);

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
    amazonDynamoDBStorage = new AmazonDynamoDBStorage(mockDynamoDBClient, "table", mockRuntime);
    putItemRequestCaptor = ArgumentCaptor.forClass(PutItemRequest.class);
    transactWriteItemsCaptor = ArgumentCaptor.forClass(TransactWriteItemsRequest.class);
  }

  @Test
  public void testPutFileItem() {
    String physicalPath = UUID.randomUUID().toString();
    DynamoDBItem item = DynamoDBItem.builder()
        .hashKey("hash")
        .sortKey("sort")
        .creationTime(100)
        .isDirectory(false)
        .size(10)
        .physicalPath(physicalPath)
        .physicalDataCommitted(true)
        .id(UUID.randomUUID())
        .version(1)
        .build();
    doAnswer(it -> null).when(mockDynamoDBClient).putItem(putItemRequestCaptor.capture());

    amazonDynamoDBStorage.putItem(item);

    PutItemRequest capture = putItemRequestCaptor.getValue();
    assertEquals("table", capture.getTableName());
    assertDynamoItemSameAsItem(item, capture.getItem());
  }

  @Test
  public void testPutDirectoryItem() {
    DynamoDBItem item = DynamoDBItem.builder()
        .hashKey("hash")
        .sortKey("sort")
        .creationTime(100)
        .isDirectory(true)
        .size(0)
        .physicalPath(Optional.empty())
        .physicalDataCommitted(false)
        .id(UUID.randomUUID())
        .version(1)
        .build();
    doAnswer(it -> null).when(mockDynamoDBClient).putItem(putItemRequestCaptor.capture());

    amazonDynamoDBStorage.putItem(item);

    PutItemRequest capture = putItemRequestCaptor.getValue();
    assertEquals("table", capture.getTableName());
    assertDynamoItemSameAsItem(item, capture.getItem());
  }

  @Test
  public void testGetFileItemSuccessfulIfItemIsStored() {
    Map<String, AttributeValue> dynamoItem = new HashMap<>();
    dynamoItem.put(AmazonDynamoDBStorage.HASH_KEY, ItemUtils.toAttributeValue("hash"));
    dynamoItem.put(AmazonDynamoDBStorage.SORT_KEY, ItemUtils.toAttributeValue("sort"));
    dynamoItem.put(AmazonDynamoDBStorage.CREATION_TIME, ItemUtils.toAttributeValue(100));
    dynamoItem.put(AmazonDynamoDBStorage.IS_DIR, ItemUtils.toAttributeValue(false));
    dynamoItem.put(AmazonDynamoDBStorage.SIZE, ItemUtils.toAttributeValue(10));
    dynamoItem.put(AmazonDynamoDBStorage.PHYSICAL_PATH, ItemUtils.toAttributeValue(UUID.randomUUID().toString()));
    dynamoItem.put(AmazonDynamoDBStorage.PHYSICAL_DATA_COMMITTED, ItemUtils.toAttributeValue(false));
    dynamoItem.put(AmazonDynamoDBStorage.ID, ItemUtils.toAttributeValue(UUID.randomUUID().toString()));
    dynamoItem.put(AmazonDynamoDBStorage.VERSION, ItemUtils.toAttributeValue(1));
    doAnswer(it -> {
      assertGetItemSpec((GetItemRequest) it.getArguments()[0], "hash", "sort");
      return new GetItemResult().withItem(dynamoItem);
    }).when(mockDynamoDBClient).getItem(any(GetItemRequest.class));

    DynamoDBItem returnedItem = amazonDynamoDBStorage.getItem("hash", "sort")
            .orElseThrow(AssertionFailedError::new);
    assertDynamoItemSameAsItem(returnedItem, dynamoItem);
  }

  @Test
  public void testGetFileItemSuccessfulIfItemIsStoredMissingPhyCommittedAttribute() {
    // test case for backwards compatibility scenarios
    Map<String, AttributeValue> dynamoItem = new HashMap<>();
    dynamoItem.put(AmazonDynamoDBStorage.HASH_KEY, ItemUtils.toAttributeValue("hash"));
    dynamoItem.put(AmazonDynamoDBStorage.SORT_KEY, ItemUtils.toAttributeValue("sort"));
    dynamoItem.put(AmazonDynamoDBStorage.CREATION_TIME, ItemUtils.toAttributeValue(100));
    dynamoItem.put(AmazonDynamoDBStorage.IS_DIR, ItemUtils.toAttributeValue(false));
    dynamoItem.put(AmazonDynamoDBStorage.SIZE, ItemUtils.toAttributeValue(10));
    dynamoItem.put(AmazonDynamoDBStorage.PHYSICAL_PATH, ItemUtils.toAttributeValue(UUID.randomUUID().toString()));
    dynamoItem.put(AmazonDynamoDBStorage.ID, ItemUtils.toAttributeValue(UUID.randomUUID().toString()));
    dynamoItem.put(AmazonDynamoDBStorage.VERSION, ItemUtils.toAttributeValue(1));
    doAnswer(it -> {
      assertGetItemSpec((GetItemRequest) it.getArguments()[0], "hash", "sort");
      return new GetItemResult().withItem(dynamoItem);
    }).when(mockDynamoDBClient).getItem(any(GetItemRequest.class));

    DynamoDBItem returnedItem = amazonDynamoDBStorage.getItem("hash", "sort")
            .orElseThrow(AssertionFailedError::new);
    assertDynamoItemSameAsItem(returnedItem, dynamoItem);
  }

  @Test
  public void testGetDirectoryItemSuccessfulIfItemIsStored() {
    Map<String, AttributeValue> item = new HashMap<>();
    item.put(AmazonDynamoDBStorage.HASH_KEY, ItemUtils.toAttributeValue("hash"));
    item.put(AmazonDynamoDBStorage.SORT_KEY, ItemUtils.toAttributeValue("sort"));
    item.put(AmazonDynamoDBStorage.CREATION_TIME, ItemUtils.toAttributeValue(100));
    item.put(AmazonDynamoDBStorage.IS_DIR, ItemUtils.toAttributeValue(true));
    item.put(AmazonDynamoDBStorage.VERSION, ItemUtils.toAttributeValue(1));
    item.put(AmazonDynamoDBStorage.ID, ItemUtils.toAttributeValue(UUID.randomUUID().toString()));
    doAnswer(it -> {
      assertGetItemSpec((GetItemRequest) it.getArguments()[0], "hash", "sort");
      return new GetItemResult().withItem(item);
    }).when(mockDynamoDBClient).getItem(any(GetItemRequest.class));

    DynamoDBItem returnedItem = amazonDynamoDBStorage.getItem("hash", "sort")
        .orElseThrow(AssertionFailedError::new);

    assertDynamoItemSameAsItem(returnedItem, item);
  }

  @Test
  public void testGetItemReturnsEmptyIfNoItemIsStored() {
    doAnswer(it -> {
      assertGetItemSpec((GetItemRequest) it.getArguments()[0], "hash", "sort");
      return null;
    }).when(mockDynamoDBClient).getItem(any(GetItemRequest.class));
    assertFalse(amazonDynamoDBStorage.getItem("hash", "sort").isPresent());
  }

  @Test
  public void testDeleteItem() {
    doAnswer(it -> {
      DeleteItemRequest deleteSpec = (DeleteItemRequest) it.getArguments()[0];
      assertKeyComponents(deleteSpec.getKey(), "hash", "sort");
      return null;
    }).when(mockDynamoDBClient).deleteItem(any(DeleteItemRequest.class));

    amazonDynamoDBStorage.deleteItem("hash", "sort");
  }

  @Test
  public void testListItems() {
    Map<String, AttributeValue> child1 = new HashMap<>();
    child1.put(AmazonDynamoDBStorage.HASH_KEY, ItemUtils.toAttributeValue("hash"));
    child1.put(AmazonDynamoDBStorage.SORT_KEY, ItemUtils.toAttributeValue("sort1"));
    child1.put(AmazonDynamoDBStorage.CREATION_TIME, ItemUtils.toAttributeValue(100));
    child1.put(AmazonDynamoDBStorage.IS_DIR, ItemUtils.toAttributeValue(false));
    child1.put(AmazonDynamoDBStorage.SIZE, ItemUtils.toAttributeValue(10));
    child1.put(AmazonDynamoDBStorage.PHYSICAL_PATH, ItemUtils.toAttributeValue(UUID.randomUUID().toString()));
    child1.put(AmazonDynamoDBStorage.PHYSICAL_DATA_COMMITTED, ItemUtils.toAttributeValue(true));
    child1.put(AmazonDynamoDBStorage.VERSION, ItemUtils.toAttributeValue(1));
    child1.put(AmazonDynamoDBStorage.ID, ItemUtils.toAttributeValue(UUID.randomUUID().toString()));

    Map<String, AttributeValue> child2 = new HashMap<>();
    child2.put(AmazonDynamoDBStorage.HASH_KEY, ItemUtils.toAttributeValue("hash"));
    child2.put(AmazonDynamoDBStorage.SORT_KEY, ItemUtils.toAttributeValue("sort2"));
    child2.put(AmazonDynamoDBStorage.CREATION_TIME, ItemUtils.toAttributeValue(200));
    child2.put(AmazonDynamoDBStorage.IS_DIR, ItemUtils.toAttributeValue(true));
    child2.put(AmazonDynamoDBStorage.VERSION, ItemUtils.toAttributeValue(1));
    child2.put(AmazonDynamoDBStorage.ID, ItemUtils.toAttributeValue(UUID.randomUUID().toString()));

    Map<String, AttributeValue> child3 = new HashMap<>();
    child3.put(AmazonDynamoDBStorage.HASH_KEY, ItemUtils.toAttributeValue("hash"));
    child3.put(AmazonDynamoDBStorage.SORT_KEY, ItemUtils.toAttributeValue("sort3"));
    child3.put(AmazonDynamoDBStorage.CREATION_TIME, ItemUtils.toAttributeValue(300));
    child3.put(AmazonDynamoDBStorage.IS_DIR, ItemUtils.toAttributeValue(false));
    child3.put(AmazonDynamoDBStorage.SIZE, ItemUtils.toAttributeValue(30));
    child3.put(AmazonDynamoDBStorage.PHYSICAL_PATH, ItemUtils.toAttributeValue(UUID.randomUUID().toString()));
    child3.put(AmazonDynamoDBStorage.PHYSICAL_DATA_COMMITTED, ItemUtils.toAttributeValue(false));
    child3.put(AmazonDynamoDBStorage.VERSION, ItemUtils.toAttributeValue(1));
    child3.put(AmazonDynamoDBStorage.ID, ItemUtils.toAttributeValue(UUID.randomUUID().toString()));

    mockAmazonQueryResponse("hash", child1, child2, child3);

    Iterable<DynamoDBItem> i = amazonDynamoDBStorage.list("hash");
    List<DynamoDBItem> listedItems = Lists.newArrayList(i);

    DynamoDBItem listedChild1 = listedItems.get(0);
    assertDynamoItemSameAsItem(listedChild1, child1);
    DynamoDBItem listedChild2 = listedItems.get(1);
    assertDynamoItemSameAsItem(listedChild2, child2);
  }

  @Test
  public void testScanItems() {
    Map<String, AttributeValue> child1 = new HashMap<>();
    child1.put(AmazonDynamoDBStorage.HASH_KEY, ItemUtils.toAttributeValue("hash1"));
    child1.put(AmazonDynamoDBStorage.SORT_KEY, ItemUtils.toAttributeValue("sort1"));
    child1.put(AmazonDynamoDBStorage.CREATION_TIME, ItemUtils.toAttributeValue(100));
    child1.put(AmazonDynamoDBStorage.IS_DIR, ItemUtils.toAttributeValue(false));
    child1.put(AmazonDynamoDBStorage.SIZE, ItemUtils.toAttributeValue(10));
    child1.put(AmazonDynamoDBStorage.PHYSICAL_PATH, ItemUtils.toAttributeValue(UUID.randomUUID().toString()));
    child1.put(AmazonDynamoDBStorage.PHYSICAL_DATA_COMMITTED, ItemUtils.toAttributeValue(true));
    child1.put(AmazonDynamoDBStorage.VERSION, ItemUtils.toAttributeValue(1));
    child1.put(AmazonDynamoDBStorage.ID, ItemUtils.toAttributeValue(UUID.randomUUID().toString()));

    Map<String, AttributeValue> child2 = new HashMap<>();
    child2.put(AmazonDynamoDBStorage.HASH_KEY, ItemUtils.toAttributeValue("hash2"));
    child2.put(AmazonDynamoDBStorage.SORT_KEY, ItemUtils.toAttributeValue("sort2"));
    child2.put(AmazonDynamoDBStorage.CREATION_TIME, ItemUtils.toAttributeValue(200));
    child2.put(AmazonDynamoDBStorage.IS_DIR, ItemUtils.toAttributeValue(true));
    child2.put(AmazonDynamoDBStorage.VERSION, ItemUtils.toAttributeValue(1));
    child2.put(AmazonDynamoDBStorage.ID, ItemUtils.toAttributeValue(UUID.randomUUID().toString()));

    Map<String, AttributeValue> child3 = new HashMap<>();
    child3.put(AmazonDynamoDBStorage.HASH_KEY, ItemUtils.toAttributeValue("hash3"));
    child3.put(AmazonDynamoDBStorage.SORT_KEY, ItemUtils.toAttributeValue("sort3"));
    child3.put(AmazonDynamoDBStorage.CREATION_TIME, ItemUtils.toAttributeValue(300));
    child3.put(AmazonDynamoDBStorage.IS_DIR, ItemUtils.toAttributeValue(false));
    child3.put(AmazonDynamoDBStorage.SIZE, ItemUtils.toAttributeValue(30));
    child3.put(AmazonDynamoDBStorage.PHYSICAL_PATH, ItemUtils.toAttributeValue(UUID.randomUUID().toString()));
    child3.put(AmazonDynamoDBStorage.PHYSICAL_DATA_COMMITTED, ItemUtils.toAttributeValue(false));
    child3.put(AmazonDynamoDBStorage.VERSION, ItemUtils.toAttributeValue(1));
    child3.put(AmazonDynamoDBStorage.ID, ItemUtils.toAttributeValue(UUID.randomUUID().toString()));

    mockAmazonScanResponse(1, 2, child1, child2, child3);

    Iterable<DynamoDBItem> i = amazonDynamoDBStorage.scan(1, 2);
    List<DynamoDBItem> listedItems = Lists.newArrayList(i);

    DynamoDBItem listedChild1 = listedItems.get(0);
    assertDynamoItemSameAsItem(listedChild1, child1);
    DynamoDBItem listedChild2 = listedItems.get(1);
    assertDynamoItemSameAsItem(listedChild2, child2);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testScanThrowsErrorForInvalidPartitionIndex() {
    Iterable<DynamoDBItem> ignored = amazonDynamoDBStorage.scan(-1, 2);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testScanThrowsErrorForZeroPartitionCount() {
    Iterable<DynamoDBItem> ignored = amazonDynamoDBStorage.scan(0, 0);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testScanThrowsErrorForPartitionIndexLargerThanPartitionCount() {
    Iterable<DynamoDBItem> ignored = amazonDynamoDBStorage.scan(3, 2);
  }

  @Test
  public void testTransactionCommit() {
    DynamoDBItem item1 = DynamoDBItem.builder()
        .hashKey("hash1")
        .sortKey("sort1")
        .creationTime(100)
        .isDirectory(false)
        .size(200)
        .physicalPath("phy1")
        .physicalDataCommitted(true)
        .id(UUID.randomUUID())
        .version(1)
        .build();
    DynamoDBItem item2 = DynamoDBItem.builder()
        .hashKey("hash2")
        .sortKey("sort2")
        .creationTime(200)
        .isDirectory(false)
        .size(300)
        .physicalDataCommitted(true)
        .physicalPath("phy2")
        .version(2)
        .id(UUID.randomUUID())
        .build();
    doAnswer(inv -> null).when(mockDynamoDBClient).transactWriteItems(transactWriteItemsCaptor.capture());


    DynamoDBStorage.Transaction transaction = amazonDynamoDBStorage.createTransaction();
    transaction.addItemToPut(item1, true);
    transaction.addItemToDelete(item2);
    assertTrue(transaction.commit());

    TransactWriteItemsRequest transactWriteItemsRequest = transactWriteItemsCaptor.getValue();
    assertNotNull(transactWriteItemsRequest.getClientRequestToken());
    assertEquals(2, transactWriteItemsRequest.getTransactItems().size());
    assertTransactionRequestPutsItem(transactWriteItemsRequest, item1, true);
    assertTransactionRequestDeletesItem(transactWriteItemsRequest, item2);
  }

  @Test
  public void testTransactionCommitReturnsFalseOnTransactionConflict() {
    DynamoDBItem item1 = DynamoDBItem.builder()
        .hashKey("hash1")
        .sortKey("sort1")
        .creationTime(100)
        .isDirectory(false)
        .size(200)
        .physicalPath("phy1")
        .physicalDataCommitted(true)
        .id(UUID.randomUUID())
        .version(1)
        .build();
    DynamoDBItem item2 = DynamoDBItem.builder()
        .hashKey("hash2")
        .sortKey("sort2")
        .creationTime(200)
        .isDirectory(false)
        .size(300)
        .physicalDataCommitted(true)
        .physicalPath("phy2")
        .version(2)
        .id(UUID.randomUUID())
        .build();
    doThrow(new TransactionConflictException("Conflict")).when(mockDynamoDBClient).transactWriteItems(transactWriteItemsCaptor.capture());


    DynamoDBStorage.Transaction transaction = amazonDynamoDBStorage.createTransaction();
    transaction.addItemToPut(item1, true);
    transaction.addItemToDelete(item2);
    assertFalse(transaction.commit());

    TransactWriteItemsRequest transactWriteItemsRequest = transactWriteItemsCaptor.getValue();
    assertNotNull(transactWriteItemsRequest.getClientRequestToken());
    assertEquals(2, transactWriteItemsRequest.getTransactItems().size());
    assertTransactionRequestPutsItem(transactWriteItemsRequest, item1, true);
    assertTransactionRequestDeletesItem(transactWriteItemsRequest, item2);
  }

  @Test
  public void testTransactionCommitReturnsFalseOnRuntimeError() {
    DynamoDBItem item1 = DynamoDBItem.builder()
        .hashKey("hash1")
        .sortKey("sort1")
        .creationTime(100)
        .isDirectory(false)
        .size(200)
        .physicalPath("phy1")
        .physicalDataCommitted(true)
        .id(UUID.randomUUID())
        .version(1)
        .build();
    DynamoDBItem item2 = DynamoDBItem.builder()
        .hashKey("hash2")
        .sortKey("sort2")
        .creationTime(200)
        .isDirectory(false)
        .size(300)
        .physicalDataCommitted(true)
        .physicalPath("phy2")
        .version(2)
        .id(UUID.randomUUID())
        .build();
    doThrow(new RuntimeException("I/O error")).when(mockDynamoDBClient).transactWriteItems(transactWriteItemsCaptor.capture());


    DynamoDBStorage.Transaction transaction = amazonDynamoDBStorage.createTransaction();
    transaction.addItemToPut(item1, true);
    transaction.addItemToDelete(item2);
    assertFalse(transaction.commit());

    TransactWriteItemsRequest transactWriteItemsRequest = transactWriteItemsCaptor.getValue();
    assertNotNull(transactWriteItemsRequest.getClientRequestToken());
    assertEquals(2, transactWriteItemsRequest.getTransactItems().size());
    assertTransactionRequestPutsItem(transactWriteItemsRequest, item1, true);
    assertTransactionRequestDeletesItem(transactWriteItemsRequest, item2);
  }

  private void assertTransactionRequestDeletesItem(TransactWriteItemsRequest transactWriteItemsRequest, DynamoDBItem item) {
    List<Delete> deletes = transactWriteItemsRequest.getTransactItems().stream()
        .map(TransactWriteItem::getDelete)
        .filter(it -> it != null)
        .collect(Collectors.toList());

    assertEquals(1, deletes.size());
    assertEquals("table", deletes.get(0).getTableName());
    assertKeyComponents(deletes.get(0).getKey(), item.getHashKey(), item.getSortKey());
  }

  private void assertTransactionRequestPutsItem(TransactWriteItemsRequest transactWriteItemsRequest, DynamoDBItem item,
                                                boolean checkEnforcesNotPresent) {
    List<Put> puts = transactWriteItemsRequest.getTransactItems().stream()
        .map(TransactWriteItem::getPut)
        .filter(it -> it != null)
        .collect(Collectors.toList());

    assertEquals(1, puts.size());
    assertEquals("table", puts.get(0).getTableName());
    if (checkEnforcesNotPresent) {
      assertEquals(AmazonDynamoDBStorage.CREATE_ITEM_IF_NOT_EXISTS_ATT_NAMES, puts.get(0).getExpressionAttributeNames());
      assertEquals("attribute_not_exists(#p) and attribute_not_exists(children)", puts.get(0).getConditionExpression());
    }
    assertDynamoItemSameAsItem(item, puts.get(0).getItem());
  }

  private void mockAmazonQueryResponse(String hash, Map<String, AttributeValue>... mockItems) {
    int pages = mockItems.length;

    Map<Map<String, AttributeValue>, QueryResult> queryResultMap = new HashMap<>();
    QueryResult firstResult = null;
    for (int i = pages - 1; i >= 0; i--) {
      QueryResult result = new QueryResult().withItems(mockItems[i]);
      if (i > 0) {
        result = result.withLastEvaluatedKey(mockItems[i]);
        queryResultMap.put(mockItems[i - 1], result);
      } else {
        firstResult = result.withLastEvaluatedKey(mockItems[i]);
      }
    }

    QueryResult finalFirstResult = firstResult;
    doAnswer(inv -> {
      QueryRequest request = inv.getArgument(0);
      assertQuerySpec(request, hash);
      if (request.getExclusiveStartKey() == null) {
        return finalFirstResult;
      }
      return queryResultMap.getOrDefault(request.getExclusiveStartKey(), new QueryResult());
    }).when(mockDynamoDBClient).query(any(QueryRequest.class));
  }

  private void mockAmazonScanResponse(int segmentIndex, int totalSegments, Map<String, AttributeValue>... mockItems) {
    int pages = mockItems.length;

    Map<Map<String, AttributeValue>, ScanResult> scanResultMap = new HashMap<>();
    ScanResult firstResult = null;
    for (int i = pages - 1; i >= 0; i--) {
      ScanResult result = new ScanResult().withItems(mockItems[i]);
      if (i > 0) {
        result = result.withLastEvaluatedKey(mockItems[i]);
        scanResultMap.put(mockItems[i - 1], result);
      } else {
        firstResult = result.withLastEvaluatedKey(mockItems[i]);;
      }
    }

    ScanResult finalFirstResult = firstResult;
    doAnswer(inv -> {
      ScanRequest request = inv.getArgument(0);
      assertScanSpec(request, segmentIndex, totalSegments);
      if (request.getExclusiveStartKey() == null) {
        return finalFirstResult;
      }
      return scanResultMap.getOrDefault(request.getExclusiveStartKey(), new ScanResult());
    }).when(mockDynamoDBClient).scan(any(ScanRequest.class));
  }

  private static void assertDynamoItemSameAsItem(DynamoDBItem dynamoDBItem, Map<String, AttributeValue> item) {
    assertEquals(dynamoDBItem.getHashKey(), item.get(AmazonDynamoDBStorage.HASH_KEY).getS());
    assertEquals(dynamoDBItem.getSortKey(), item.get(AmazonDynamoDBStorage.SORT_KEY).getS());
    assertEquals(dynamoDBItem.getCreationTime(), (long)Long.valueOf(item.get(AmazonDynamoDBStorage.CREATION_TIME).getN()));
    assertEquals(dynamoDBItem.id(), UUID.fromString(item.get(AmazonDynamoDBStorage.ID).getS()));
    assertEquals(dynamoDBItem.version(), (long)Long.valueOf(item.get(AmazonDynamoDBStorage.VERSION).getN()));

    assertEquals(dynamoDBItem.isDirectory(), item.get(AmazonDynamoDBStorage.IS_DIR).getBOOL());
    if (dynamoDBItem.isDirectory()) {
      assertEquals(0, dynamoDBItem.getSize());
      assertFalse(dynamoDBItem.physicalDataCommitted());
      assertFalse(dynamoDBItem.getPhysicalPath().isPresent());
      assertFalse(item.containsKey(AmazonDynamoDBStorage.SIZE));
      assertFalse(item.containsKey(AmazonDynamoDBStorage.PHYSICAL_PATH));
      assertFalse(item.containsKey(AmazonDynamoDBStorage.PHYSICAL_DATA_COMMITTED));
    } else {
      assertEquals(dynamoDBItem.getSize(), (long)Long.valueOf(item.get(AmazonDynamoDBStorage.SIZE).getN()));
      assertEquals(dynamoDBItem.getPhysicalPath().get(), item.get(AmazonDynamoDBStorage.PHYSICAL_PATH).getS());
      if (item.get(AmazonDynamoDBStorage.PHYSICAL_DATA_COMMITTED) == null) {
        // backwards compatibility check; if the attribute is missing, assume it's true
        assertEquals(dynamoDBItem.physicalDataCommitted(), true);
      } else {
        assertEquals(dynamoDBItem.physicalDataCommitted(), item.get(AmazonDynamoDBStorage.PHYSICAL_DATA_COMMITTED).getBOOL());
      }
    }

  }

  private static void assertKeyComponents(Map<String, AttributeValue> keyAttributes, String hash, String sort) {
    assertEquals(2, keyAttributes.size());
    assertEquals(hash, keyAttributes.get(AmazonDynamoDBStorage.HASH_KEY).getS());
    assertEquals(sort, keyAttributes.get(AmazonDynamoDBStorage.SORT_KEY).getS());
  }

  private static void assertScanSpec(ScanRequest spec, int segmentIndex, int totalSegments) {
    assertTrue(spec.isConsistentRead());
    assertEquals(EXPECTED_ATTRIBUTES_TO_GET, Sets.newHashSet(spec.getAttributesToGet()));
    assertEquals(segmentIndex, spec.getSegment().intValue());
    assertEquals(totalSegments, spec.getTotalSegments().intValue());
  }

  private static void assertGetItemSpec(GetItemRequest spec, String hashKey, String sort) {
    assertEquals("table", spec.getTableName());
    assertTrue(spec.isConsistentRead());
    assertKeyComponents(spec.getKey(), hashKey, sort);
    assertEquals(EXPECTED_ATTRIBUTES_TO_GET, Sets.newHashSet(spec.getAttributesToGet()));
  }

  private static void assertQuerySpec(QueryRequest querySpec, String hashKey) {
    assertEquals(1, querySpec.getKeyConditions().size());
    assertEquals(ComparisonOperator.EQ.toString(),
                 querySpec.getKeyConditions().get(AmazonDynamoDBStorage.HASH_KEY).getComparisonOperator());
    assertEquals(Arrays.asList(ItemUtils.toAttributeValue(hashKey)),
                 querySpec.getKeyConditions().get(AmazonDynamoDBStorage.HASH_KEY).getAttributeValueList());
    assertTrue(querySpec.isConsistentRead());
    assertEquals(EXPECTED_ATTRIBUTES_TO_GET, Sets.newHashSet(querySpec.getAttributesToGet()));
  }
}
