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
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import junit.framework.AssertionFailedError;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.util.*;
import java.util.stream.Collectors;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class AmazonDynamoDBStorageTest {

  private AmazonDynamoDBStorage amazonDynamoDBStorage;

  @Mock
  private DynamoDbClient mockDynamoDBClient;

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
    doAnswer(it -> PutItemResponse.builder().build()).when(mockDynamoDBClient).putItem(putItemRequestCaptor.capture());

    amazonDynamoDBStorage.putItem(item);

    PutItemRequest capture = putItemRequestCaptor.getValue();
    assertEquals("table", capture.tableName());
    assertDynamoItemSameAsItem(item, capture.item());
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
    doAnswer(it -> PutItemResponse.builder().build()).when(mockDynamoDBClient).putItem(putItemRequestCaptor.capture());

    amazonDynamoDBStorage.putItem(item);

    PutItemRequest capture = putItemRequestCaptor.getValue();
    assertEquals("table", capture.tableName());
    assertDynamoItemSameAsItem(item, capture.item());
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
      return GetItemResponse.builder().item(dynamoItem).build();
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
      return GetItemResponse.builder().item(dynamoItem).build();
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
      return GetItemResponse.builder().item(item).build();
    }).when(mockDynamoDBClient).getItem(any(GetItemRequest.class));

    DynamoDBItem returnedItem = amazonDynamoDBStorage.getItem("hash", "sort")
        .orElseThrow(AssertionFailedError::new);

    assertDynamoItemSameAsItem(returnedItem, item);
  }

  @Test
  public void testGetItemReturnsEmptyIfNoItemIsStored() {
    doAnswer(it -> {
      assertGetItemSpec((GetItemRequest) it.getArguments()[0], "hash", "sort");
      return GetItemResponse.builder().build();
    }).when(mockDynamoDBClient).getItem(any(GetItemRequest.class));
    assertFalse(amazonDynamoDBStorage.getItem("hash", "sort").isPresent());
  }

  @Test
  public void testDeleteItem() {
    doAnswer(it -> {
      DeleteItemRequest deleteSpec = (DeleteItemRequest) it.getArguments()[0];
      assertKeyComponents(deleteSpec.key(), "hash", "sort");
      return DeleteItemResponse.builder().build();
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
    doAnswer(inv -> TransactWriteItemsResponse.builder().build()).when(mockDynamoDBClient).transactWriteItems(transactWriteItemsCaptor.capture());


    DynamoDBStorage.Transaction transaction = amazonDynamoDBStorage.createTransaction();
    transaction.addItemToPut(item1, true);
    transaction.addItemToDelete(item2);
    assertTrue(transaction.commit());

    TransactWriteItemsRequest transactWriteItemsRequest = transactWriteItemsCaptor.getValue();
    assertNotNull(transactWriteItemsRequest.clientRequestToken());
    assertEquals(2, transactWriteItemsRequest.transactItems().size());
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
    doThrow(TransactionConflictException.builder().message("Conflict").build()).when(mockDynamoDBClient).transactWriteItems(transactWriteItemsCaptor.capture());


    DynamoDBStorage.Transaction transaction = amazonDynamoDBStorage.createTransaction();
    transaction.addItemToPut(item1, true);
    transaction.addItemToDelete(item2);
    assertFalse(transaction.commit());

    TransactWriteItemsRequest transactWriteItemsRequest = transactWriteItemsCaptor.getValue();
    assertNotNull(transactWriteItemsRequest.clientRequestToken());
    assertEquals(2, transactWriteItemsRequest.transactItems().size());
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
    assertNotNull(transactWriteItemsRequest.clientRequestToken());
    assertEquals(2, transactWriteItemsRequest.transactItems().size());
    assertTransactionRequestPutsItem(transactWriteItemsRequest, item1, true);
    assertTransactionRequestDeletesItem(transactWriteItemsRequest, item2);
  }

  @Test
  public void testResourcesAreCleanedUp() {
    amazonDynamoDBStorage.close();

    verify(mockDynamoDBClient, times(1)).close();
  }

  private void assertTransactionRequestDeletesItem(TransactWriteItemsRequest transactWriteItemsRequest, DynamoDBItem item) {
    List<Delete> deletes = transactWriteItemsRequest.transactItems().stream()
        .map(TransactWriteItem::delete)
        .filter(Objects::nonNull)
        .collect(Collectors.toList());

    assertEquals(1, deletes.size());
    assertEquals("table", deletes.get(0).tableName());
    assertKeyComponents(deletes.get(0).key(), item.getHashKey(), item.getSortKey());
  }

  private void assertTransactionRequestPutsItem(TransactWriteItemsRequest transactWriteItemsRequest, DynamoDBItem item,
                                                boolean checkEnforcesNotPresent) {
    List<Put> puts = transactWriteItemsRequest.transactItems().stream()
        .map(TransactWriteItem::put)
        .filter(Objects::nonNull)
        .collect(Collectors.toList());

    assertEquals(1, puts.size());
    assertEquals("table", puts.get(0).tableName());
    if (checkEnforcesNotPresent) {
      assertEquals(AmazonDynamoDBStorage.CREATE_ITEM_IF_NOT_EXISTS_ATT_NAMES, puts.get(0).expressionAttributeNames());
      assertEquals("attribute_not_exists(#p) and attribute_not_exists(children)", puts.get(0).conditionExpression());
    }
    assertDynamoItemSameAsItem(item, puts.get(0).item());
  }

  @SafeVarargs
  private final void mockAmazonQueryResponse(String hash, Map<String, AttributeValue>... mockItems) {
    int pages = mockItems.length;

    Map<Map<String, AttributeValue>, QueryResponse> queryResultMap = new HashMap<>();
    QueryResponse firstResult = null;
    for (int i = pages - 1; i >= 0; i--) {
      QueryResponse.Builder builder = QueryResponse.builder().items(mockItems[i]);
      if (i > 0) {
        QueryResponse result = builder.lastEvaluatedKey(mockItems[i]).build();
        queryResultMap.put(mockItems[i - 1], result);
      } else {
        firstResult = builder.lastEvaluatedKey(mockItems[i]).build();
      }
    }

    QueryResponse finalFirstResult = firstResult;
    doAnswer(inv -> {
      QueryRequest request = inv.getArgument(0);
      assertQuerySpec(request, hash);
      if (request.exclusiveStartKey() == null || request.exclusiveStartKey().isEmpty()) {
        return finalFirstResult;
      }
      return queryResultMap.getOrDefault(request.exclusiveStartKey(), QueryResponse.builder().build());
    }).when(mockDynamoDBClient).query(any(QueryRequest.class));
  }

  @SafeVarargs
  private final void mockAmazonScanResponse(int segmentIndex, int totalSegments, Map<String, AttributeValue>... mockItems) {
    int pages = mockItems.length;

    Map<Map<String, AttributeValue>, ScanResponse> scanResultMap = new HashMap<>();
    ScanResponse firstResult = null;
    for (int i = pages - 1; i >= 0; i--) {
      ScanResponse.Builder builder = ScanResponse.builder().items(mockItems[i]);
      if (i > 0) {
        ScanResponse result = builder.lastEvaluatedKey(mockItems[i]).build();
        scanResultMap.put(mockItems[i - 1], result);
      } else {
        firstResult = builder.lastEvaluatedKey(mockItems[i]).build();
      }
    }

    ScanResponse finalFirstResult = firstResult;
    doAnswer(inv -> {
      ScanRequest request = inv.getArgument(0);
      assertScanSpec(request, segmentIndex, totalSegments);
      if (request.exclusiveStartKey() == null || request.exclusiveStartKey().isEmpty()) {
        return finalFirstResult;
      }
      return scanResultMap.getOrDefault(request.exclusiveStartKey(), ScanResponse.builder().build());
    }).when(mockDynamoDBClient).scan(any(ScanRequest.class));
  }

  private static void assertDynamoItemSameAsItem(DynamoDBItem dynamoDBItem, Map<String, AttributeValue> item) {
    assertEquals(dynamoDBItem.getHashKey(), item.get(AmazonDynamoDBStorage.HASH_KEY).s());
    assertEquals(dynamoDBItem.getSortKey(), item.get(AmazonDynamoDBStorage.SORT_KEY).s());
    assertEquals(dynamoDBItem.getCreationTime(), Long.parseLong(item.get(AmazonDynamoDBStorage.CREATION_TIME).n()));
    assertEquals(dynamoDBItem.id(), UUID.fromString(item.get(AmazonDynamoDBStorage.ID).s()));
    assertEquals(dynamoDBItem.version(), Long.parseLong(item.get(AmazonDynamoDBStorage.VERSION).n()));

    assertEquals(dynamoDBItem.isDirectory(), item.get(AmazonDynamoDBStorage.IS_DIR).bool());
    if (dynamoDBItem.isDirectory()) {
      assertEquals(0, dynamoDBItem.getSize());
      assertFalse(dynamoDBItem.physicalDataCommitted());
      assertFalse(dynamoDBItem.getPhysicalPath().isPresent());
      assertFalse(item.containsKey(AmazonDynamoDBStorage.SIZE));
      assertFalse(item.containsKey(AmazonDynamoDBStorage.PHYSICAL_PATH));
      assertFalse(item.containsKey(AmazonDynamoDBStorage.PHYSICAL_DATA_COMMITTED));
    } else {
      assertEquals(dynamoDBItem.getSize(), Long.parseLong(item.get(AmazonDynamoDBStorage.SIZE).n()));
      assertEquals(dynamoDBItem.getPhysicalPath().get(), item.get(AmazonDynamoDBStorage.PHYSICAL_PATH).s());
      if (item.get(AmazonDynamoDBStorage.PHYSICAL_DATA_COMMITTED) == null) {
        // backwards compatibility check; if the attribute is missing, assume it's true
        assertTrue(dynamoDBItem.physicalDataCommitted());
      } else {
        assertEquals(dynamoDBItem.physicalDataCommitted(), item.get(AmazonDynamoDBStorage.PHYSICAL_DATA_COMMITTED).bool());
      }
    }

  }

  private static void assertKeyComponents(Map<String, AttributeValue> keyAttributes, String hash, String sort) {
    assertEquals(2, keyAttributes.size());
    assertEquals(hash, keyAttributes.get(AmazonDynamoDBStorage.HASH_KEY).s());
    assertEquals(sort, keyAttributes.get(AmazonDynamoDBStorage.SORT_KEY).s());
  }

  private static void assertScanSpec(ScanRequest spec, int segmentIndex, int totalSegments) {
    assertTrue(spec.consistentRead());
    assertNotNull(spec.projectionExpression());
    assertNotNull(spec.expressionAttributeNames());
    assertEquals(segmentIndex, spec.segment().intValue());
    assertEquals(totalSegments, spec.totalSegments().intValue());
  }

  private static void assertGetItemSpec(GetItemRequest spec, String hashKey, String sort) {
    assertEquals("table", spec.tableName());
    assertTrue(spec.consistentRead());
    assertKeyComponents(spec.key(), hashKey, sort);
    assertNotNull(spec.projectionExpression());
    assertNotNull(spec.expressionAttributeNames());
  }

  private static void assertQuerySpec(QueryRequest querySpec, String hashKey) {
    assertEquals(1, querySpec.keyConditions().size());
    assertEquals(ComparisonOperator.EQ.toString(),
                 querySpec.keyConditions().get(AmazonDynamoDBStorage.HASH_KEY).comparisonOperatorAsString());
    assertEquals(Collections.singletonList(ItemUtils.toAttributeValue(hashKey)),
                 querySpec.keyConditions().get(AmazonDynamoDBStorage.HASH_KEY).attributeValueList());
    assertTrue(querySpec.consistentRead());
    assertNotNull(querySpec.projectionExpression());
    assertNotNull(querySpec.expressionAttributeNames());
  }
}
