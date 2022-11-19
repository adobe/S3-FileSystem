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

package com.adobe.s3fs;

import com.adobe.s3fs.common.configuration.FileSystemConfiguration;
import com.adobe.s3fs.common.configuration.HadoopKeyValueConfiguration;
import com.adobe.s3fs.common.context.FileSystemContext;
import com.adobe.s3fs.common.runtime.FileSystemRuntime;
import com.adobe.s3fs.common.runtime.FileSystemRuntimeFactory;
import com.adobe.s3fs.metastore.api.ObjectHandle;
import com.adobe.s3fs.metastore.api.ObjectMetadata;
import com.adobe.s3fs.metastore.internal.dynamodb.DynamoDBMetadataStore;
import com.adobe.s3fs.metastore.internal.dynamodb.DynamoDBMetadataStoreFactory;
import com.adobe.s3fs.utils.DynamoTable;
import com.adobe.s3fs.utils.ITUtils;
import com.adobe.s3fs.utils.InMemoryMetadataOperationLog;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.collect.Streams;
import junit.framework.AssertionFailedError;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.*;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.*;

public class DynamoDBMetadataStoreIntegrationTest {

  @Rule
  public DynamoTable dynamoTable = new DynamoTable(ITUtils.amazonDynamoDB());


  private DynamoDBMetadataStore metadataStore;

  private InMemoryMetadataOperationLog mockOperationLog;

  private final Random random = new Random();

  private static final String BUCKET = "bucket";

  private static AmazonDynamoDB dynamoDB;

  @BeforeClass
  public static void beforeClass() {
    dynamoDB = ITUtils.amazonDynamoDB();
  }

  @Before
  public void setup() {
    Configuration configuration = new Configuration(false);

    ITUtils.configureSuffixCount(configuration, "bucket", 3);

    ITUtils.mapBucketToTable(configuration, "bucket", dynamoTable.getTable());

    ITUtils.configureDynamoAccess(configuration, "bucket");

    ITUtils.configureAsyncOperations(configuration, "bucket", "ctx");

    ITUtils.setFileSystemContext("ctx");

    mockOperationLog = new InMemoryMetadataOperationLog();

    DynamoDBMetadataStoreFactory metadataStoreFactory = ReflectionUtils.newInstance(DynamoDBMetadataStoreFactory.class, configuration);

    FileSystemConfiguration fileSystemConfiguration =
        new FileSystemConfiguration("bucket", new HadoopKeyValueConfiguration(configuration), () -> Optional.of("ctx"));

    FileSystemRuntime runtime = FileSystemRuntimeFactory.create(fileSystemConfiguration);

    FileSystemContext context = FileSystemContext.builder()
        .bucket("bucket")
        .configuration(fileSystemConfiguration)
        .runtime(runtime)
        .build();

    metadataStore = (DynamoDBMetadataStore) metadataStoreFactory.create(context, mockOperationLog);
  }

  @After
  public void tearDown() throws Exception {
    metadataStore.close();
  }

  @Test
  public void testGetInexistentObjectReturnsEmpty() {
    Path key = new Path("ks://bucket/d1/d2/f");

    assertFalse(metadataStore.getObject(key).isPresent());
  }

  @Test
  public void testGetReturnsPreviouslyAddedKey() {
    ObjectMetadata objectMetadata = fileBuilder()
        .key(new Path("ks://bucket/f"))
        .build();

    storeObjectsInOrder(objectMetadata);

    assertEquals(objectMetadata,
                 metadataStore.getObject(objectMetadata.getKey()).orElseThrow(AssertionFailedError::new).metadata());
    assertEquals(Sets.newHashSet(objectMetadata), Sets.newHashSet(mockOperationLog.getCreatedObjects()));
  }

  @Test
  public void testListReturnsFirstLevelChildren() {
    ObjectMetadata rootDirectory = directoryBuilder()
        .key(new Path("ks://bucket/d1"))
        .build();
    ObjectMetadata child1 = fileBuilder()
        .key(new Path("ks://bucket/d1/f1"))
        .build();
    ObjectMetadata child2 = fileBuilder()
        .key(new Path("ks://bucket/d1/f2"))
        .build();
    ObjectMetadata directoryChild = directoryBuilder()
        .key(new Path("ks://bucket/d1/d2"))
        .build();
    ObjectMetadata child3 = fileBuilder()
        .key(new Path("ks://bucket/d1/d2/f3"))
        .build();

    storeObjectsInOrder(rootDirectory, child1, child2, directoryChild, child3);

    /*
      Act
     */
    Iterable<? extends ObjectHandle> listing = metadataStore.listChildObjects(
        metadataStore.getObject(new Path("ks://bucket/d1")).get());

    /*
     * Assert
     */
    assertEquals(Sets.newHashSet(child1, child2, directoryChild),
                 Sets.newHashSet(handlesToMetadata(listing)));
  }

  @Test
  public void testListReturnEmptyWhenThereAreNoChildren() {
    ObjectMetadata directory = directoryBuilder()
        .key(new Path("ks://bucket/d1"))
        .build();

    storeObjectsInOrder(directory);

    ObjectHandle directoryHandle = metadataStore.getObject(new Path("ks://bucket/d1")).get();
    assertTrue(Lists.newArrayList(metadataStore.listChildObjects(directoryHandle)).isEmpty());
    assertTrue(Lists.newArrayList(metadataStore.recursiveList(directory.getKey())).isEmpty());
    assertTrue(Lists.newArrayList(flatScan(directory.getKey(), 1)).isEmpty());
  }

  @Test
  public void testFileObjectsAreDeleted() {
    Path directoryKey = new Path("ks://bucket/d1");
    ObjectMetadata directory = directoryBuilder()
        .key(directoryKey)
        .build();
    Path childKey = new Path("ks://bucket/d1/f");
    ObjectMetadata child = fileBuilder()
        .key(childKey)
        .build();

    storeObjectsInOrder(directory, child);

    AtomicReference<ObjectMetadata> deleteCallbackCapture = new AtomicReference<>();
    metadataStore.deleteObject(metadataStore.getObject(childKey).get(), o -> {
      deleteCallbackCapture.set(o.metadata());
      return true;
    });

    assertFalse(metadataStore.getObject(child.getKey()).isPresent());
    assertEquals(child, deleteCallbackCapture.get());
    assertEquals(Sets.newHashSet(child), Sets.newHashSet(mockOperationLog.getDeletedObjects()));

    assertTrue(
        Lists.newArrayList(metadataStore
                               .listChildObjects(metadataStore.getObject(directoryKey).get())).isEmpty());
    assertTrue(Lists.newArrayList(metadataStore.recursiveList(directory.getKey())).isEmpty());
    assertTrue(Lists.newArrayList(flatScan(directory.getKey(), 1)).isEmpty());

  }

  @Test
  public void testDirectoryObjectsAreDeleted() {
    /*
     * Prepare
     */
    ObjectMetadata root = rootObject(BUCKET);
    Path directoryKey = new Path("ks://bucket/d1");
    ObjectMetadata directory = directoryBuilder()
        .key(directoryKey)
        .build();
    ObjectMetadata directoryChild = directoryBuilder()
        .key(new Path("ks://bucket/d1/d2"))
        .build();
    ObjectMetadata fileChild1 = fileBuilder()
        .key(new Path("ks://bucket/d1/f1"))
        .build();
    ObjectMetadata fileChild2 = fileBuilder()
        .key(new Path("ks://bucket/d1/d2/f2"))
        .build();

    storeObjectsInOrder(directory, directoryChild, fileChild1, fileChild2);

    /*
     * Act
     */
    Set<ObjectMetadata> deletedObjectsCapture = new HashSet<>();
    assertTrue(metadataStore.deleteObject(metadataStore.getObject(directoryKey).get(),
                                          it -> deletedObjectsCapture.add(it.metadata())));

    /*
     * Assert
     */
    assertMetadataStoreState(wrap(root));
    assertTrue(Lists.newArrayList(metadataStore.recursiveList(root.getKey())).isEmpty());
    assertTrue(Lists.newArrayList(flatScan(root.getKey(), 1)).isEmpty());

    assertAllObjectsAreNotRetrievable(directory, directoryChild, fileChild1, fileChild2);

    assertEquals(Sets.newHashSet(directory, directoryChild, fileChild1, fileChild2), deletedObjectsCapture);
    assertEquals(Sets.newHashSet(fileChild1, fileChild2),
                 Sets.newHashSet(mockOperationLog.getDeletedObjects()));
  }

  @Test
  public void testRecursiveListing() {
    ObjectMetadata directory = directoryBuilder()
        .key(new Path("ks://bucket/d1"))
        .build();
    ObjectMetadata directoryChild = directoryBuilder()
        .key(new Path("ks://bucket/d1/d2"))
        .build();
    ObjectMetadata fileChild1 = fileBuilder()
        .key(new Path("ks://bucket/d1/f1"))
        .build();
    ObjectMetadata fileChild2 = fileBuilder()
        .key(new Path("ks://bucket/d1/d2/f2"))
        .build();

    storeObjectsInOrder(directory, directoryChild, fileChild1, fileChild2);

    Iterable<ObjectMetadata> listing = handlesToMetadata(metadataStore.recursiveList(directory.getKey()));

    assertEquals(Sets.newHashSet(directoryChild, fileChild1, fileChild2), Sets.newHashSet(listing));
  }

  @Test
  public void testFileObjectRename() {
    /*
     * Prepare
     */
    ObjectMetadata root = rootObject(BUCKET);
    ObjectMetadata d1 = directoryBuilder()
        .key(new Path("ks://bucket/d1"))
        .build();
    ObjectMetadata d2 = directoryBuilder()
        .key(new Path("ks://bucket/d2"))
        .build();
    ObjectMetadata.Builder f1Builder = fileBuilder();
    Path f1Key = new Path("ks://bucket/d1/f1");
    ObjectMetadata f1 = f1Builder.key(f1Key).build();

    storeObjectsInOrder(d1, f1, d2);

    /*
     * Act
     */
    Path f2Key = new Path("ks://bucket/d2/f2");
    assertTrue(metadataStore.renameObject(metadataStore.getObject(f1Key).get(), f2Key));

    /*
     * Assert
     */
    assertFalse(metadataStore.getObject(f1.getKey()).isPresent());

    ObjectMetadata f2 = f1.withKey(f2Key);

    assertEquals(set(pair(f1, f2)), Sets.newHashSet(mockOperationLog.getRenamedObjects()));

    ObjectMetadataWrapper rootWrapper = wrap(root)
        .addChildren(d1)
        .addChildren(wrap(d2).addChildren(wrap(f2)));

    assertMetadataStoreState(rootWrapper);
    assertEquals(Sets.newHashSet(d1, d2, f2),
                 Sets.newHashSet(handlesToMetadata(metadataStore.recursiveList(root.getKey()))));
    assertEquals(Sets.newHashSet(d1, d2, f2), Sets.newHashSet(flatScan(root.getKey(), 1)));
  }

  /**
   * Initial directory structure:
   *      d
   *     /
   *    d1
   *   /  \
   *  f1   d2
   *        \
   *         f2
   * Structure after moving /d/d1 to /d/d3:
   *      d
   *     /
   *    d3
   *   /  \
   *  f1   d2
   *        \
   *         f2
   */
  @Test
  public void testDirectoryObjectRename() {
    /*
     * Prepare
     */
    ObjectMetadata root = rootObject(BUCKET);
    ObjectMetadata d = directoryBuilder()
        .key(new Path("ks://bucket/d"))
        .build();
    ObjectMetadata.Builder d1Builder = directoryBuilder();
    Path d1Key = new Path("ks://bucket/d/d1");
    ObjectMetadata d1 = d1Builder.key(d1Key).build();

    ObjectMetadata.Builder d2Builder = directoryBuilder();
    ObjectMetadata d2 = d2Builder.key(new Path("ks://bucket/d/d1/d2")).build();

    ObjectMetadata.Builder f1Builder = fileBuilder();
    ObjectMetadata f1 = f1Builder.key(new Path("ks://bucket/d/d1/f1")).build();

    ObjectMetadata.Builder f2Builder = fileBuilder();
    ObjectMetadata f2 = f2Builder.key(new Path("ks://bucket/d/d1/d2/f2")).build();

    storeObjectsInOrder(d, d1, d2, f1, f2);

    Path newKey = new Path("ks://bucket/d/d3");

    /*
     * Act
     */
    assertTrue(metadataStore.renameObject(metadataStore.getObject(d1Key).get(), newKey));

    /*
     * Assert
     */
    ObjectMetadata d3 = d1.withKey(newKey);

    // assert everything was moved correctly to the new root
    ObjectMetadata d2Moved = d2.withKey(new Path("ks://bucket/d/d3/d2"));
    ObjectMetadata f1Moved = f1.withKey(new Path( "ks://bucket/d/d3/f1"));
    ObjectMetadata f2Moved = f2.withKey(new Path("ks://bucket/d/d3/d2/f2"));

    ObjectMetadataWrapper rootWrapper = wrap(root)
        .addChildren(
            wrap(d)
                .addChildren(
                    wrap(d3)
                        .addChildren(f1Moved)
                        .addChildren(wrap(d2Moved).addChildren(f2Moved))));
    assertMetadataStoreState(rootWrapper);
    assertEquals(Sets.newHashSet(d, d3, d2Moved, f1Moved, f2Moved),
                 Sets.newHashSet(handlesToMetadata(metadataStore.recursiveList(root.getKey()))));
    assertEquals(Sets.newHashSet(d, d3, d2Moved, f1Moved, f2Moved), Sets.newHashSet(flatScan(root.getKey(),1)));

    assertEquals(set(pair(f1, f1Moved), pair(f2, f2Moved)),
                 Sets.newHashSet(mockOperationLog.getRenamedObjects()));

    assertAllObjectsCanBeRetrievedByKey(d, d3, d2Moved, f1Moved, f2Moved);
    assertAllObjectsAreNotRetrievable(d1, d2, f1, f2);
  }

  /**
   *      d
   *     /
   *    d1
   *   /  \
   *  f1   X // this node is not added to the metadata, but f2 is, scanning /d should return f2 as well
   *        \
   *         f2
   */
  @Test
  public void testScanListing() {
    /*
     * Prepare
     */
    ObjectMetadata rootDirectory = directoryBuilder()
        .key(new Path( "ks://bucket//d"))
        .build();
    ObjectMetadata.Builder directory1Builder = directoryBuilder();
    ObjectMetadata directory1 = directory1Builder.key(new Path( "ks://bucket/d/d1")).build();

    ObjectMetadata.Builder file1Builder = fileBuilder();
    ObjectMetadata file1 = file1Builder.key(new Path( "ks://bucket/d/d1/f1")).build();

    ObjectMetadata.Builder file2Builder = fileBuilder();
    ObjectMetadata file2 = file2Builder.key(new Path( "ks://bucket/d/d1/d2/f2")).build();

    /*
     * Act
     */
    storeObjectsInOrder(rootDirectory, directory1, file1, file2);

    /*
     * Assert
     */
    Iterable<ObjectMetadata> scanListing = flatScan(rootDirectory.getKey(), 1);
    assertEquals(Sets.newHashSet(directory1, file1, file2), Sets.newHashSet(scanListing));
    Iterable<ObjectMetadata> recursiveListing = handlesToMetadata(metadataStore.recursiveList(rootDirectory.getKey()));
    assertEquals(Sets.newHashSet(directory1, file1), Sets.newHashSet(recursiveListing));
  }

  @Test
  public void testRenameReturnsFalseWhenRenamingOverAnExistingFile() {
    ObjectMetadata root = rootObject(BUCKET);
    Path key1 = new Path("ks://bucket/f1");
    ObjectMetadata file1 = fileBuilder()
        .key(key1)
        .build();
    Path key2 = new Path("ks://bucket/f2");
    ObjectMetadata file2 = fileBuilder()
        .key(key2)
        .build();
    Path key3 = new Path("ks://bucket/f3");
    storeObjectsInOrder(file1, file2);

    assertTrue(metadataStore.renameObject(metadataStore.getObject(key1).get(), key3));
    assertFalse(metadataStore.renameObject(metadataStore.getObject(key2).get(), key3));
  }

  private ObjectMetadata.Builder directoryBuilder() {
    return ObjectMetadata.builder()
        .isDirectory(true)
        .size(0L)
        .creationTime(randomInt());
  }

  private ObjectMetadata.Builder fileBuilder() {
    return ObjectMetadata.builder()
        .isDirectory(false)
        .creationTime(randomInt())
        .size(random.nextInt())
        .physicalPath(UUID.randomUUID().toString())
        .physicalDataCommitted(true);
  }

  private void storeObjectsInOrder(ObjectMetadata... objectMetadatas) {
    Arrays.stream(objectMetadatas)
        .forEach(o -> metadataStore.createObject(o));
  }

  private int randomInt() {
    int rnd = random.nextInt(500);
    if (rnd == 0) {
      return 1;
    }
    return rnd;
  }

  private ObjectMetadata rootObject(String bucket) {
    return ObjectMetadata.builder()
        .key(new Path("ks://" + bucket + "/"))
        .isDirectory(true)
        .size(0)
        .creationTime(0)
        .build();
  }

  private Iterable<ObjectMetadata> flatScan(Path key, int partitionCount) {
    return FluentIterable.from(() -> IntStream.range(0, partitionCount).iterator())
        .transformAndConcat(it -> metadataStore.scanPartition(key, it, partitionCount))
        .transform(ObjectHandle::metadata);
  }

  /**
   * Validates the state of the metadata rooted at a particular object
   * The {@link ObjectMetadataWrapper} gives the full state that is expected.
   * @param wrapper
   */
  private void assertMetadataStoreState(ObjectMetadataWrapper wrapper) {
    ObjectHandle wrapperHandle = metadataStore.getObject(wrapper.metadata.getKey()).orElseThrow(AssertionFailedError::new);
    assertEquals(wrapper.metadata, wrapperHandle.metadata());
    if (!wrapper.metadata.isDirectory()) {
      return;
    }

    Set<ObjectMetadata> children = Streams.stream(metadataStore.listChildObjects(wrapperHandle))
        .map(ObjectHandle::metadata)
        .collect(Collectors.toSet());
    Set<ObjectMetadata> expectedChildren = wrapper.children.stream()
        .map(it -> it.metadata)
        .collect(Collectors.toSet());
    assertEquals(expectedChildren, children);
    wrapper.children.forEach(this::assertMetadataStoreState);
  }

  private void assertAllObjectsCanBeRetrievedByKey(Iterable<ObjectMetadata> objects) {
    for (ObjectMetadata objectMetadata : objects) {
      assertEquals(objectMetadata,
                   metadataStore.getObject(objectMetadata.getKey()).orElseThrow(AssertionFailedError::new).metadata());
    }
  }

  private void assertAllObjectsCanBeRetrievedByKey(ObjectMetadata... objects) {
    assertAllObjectsCanBeRetrievedByKey(Arrays.asList(objects));
  }

  private void assertAllObjectsAreNotRetrievable(Iterable<ObjectMetadata> objects) {
    for (ObjectMetadata objectMetadata : objects) {
      assertFalse(metadataStore.getObject(objectMetadata.getKey()).isPresent());
    }
  }

  private void assertAllObjectsAreNotRetrievable(ObjectMetadata... objects) {
    assertAllObjectsAreNotRetrievable(Arrays.asList(objects));
  }

  /**
   * Helper object used to validate subtree equality starting at given object.
   */
  private static class ObjectMetadataWrapper {
    ObjectMetadata metadata;
    List<ObjectMetadataWrapper> children = new ArrayList<>();

    ObjectMetadataWrapper addChildren(ObjectMetadata... objects) {
      Arrays.stream(objects).forEach(it -> children.add(wrap(it)));
      return this;
    }

    ObjectMetadataWrapper addChildren(ObjectMetadataWrapper... objects) {
      Collections.addAll(children, objects);
      return this;
    }
  }

  private static ObjectMetadataWrapper wrap(ObjectMetadata metadata) {
    ObjectMetadataWrapper wrapper = new ObjectMetadataWrapper();
    wrapper.metadata = metadata;
    return wrapper;
  }

  private static <T> Set<T> set(T... elements) {
    return Sets.newHashSet(elements);
  }

  private static Iterable<ObjectMetadata> handlesToMetadata(Iterable<? extends ObjectHandle> handles) {
    return FluentIterable.from(handles)
        .transform(ObjectHandle::metadata);
  }

  private static <T,U> AbstractMap.SimpleImmutableEntry<T, U> pair(T first, U second) {
    return new AbstractMap.SimpleImmutableEntry<T, U>(first, second);
  }
}
