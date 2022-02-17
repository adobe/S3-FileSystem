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

package com.adobe.s3fs.metastore.internal.dynamodb.hashing;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

public class KeyOperationsTest {

  private static final ArrayList<String> SUFFIXES = Lists.newArrayList("s1", "s2", "s3");

  private KeyOperations keyOperations;

  @Before
  public void setup() {
    keyOperations = new KeyOperations(SUFFIXES, new DefaultHashFunction());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testConstructorThrowsErrorOnEmptySuffixes() {
    KeyOperations keyOperations = new KeyOperations(new ArrayList<>(), new DefaultHashFunction());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testConstructorThrowsErrorOnInvalidSuffixes() {
    KeyOperations keyOperations =
        new KeyOperations(Lists.newArrayList("-s1", "s2"), new DefaultHashFunction());
  }

  @Test
  public void testLogicalKeyToHashKey() {
    Path key = new Path("ks://bucket/ab/bf");

    String hashKey = keyOperations.logicalKeyToHashKey(key);

    assertTrue(SUFFIXES.stream().anyMatch(hashKey::endsWith));
  }

  @Test
  public void testLogicalKeyToSortKey() {
    Path key = new Path("ks://bucket/a/b/f");

    String sortKey = keyOperations.logicalKeyToSortKey(key);

    assertEquals(key.getName(), sortKey);
  }

  @Test
  public void testHashAndSortKeyToLogicalKeySuffixNotDelimited() {
    // parent not suffix delimited
    assertFalse(keyOperations.hashAndSortKeyToLogicalKey("ks://bucket/a/b", "c").isPresent());
  }

  @Test
  public void testHashAndSortKeyToLogicalKeyValidObjectInDirectory() {
    // valid
    // file in directory
    Optional<Path> resolvedKey = keyOperations.hashAndSortKeyToLogicalKey("ks://bucket/a-s1", "f");

    Path key = new Path("ks://bucket/a/f");
    assertTrue(resolvedKey.isPresent());
    assertEquals(key, resolvedKey.get());
  }

  @Test
  public void testHashAndSortKeyToLogicalKeyValidObjectInRoot() {
    // valid
    // file in directory
    Optional<Path> resolvedKey = keyOperations.hashAndSortKeyToLogicalKey("ks://bucket/-s1", "f");

    Path key = new Path("ks://bucket/f");
    assertTrue(resolvedKey.isPresent());
    assertEquals(key, resolvedKey.get());
  }

  @Test
  public void testLogicalKeyToAllPossibleHashKeys() {
    Path key = new Path("ks://bucket/a/b");

    List<String> allSuffixed =
        Lists.newArrayList(keyOperations.logicalKeyToAllPossibleHashKeys(key));

    assertEquals(SUFFIXES.size(), allSuffixed.size());
    for (String suffix : SUFFIXES) {
      assertTrue(allSuffixed.stream().anyMatch(it -> it.endsWith(suffix)));
    }
  }

  @Test
  public void testHashKeyIsPresentInAllPossibleHashKeysRootCase() {
    Path child = new Path("ks://bucket/a");
    Path parent = new Path("ks://bucket/");

    String hashKey = keyOperations.logicalKeyToHashKey(child);
    Set<String> allHashKeys =
        Sets.newHashSet(keyOperations.logicalKeyToAllPossibleHashKeys(parent));

    assertTrue(allHashKeys.contains(hashKey));
  }

  @Test
  public void testHashKeyIsPresentInAllPossibleHashKeysChildCase() {
    Path child = new Path("ks://bucket/a/b");
    Path parent = new Path("ks://bucket/a");

    String hashKey = keyOperations.logicalKeyToHashKey(child);
    Set<String> allHashKeys =
        Sets.newHashSet(keyOperations.logicalKeyToAllPossibleHashKeys(parent));

    assertTrue(allHashKeys.contains(hashKey));
  }
}
