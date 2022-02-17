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

import com.google.common.base.Preconditions;
import com.google.common.collect.FluentIterable;

import org.apache.hadoop.fs.Path;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.ToIntFunction;

public class KeyOperations {

  private final ArrayList<String> suffixPool;
  private final ToIntFunction<Path> hashFunction;

  public static final String SUFFIX_DELIMITER = "-";

  public KeyOperations(ArrayList<String> suffixPool, ToIntFunction<Path> hashFunction) {
    Preconditions.checkArgument(
        suffixPool != null && !suffixPool.isEmpty() && suffixesAreValid(suffixPool));
    this.suffixPool = suffixPool;

    this.hashFunction = Preconditions.checkNotNull(hashFunction);
  }

  public String logicalKeyToHashKey(Path key) {
    if (key.isRoot()) {
      throw new IllegalArgumentException();
    }

    int hash = hashFunction.applyAsInt(key);

    String suffix = suffixPool.get(Math.abs(hash) % suffixPool.size());

    return key.getParent().toString() + SUFFIX_DELIMITER + suffix;
  }

  public String logicalKeyToSortKey(Path key) {
    if (key.isRoot()) {
      throw new IllegalArgumentException();
    }
    return key.getName();
  }

  public Optional<Path> hashAndSortKeyToLogicalKey(String hashKey, String sortKey) {
    int indexOfSuffixDelimiter = hashKey.lastIndexOf(SUFFIX_DELIMITER);
    if (indexOfSuffixDelimiter < 0) {
      return Optional.empty();
    }

    String parent = hashKey.substring(0, indexOfSuffixDelimiter);

    return Optional.of(new Path(parent, sortKey));
  }

  public Iterable<String> logicalKeyToAllPossibleHashKeys(Path key) {
    String finalFullLogicalPath = key.toString();
    return FluentIterable.from(suffixPool)
        .transform(it -> finalFullLogicalPath + SUFFIX_DELIMITER + it);
  }

  private static boolean suffixesAreValid(List<String> suffixPool) {
    return suffixPool.stream().noneMatch(it -> it.contains(SUFFIX_DELIMITER));
  }
}
