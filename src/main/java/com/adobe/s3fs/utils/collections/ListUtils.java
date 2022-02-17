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

package com.adobe.s3fs.utils.collections;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;

import java.util.*;
import java.util.stream.IntStream;

public final class ListUtils {

  private ListUtils() {}

  /**
   * Randomly divides the input list into at most partitionCount sublists. Note that order of elements is not preserved.
   * @param input
   * @param partitionCount
   * @param <T>
   * @return
   */
  public static <T> List<List<T>> randomPartition(List<T> input, int partitionCount) {
    if (input.size() <= partitionCount) {
      return Lists.partition(input, 1);
    }
    List<List<T>> partitioned = new ArrayList<>(partitionCount);
    IntStream.range(0, partitionCount).forEach(i -> partitioned.add(new ArrayList<>()));

    Iterator<List<T>> nextPartitionProvider = Iterators.cycle(partitioned);
    for (T elem : input) {
      nextPartitionProvider.next().add(elem);
    }

    return partitioned;
  }
}
