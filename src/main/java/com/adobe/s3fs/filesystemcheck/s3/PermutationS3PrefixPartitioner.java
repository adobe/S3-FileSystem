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

package com.adobe.s3fs.filesystemcheck.s3;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Partitioner that generates all possible prefixes of a a certain length by permutating the elements from a given set.
 * For example in length is 2 and the set of prefixAtoms is ["0" .. "2"] then the result will be:
 * <ol>
 *   <li>"00"</li>
 *   <li>"01"</li>
 *   <li>"02"</li>
 *   <li>"10"</li>
 *   <li>"11"</li>
 *   <li>"12"</li>
 *   <li>"20"</li>
 *   <li>"21"</li>
 *   <li>"22"</li>
 * </ol>
 */
public class PermutationS3PrefixPartitioner implements S3Partitioner {

  private final List<String> prefixes;

  public PermutationS3PrefixPartitioner(int length, List<String> prefixAtoms) {
    this.prefixes = prefixPermutationsDelegate(length, prefixAtoms)
        .stream()
        .map(it -> String.join("", it))
        .collect(Collectors.toList());
  }

  @Override
  public List<String> prefixes() {
    return prefixes;
  }

  private static List<String[]> prefixPermutationsDelegate(int length, List<String> prefixAtoms) {
    List<String[]> result = new ArrayList<>();
    String[] partialPrefixHolder = new String[length];
    prefixPermutations(0, length, prefixAtoms, partialPrefixHolder, result);
    return result;
  }

  private static void prefixPermutations(int level,
                                         int maxLength,
                                         List<String> prefixAtoms,
                                         String[] partialPrefix,
                                         List<String[]> finalPrefixes) {
    if (level == maxLength) {
      finalPrefixes.add(Arrays.copyOf(partialPrefix, partialPrefix.length));
      return;
    }

    int nextLevel = level + 1;

    for (String prefixAtom : prefixAtoms) {
      partialPrefix[level] = prefixAtom;
      prefixPermutations(nextLevel, maxLength, prefixAtoms, partialPrefix, finalPrefixes);
    }
  }
}
