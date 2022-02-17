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

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

import com.google.common.collect.Lists;
import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;

import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.List;

@RunWith(DataProviderRunner.class)
public class RoundRobinIterableTest {

  @DataProvider
  public static Object[][] dataSet() {
    return new Object[][] {
        { asList(), asList() },
        { asList(asList()), asList()},
        { asList(asList(1)), asList(1) },
        { asList(asList(1, 2), asList(4)), asList(1, 4, 2)},
        { asList(asList(1, 2), asList(3, 4), asList(5, 6)), asList(1, 3, 5, 2, 4, 6)},
        { asList(asList(), asList(1, 2)), asList(1, 2)},
        { asList(asList(1, 2), asList(3), asList(4, 5, 6)), asList(1, 3, 4, 2, 5, 6)}
    };
  }

  @Test
  @UseDataProvider("dataSet")
  public void testRoundRobinBasedOnDataSet(List<List<Integer>> source, List<Integer> expected) {
    // prepare
    RoundRobinIterable<Integer> roundRobinIterable = new RoundRobinIterable<>(source);

    // act
    List<Integer> iterated = Lists.newArrayList(roundRobinIterable);

    // assert
    assertEquals(expected, iterated);
  }
}
