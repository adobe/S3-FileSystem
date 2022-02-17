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

import com.google.common.collect.Iterables;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collection;

public class CartesianS3PrefixPartitionerTest {

  private CartesianS3PrefixPartitioner s3Prefixes;

  @Before
  public void setup() {
    s3Prefixes =
        new CartesianS3PrefixPartitioner(new SingleDigitS3PrefixPartitioner());
  }

  @Test
  public void testPrefixes() {
    // Act
    Collection<String> prefixes = s3Prefixes.prefixes();
    // Verify
    Assert.assertEquals("Expected first prefix to be 00", "00", Iterables.get(prefixes, 0));
    Assert.assertEquals("Expected last prefix to be ff", "ff", Iterables.get(prefixes, 255));
  }
}
