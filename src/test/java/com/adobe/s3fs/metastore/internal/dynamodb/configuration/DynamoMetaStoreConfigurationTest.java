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

package com.adobe.s3fs.metastore.internal.dynamodb.configuration;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

import com.adobe.s3fs.common.configuration.FileSystemConfiguration;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class DynamoMetaStoreConfigurationTest {

  @Mock
  private FileSystemConfiguration mockConfig;

  private DynamoMetaStoreConfiguration configuration;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
    configuration = new DynamoMetaStoreConfiguration(mockConfig);
  }

  @Test
  public void testGetSuffixReturnCorrectNumberOfSuffixes() {
    when(mockConfig.getInt(DynamoMetaStoreConfiguration.SUFFIX_COUNT_PROP_NAME, 0)).thenReturn(10);

    assertEquals(10, configuration.getSuffixCount());
  }

  @Test(expected = IllegalStateException.class)
  public void testGetSuffixThrowsErrorIfSuffixPropertyNegative() {
    when(mockConfig.getInt(DynamoMetaStoreConfiguration.SUFFIX_COUNT_PROP_NAME,0 )).thenReturn(-1);

    int ignored = configuration.getSuffixCount();
  }

  @Test(expected = IllegalStateException.class)
  public void testGetSuffixThrowsErrorIfSuffixPropertyZero() {
    when(mockConfig.getInt(DynamoMetaStoreConfiguration.SUFFIX_COUNT_PROP_NAME, 0)).thenReturn(0);

    int ignored = configuration.getSuffixCount();
  }

  @Test
  public void testGetTableReturnsTheCorrectTable() {
    when(mockConfig.getString("fs.s3k.metastore.dynamo.table")).thenReturn("table");

    assertEquals("table", configuration.getDynamoTableForBucket());
  }

  @Test(expected = IllegalStateException.class)
  public void testGetTableThrowsErrorIfTableIsNotSet() {
    when(mockConfig.getString("fs.s3k.metastore.dynamo.table")).thenReturn("");

    String ignored = configuration.getDynamoTableForBucket();
  }
}
