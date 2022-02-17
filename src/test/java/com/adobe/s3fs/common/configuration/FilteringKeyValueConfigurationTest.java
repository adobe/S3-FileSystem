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

package com.adobe.s3fs.common.configuration;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

import com.adobe.s3fs.common.configuration.FilteringKeyValueConfiguration;
import com.adobe.s3fs.common.configuration.KeyValueConfiguration;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class FilteringKeyValueConfigurationTest {

  @Mock
  private KeyValueConfiguration mockConfig;

  private FilteringKeyValueConfiguration configuration;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
    configuration = new FilteringKeyValueConfiguration(mockConfig, "ctx");
  }

  @Test
  public void testGetIntReturnCorrectValue() {
    when(mockConfig.getInt("prop.int.ctx")).thenReturn(10);

    assertEquals(10, configuration.getInt("prop.int"));
  }

  @Test
  public void testGetIntReturnsDefaultValue() {
    when(mockConfig.getInt("prop.int.ctx", 3)).thenReturn(3);

    assertEquals(3, configuration.getInt("prop.int", 3));
  }

  @Test
  public void testGetLongReturnCorrectValue() {
    when(mockConfig.getLong("prop.long.ctx")).thenReturn(10L);

    assertEquals(10, configuration.getLong("prop.long"));
  }

  @Test
  public void testGetLongReturnsDefaultValue() {
    when(mockConfig.getLong("prop.long.ctx", 3)).thenReturn(3L);

    assertEquals(3, configuration.getLong("prop.long", 3));
  }

  @Test
  public void testGetStringReturnCorrectValue() {
    when(mockConfig.getString("prop.string.ctx")).thenReturn("xxx");

    assertEquals("xxx", configuration.getString("prop.string"));
  }

  @Test
  public void testGetStringReturnsDefaultValue() {
    when(mockConfig.getString("prop.string.ctx", "xxx1")).thenReturn("xxx1");

    assertEquals("xxx1", configuration.getString("prop.string", "xxx1"));
  }

  @Test
  public void testGetBooleanReturnCorrectValue() {
    when(mockConfig.getBoolean("prop.bool.ctx")).thenReturn(true);

    assertEquals(true, configuration.getBoolean("prop.bool"));
  }

  @Test
  public void testGetBooleanReturnsDefaultValue() {
    when(mockConfig.getBoolean("prop.bool.ctx", true)).thenReturn(true);

    assertEquals(true, configuration.getBoolean("prop.bool", true));
  }
}
