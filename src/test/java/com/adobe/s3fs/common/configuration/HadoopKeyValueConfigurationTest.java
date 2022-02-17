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

import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Test;

public class HadoopKeyValueConfigurationTest {

  private Configuration mockConfig;

  private HadoopKeyValueConfiguration configuration;

  @Before
  public void setup() {
    mockConfig = new Configuration(false);
    configuration = new HadoopKeyValueConfiguration(mockConfig);
  }

  @Test
  public void testGetIntReturnCorrectValue() {
    mockConfig.setInt("prop.int", 10);

    assertEquals(10, configuration.getInt("prop.int"));
  }

  @Test
  public void testGetIntReturnsDefaultValue() {
    assertEquals(3, configuration.getInt("prop.int", 3));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetIntThrowsErrorIfNoPropIsSet() {
    int ignored = configuration.getInt("prop.int");
  }

  @Test
  public void testGetLongReturnCorrectValue() {
    mockConfig.setLong("prop.long", 10);

    assertEquals(10, configuration.getInt("prop.long"));
  }

  @Test
  public void testGetLongReturnsDefaultValue() {
    assertEquals(3, configuration.getLong("prop.long", 3));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetLongThrowsErrorIfNoPropIsSet() {
    long ignored = configuration.getInt("prop.long");
  }

  @Test
  public void testGetStringReturnCorrectValue() {
    mockConfig.set("prop.string", "xxx");

    assertEquals("xxx", configuration.getString("prop.string"));
  }

  @Test
  public void testGetStringReturnsDefaultValue() {
    assertEquals("xxx1", configuration.getString("prop.string", "xxx1"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetStringThrowsErrorIfNoPropIsSet() {
    String ignored = configuration.getString("prop.string");
  }

  @Test
  public void testGetBooleanReturnCorrectValue() {
    mockConfig.setBoolean("prop.bool", true);

    assertEquals(true, configuration.getBoolean("prop.bool"));
  }

  @Test
  public void testGetBooleanReturnsDefaultValue() {
    assertEquals(true, configuration.getBoolean("prop.bool", true));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetBooleanThrowsErrorIfNoPropIsSet() {
    String ignored = configuration.getString("prop.bool");
  }
}
