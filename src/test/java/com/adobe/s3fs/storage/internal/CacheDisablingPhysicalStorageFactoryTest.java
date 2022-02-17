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

package com.adobe.s3fs.storage.internal;

import static org.junit.Assert.*;
import static org.mockito.Mockito.when;

import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.HashMap;
import java.util.Map;

public class CacheDisablingPhysicalStorageFactoryTest {

  private Configuration hadoopConfiguration;

  @Mock
  private FileSystemPhysicalStorageConfiguration mockPhysicalStorageConfiguration;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
    hadoopConfiguration = new Configuration(false);
    hadoopConfiguration.setBoolean("fs.s3a.impl.disable.cache", false);
    when(mockPhysicalStorageConfiguration.getUnderlyingFileSystemScheme()).thenReturn("s3a");
  }

  @Test
  public void testHadoopConfigurationIsCloned() {
    CacheDisablingPhysicalStorageFactory factory = new CacheDisablingPhysicalStorageFactory(mockPhysicalStorageConfiguration,
                                                                                            hadoopConfiguration);

    assertTrue(hadoopConfiguration != factory.getHadoopConfiguration());
    assertNotEquals(hadoopConfiguration, factory.getHadoopConfiguration());
  }


  @Test
  public void testUnderlyingFileSystemSchemeHashCachingDisabled() {
    CacheDisablingPhysicalStorageFactory factory = new CacheDisablingPhysicalStorageFactory(mockPhysicalStorageConfiguration,
                                                                                            hadoopConfiguration);

    assertFalse(hadoopConfiguration.getBoolean("fs.s3a.impl.disable.cache", false));
    assertTrue(factory.getHadoopConfiguration().getBoolean("fs.s3a.impl.disable.cache", false));
  }

  @Test
  public void testCustomUnderlyingFsPropertiesAreExtracted() {
    Map<String, String> customProps = new HashMap<>();
    customProps.put("custom.prop", "val1");
    when(mockPhysicalStorageConfiguration.getUnderlyingFileSystemProperties()).thenReturn(customProps.entrySet());
    CacheDisablingPhysicalStorageFactory factory = new CacheDisablingPhysicalStorageFactory(mockPhysicalStorageConfiguration, hadoopConfiguration);

    assertEquals("val1", factory.getHadoopConfiguration().get("custom.prop"));
  }
}
