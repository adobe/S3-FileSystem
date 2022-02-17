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

import static org.junit.Assert.assertEquals;

import com.adobe.s3fs.common.configuration.FileSystemConfiguration;
import com.adobe.s3fs.common.configuration.HadoopKeyValueConfiguration;
import com.adobe.s3fs.common.configuration.KeyValueConfiguration;
import com.adobe.s3fs.common.context.ContextProvider;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public class FileSystemPhysicalStorageConfigurationTest {

  private Configuration hadoopConfiguration;
  private FileSystemConfiguration fileSystemConfiguration;

  private FileSystemPhysicalStorageConfiguration physicalStorageConfiguration;

  @Before
  public void setup() {
    hadoopConfiguration = new Configuration(false);
    KeyValueConfiguration keyValueConfiguration = new HadoopKeyValueConfiguration(hadoopConfiguration);
    ContextProvider contextProvider = () -> Optional.empty();
    fileSystemConfiguration = new FileSystemConfiguration("bucket", keyValueConfiguration, contextProvider);
    physicalStorageConfiguration = new FileSystemPhysicalStorageConfiguration(fileSystemConfiguration);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNonSetUnderlyingFileSystemSchemeThrows() {
    String scheme = physicalStorageConfiguration.getUnderlyingFileSystemScheme();
  }

  @Test
  public void testUnderlyingFileSystemIsReadCorrectly() {
    hadoopConfiguration.set(FileSystemPhysicalStorageConfiguration.UNDERLYING_FILE_SYSTEM_PROP + ".bucket", "bla");

    assertEquals("bla", physicalStorageConfiguration.getUnderlyingFileSystemScheme());
  }

  @Test
  public void testMaxRetriesIsReadCorrectly() {
    hadoopConfiguration.setInt(FileSystemPhysicalStorageConfiguration.EVENTUAL_CONSISTENCY_MAX_RETRIES + ".bucket", 10);

    assertEquals(10, physicalStorageConfiguration.getEventualConsistencyMaxRetries());
  }

  @Test
  public void testDelayBetweenRetriesIsReadCorrectly() {
    hadoopConfiguration.setInt(FileSystemPhysicalStorageConfiguration.EVENTUAL_CONSISTENCY_DELAY_BETWEEN_RETRIES + ".bucket", 500);

    assertEquals(500, physicalStorageConfiguration.getEventualConsistencyDelayBetweenRetriesMillis());
  }

  @Test
  public void testUnderlyingFsPropertiesAreExtracted() {
    hadoopConfiguration.set(FileSystemPhysicalStorageConfiguration.UNDERLYING_FS_PROP_MARKER + "custom.prop.bucket", "val1");

    List<Map.Entry<String, String>> props = Lists.newArrayList(physicalStorageConfiguration.getUnderlyingFileSystemProperties());
    assertEquals(1, props.size());
    assertEquals("custom.prop", props.get(0).getKey());
    assertEquals("val1", props.get(0).getValue());
  }
}
