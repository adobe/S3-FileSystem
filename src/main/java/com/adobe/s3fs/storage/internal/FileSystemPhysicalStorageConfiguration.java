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

import com.adobe.s3fs.common.configuration.FileSystemConfiguration;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.FluentIterable;

import java.util.AbstractMap;
import java.util.Map;

public class FileSystemPhysicalStorageConfiguration {

  public static final String UNDERLYING_FILE_SYSTEM_PROP = "fs.s3k.storage.underlying.filesystem.scheme";
  public static final String EVENTUAL_CONSISTENCY_MAX_RETRIES = "fs.s3k.storage.eventual.consistency.max.retries";
  public static final String EVENTUAL_CONSISTENCY_DELAY_BETWEEN_RETRIES = "fs.s3k.storage.eventual.consistency.delay.retries.millis";
  public static final String UNDERLYING_FS_PROP_MARKER = "fs.s3k.storage.underlying.fs.prop.marker.";

  private final FileSystemConfiguration configuration;

  public FileSystemPhysicalStorageConfiguration(FileSystemConfiguration configuration) {
    this.configuration = Preconditions.checkNotNull(configuration);
  }

  public String getUnderlyingFileSystemScheme() {
    String val = configuration.getString(UNDERLYING_FILE_SYSTEM_PROP);
    if (Strings.isNullOrEmpty(val)) {
      throw new IllegalStateException();
    }
    return val;
  }

  public int getEventualConsistencyDelayBetweenRetriesMillis() {
    return configuration.getInt(EVENTUAL_CONSISTENCY_DELAY_BETWEEN_RETRIES, 5);
  }

  public int getEventualConsistencyMaxRetries() {
    return configuration.getInt(EVENTUAL_CONSISTENCY_MAX_RETRIES, 10);
  }

  public Iterable<Map.Entry<String, String>> getUnderlyingFileSystemProperties() {
    return FluentIterable.from(configuration.properties())
        .filter(it -> it.getKey().startsWith(UNDERLYING_FS_PROP_MARKER))
        .transform(it -> new AbstractMap.SimpleEntry<>(it.getKey().replaceFirst(UNDERLYING_FS_PROP_MARKER, ""), it.getValue()));
  }
}
