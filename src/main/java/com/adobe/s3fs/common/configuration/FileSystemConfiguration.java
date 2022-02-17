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

import com.adobe.s3fs.common.context.ContextProvider;

import com.google.common.base.Preconditions;

import java.util.Map;
import java.util.Optional;

/**
 * Provides configuration information for a specific file system instance
 */
public class FileSystemConfiguration {

  private final KeyValueConfiguration bucketAwareConfiguration;
  private final KeyValueConfiguration contextAwareKeyValueConfiguration;

  public FileSystemConfiguration(String bucket,
                                 KeyValueConfiguration keyValueConfiguration,
                                 ContextProvider contextProvider) {
    Preconditions.checkNotNull(keyValueConfiguration);
    this.bucketAwareConfiguration = new FilteringKeyValueConfiguration(keyValueConfiguration, bucket);
    this.contextAwareKeyValueConfiguration = contextProvider.getContextID()
        .<KeyValueConfiguration>map(it -> new FilteringKeyValueConfiguration(new FilteringKeyValueConfiguration(keyValueConfiguration, it), bucket))
        .orElse(bucketAwareConfiguration);
  }

  public KeyValueConfiguration contextAware() {
    return contextAwareKeyValueConfiguration;
  }

  public int getInt(String key, int defaultValue) {
    return bucketAwareConfiguration.getInt(key, defaultValue);
  }

  public int getInt(String key) {
    return bucketAwareConfiguration.getInt(key);
  }

  public long getLong(String key, long defaultValue) {
    return bucketAwareConfiguration.getLong(key, defaultValue);
  }

  public long getLong(String key) {
    return bucketAwareConfiguration.getLong(key);
  }

  public String getString(String key, String defaultValue) {
    return bucketAwareConfiguration.getString(key, defaultValue);
  }

  public String getString(String key) {
    return bucketAwareConfiguration.getString(key);
  }

  public boolean getBoolean(String key) {
    return bucketAwareConfiguration.getBoolean(key);
  }

  public boolean getBoolean(String key, boolean defaultValue) {
    return bucketAwareConfiguration.getBoolean(key, defaultValue);
  }

  public Iterable<Map.Entry<String, String>> properties() {
    return bucketAwareConfiguration;
  }
}
