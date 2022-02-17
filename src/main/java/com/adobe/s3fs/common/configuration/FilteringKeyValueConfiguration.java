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

import com.google.common.base.Preconditions;
import com.google.common.collect.FluentIterable;

import java.util.AbstractMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Implementation of {@link KeyValueConfiguration} that filters based on specific suffix.
 */
public class FilteringKeyValueConfiguration implements KeyValueConfiguration {

  private final KeyValueConfiguration keyValueConfiguration;
  private final String filter;

  public FilteringKeyValueConfiguration(KeyValueConfiguration keyValueConfiguration, String filter) {
    this.keyValueConfiguration = Preconditions.checkNotNull(keyValueConfiguration);
    this.filter = Preconditions.checkNotNull(filter);
  }

  @Override
  public int getInt(String key, int defaultValue) {
    return keyValueConfiguration.getInt(appendFilter(key), defaultValue);
  }

  @Override
  public int getInt(String key) {
    return keyValueConfiguration.getInt(appendFilter(key));
  }

  @Override
  public long getLong(String key, long defaultValue) {
    return keyValueConfiguration.getLong(appendFilter(key), defaultValue);
  }

  @Override
  public long getLong(String key) {
    return keyValueConfiguration.getLong(appendFilter(key));
  }

  @Override
  public String getString(String key, String defaultValue) {
    return keyValueConfiguration.getString(appendFilter(key), defaultValue);
  }

  @Override
  public String getString(String key) {
    return keyValueConfiguration.getString(appendFilter(key));
  }

  @Override
  public boolean getBoolean(String key) {
    return keyValueConfiguration.getBoolean(appendFilter(key));
  }

  @Override
  public boolean getBoolean(String key, boolean defaultValue) {
    return keyValueConfiguration.getBoolean(appendFilter(key), defaultValue);
  }

  @Override
  public Iterator<Map.Entry<String, String>> iterator() {
    Iterable<Map.Entry<String, String>> iterable = FluentIterable.from(keyValueConfiguration)
        .filter(it -> it.getKey().endsWith(filter))
        .transform(it -> new AbstractMap.SimpleEntry<>(trimSuffix(it.getKey(),"." + filter), it.getValue()));
    return iterable.iterator();
  }

  private String appendFilter(String propName) {
    return propName + "." + filter;
  }

  private static String trimSuffix(String str, String suffix) {
    int idx = str.lastIndexOf(suffix);
    return str.substring(0, idx);
  }
}
