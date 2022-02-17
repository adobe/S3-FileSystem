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
import com.google.common.base.Strings;

import com.google.common.collect.Iterators;
import org.apache.hadoop.conf.Configuration;

import java.util.Iterator;
import java.util.Map;

public class HadoopKeyValueConfiguration implements KeyValueConfiguration {

  private final Configuration configuration;

  public HadoopKeyValueConfiguration(Configuration configuration) {
    this.configuration = Preconditions.checkNotNull(configuration);
  }

  /**
   * @return The int value associated with given key or the default if no value is associated.
   */
  @Override
  public int getInt(String key, int defaultValue) {
    return configuration.getInt(key, defaultValue);
  }

  /**
   * @return The int value associated with the given. An error is thrown if no value is associated.
   */
  @Override
  public int getInt(String key) {
    String val = configuration.get(key);
    if (Strings.isNullOrEmpty(val)) {
      throw new IllegalArgumentException();
    }
    return Integer.parseInt(val);
  }

  /**
   * @return The long value associated with given key or the default if no value is associated.
   */
  @Override
  public long getLong(String key, long defaultValue) {
    return configuration.getLong(key, defaultValue);
  }

  /**
   * @return The long value associated with the given. An error is thrown if no value is associated.
   */
  @Override
  public long getLong(String key) {
    String val = configuration.get(key);
    if (Strings.isNullOrEmpty(val)) {
      throw new IllegalArgumentException();
    }
    return Long.parseLong(val);
  }

  /**
   * @return The String value associated with given key or the default if no value is associated.
   */
  @Override
  public String getString(String key, String defaultValue) {
    return configuration.get(key, defaultValue);
  }

  /**
   * @return The String value associated with the given. An error is thrown if no value is associated.
   */
  @Override
  public String getString(String key) {
    String val = configuration.get(key);
    if (Strings.isNullOrEmpty(val)) {
      throw new IllegalArgumentException();
    }
    return val;
  }

  /**
   * @return The boolean associated with the given key, or the default value if no value is associated.
   */
  @Override
  public boolean getBoolean(String key, boolean defaultValue) {
    return configuration.getBoolean(key, defaultValue);
  }

  /**
   * @return The boolean associated with the given key. An error is thrown if no value is associated.
   */
  @Override
  public boolean getBoolean(String key) {
    String val = configuration.get(key);
    if (Strings.isNullOrEmpty(val)) {
      throw new IllegalArgumentException();
    }
    return Boolean.parseBoolean(val);
  }

  @Override
  public Iterator<Map.Entry<String, String>> iterator() {
    return Iterators.unmodifiableIterator(configuration.iterator());
  }
}
