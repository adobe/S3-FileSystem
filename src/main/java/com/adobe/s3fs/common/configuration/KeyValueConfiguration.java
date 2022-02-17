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

import java.util.Map;

/**
 * Generic K/V style configuration.
 */
public interface KeyValueConfiguration extends Iterable<Map.Entry<String, String>> {

  /**
   * @return The int value associated with given key or the default if no value is associated.
   */
  int getInt(String key, int defaultValue);

  /**
   * @return The int value associated with the given. An error is thrown if no value is associated.
   */
  int getInt(String key);

  /**
   * @return The long value associated with given key or the default if no value is associated.
   */
  long getLong(String key, long defaultValue);

  /**
   * @return The long value associated with the given. An error is thrown if no value is associated.
   */
  long getLong(String key);

  /**
   * @return The String value associated with given key or the default if no value is associated.
   */
  String getString(String key, String defaultValue);

  /**
   * @return The String value associated with the given. An error is thrown if no value is associated.
   */
  String getString(String key);

  /**
   * @return The boolean associated with the given key, or the default value if no value is associated.
   */
  boolean getBoolean(String key, boolean defaultValue);

  /**
   * @return The boolean associated with the given key. An error is thrown if no value is associated.
   */
  boolean getBoolean(String key);
}
