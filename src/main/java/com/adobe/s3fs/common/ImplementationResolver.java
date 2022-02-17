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

package com.adobe.s3fs.common;

import com.google.common.base.Preconditions;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;

import java.util.Optional;

/**
 * Helper used to instantiate types bound dynamically in the Hadoop config.
 */
public class ImplementationResolver {

  private final Configuration configuration;

  public ImplementationResolver(Configuration configuration) {
    this.configuration = Preconditions.checkNotNull(configuration);
  }

  public <T> T resolve(String key, Class<? extends T> defaultImplementation) {
    return resolveToOptional(key, defaultImplementation)
        .orElseThrow(IllegalArgumentException::new);
  }

  private  <T> Optional<T> resolveToOptional(String key, Class<? extends T> defaultImplementation) {
    Class<T> type = (Class<T>) configuration.getClass(key, defaultImplementation);
    if (type == null) {
      return Optional.empty();
    }

    return Optional.of(ReflectionUtils.newInstance(type, configuration));
  }
}
