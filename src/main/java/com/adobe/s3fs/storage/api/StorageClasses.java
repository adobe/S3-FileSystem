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

package com.adobe.s3fs.storage.api;

import com.google.common.base.Preconditions;

public enum StorageClasses {
  PATH_TRANSLATOR_FACTORY("fs.s3k.storage.path.translator.factory"),
  PHYSICAL_STORAGE_FACTORY("fs.s3k.storage.factory.class");

  private final String key;

  StorageClasses(String key) {
    this.key = Preconditions.checkNotNull(key);
  }

  public String getKey() {
    return key;
  }

  @Override
  public String toString() {
    return key;
  }
}
