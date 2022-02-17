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

package com.adobe.s3fs.metastore.internal.dynamodb.configuration;

import com.adobe.s3fs.common.configuration.FileSystemConfiguration;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

public class DynamoMetaStoreConfiguration {

  public static final String SUFFIX_COUNT_PROP_NAME = "fs.s3k.metastore.dynamo.suffix.count";

  private final FileSystemConfiguration configuration;

  public DynamoMetaStoreConfiguration(FileSystemConfiguration configuration) {
    this.configuration = Preconditions.checkNotNull(configuration);
  }

  public int getSuffixCount() {
    int suffixCount = configuration.getInt(SUFFIX_COUNT_PROP_NAME, 0);
    if (suffixCount <= 0) {
      throw new IllegalStateException();
    }
    return suffixCount;
  }

  public String getDynamoTableForBucket() {
    String tableName = configuration.getString("fs.s3k.metastore.dynamo.table");
    if (Strings.isNullOrEmpty(tableName)) {
      throw new IllegalStateException("No table set");
    }
    return tableName;
  }

  public boolean useAsynchronousOperationsForBucket() {
    String propName = "fs.s3k.metastore.operations.async";
    return configuration.contextAware().getBoolean(propName, true);
  }
}
