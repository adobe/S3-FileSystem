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

package com.adobe.s3fs.metastore.internal.dynamodb.storage;

import com.adobe.s3fs.common.context.FileSystemContext;
import com.adobe.s3fs.metastore.internal.dynamodb.configuration.DynamoMetaStoreConfiguration;
import com.adobe.s3fs.metastore.internal.dynamodb.hashing.KeyOperations;

import com.google.common.base.Preconditions;

import org.apache.hadoop.fs.Path;

import java.util.ArrayList;
import java.util.function.ToIntFunction;

public class HashingAwareObjectStorageFactory {

  private final ToIntFunction<Path> hashFunction;
  private final DynamoMetaStoreConfiguration metadataStoreConfiguration;
  private final DynamoDBStorageFactory dynamoDBStorageFactory;

  public HashingAwareObjectStorageFactory(ToIntFunction<Path> hashFunction,
                                          DynamoMetaStoreConfiguration metadataStoreConfiguration,
                                          DynamoDBStorageFactory dynamoDBStorageFactory) {
    this.hashFunction = Preconditions.checkNotNull(hashFunction);
    this.metadataStoreConfiguration = Preconditions.checkNotNull(metadataStoreConfiguration);
    this.dynamoDBStorageFactory = Preconditions.checkNotNull(dynamoDBStorageFactory);
  }

  public ObjectMetadataStorage create(FileSystemContext context) {
    String table = metadataStoreConfiguration.getDynamoTableForBucket();
    int suffixCount = metadataStoreConfiguration.getSuffixCount();

    KeyOperations keyOperations = new KeyOperations(makeSuffixes(suffixCount), hashFunction);

    return new HashingAwareObjectStorage(keyOperations,
                                         dynamoDBStorageFactory.create(table, context));
  }

  private ArrayList<String> makeSuffixes(int suffixCount) {
    ArrayList<String> suffixPool = new ArrayList<>(suffixCount);
    for (int i = 0; i < suffixCount; i++) {
      suffixPool.add("sf" + i);
    }
    return suffixPool;
  }
}
