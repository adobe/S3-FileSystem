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

package com.adobe.s3fs.metastore.internal.dynamodb;

import com.adobe.s3fs.common.ImplementationResolver;
import com.adobe.s3fs.common.context.FileSystemContext;
import com.adobe.s3fs.metastore.api.MetadataOperationLog;
import com.adobe.s3fs.metastore.api.MetadataStore;
import com.adobe.s3fs.metastore.api.MetadataStoreFactory;
import com.adobe.s3fs.metastore.internal.dynamodb.configuration.DynamoMetaStoreConfiguration;
import com.adobe.s3fs.metastore.internal.dynamodb.hashing.DefaultHashFunction;
import com.adobe.s3fs.metastore.internal.dynamodb.operations.AsynchronousMetadataTreeOperations;
import com.adobe.s3fs.metastore.internal.dynamodb.operations.MetadataOperations;
import com.adobe.s3fs.metastore.internal.dynamodb.operations.MetadataTreeOperations;
import com.adobe.s3fs.metastore.internal.dynamodb.operations.SynchronousMetadataTreeOperations;
import com.adobe.s3fs.metastore.internal.dynamodb.storage.AmazonDynamoDbStorageFactory;
import com.adobe.s3fs.metastore.internal.dynamodb.storage.DynamoDBStorageConfiguration;
import com.adobe.s3fs.metastore.internal.dynamodb.storage.HashingAwareObjectStorageFactory;
import com.adobe.s3fs.metastore.internal.dynamodb.storage.ObjectMetadataStorage;
import com.adobe.s3fs.metrics.data.ObjectLevelMetricsSource;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.util.function.ToIntFunction;

public class DynamoDBMetadataStoreFactory implements MetadataStoreFactory, Configurable {

  public static final String HASH_FUNCTION_CLASS = "fs.s3k.metastore.dynamo.hash.function.class";

  private Configuration hadoopConfiguration;

  @Override
  public MetadataStore create(FileSystemContext context, MetadataOperationLog operationLog) {
    ImplementationResolver implementationResolver = new ImplementationResolver(hadoopConfiguration);

    DynamoMetaStoreConfiguration metaStoreConfiguration = new DynamoMetaStoreConfiguration(context.configuration());

    DynamoDBStorageConfiguration storageConfiguration = new DynamoDBStorageConfiguration(context.configuration());

    ToIntFunction<Path> hashFunction = implementationResolver.resolve(HASH_FUNCTION_CLASS, DefaultHashFunction.class);

    ObjectMetadataStorage objectMetadataStorage =
                new HashingAwareObjectStorageFactory(hashFunction,
                                                     metaStoreConfiguration,
                                                     new AmazonDynamoDbStorageFactory(storageConfiguration)).create(context);

    MetadataOperations metadataOperations =
        new MetadataOperations(
            objectMetadataStorage, operationLog, ObjectLevelMetricsSource.getInstance());
    MetadataTreeOperations metadataTreeOperations = createMetadataTreeOperations(metaStoreConfiguration, metadataOperations);
    return new DynamoDBMetadataStore(metadataOperations, metadataTreeOperations);
  }

  private MetadataTreeOperations createMetadataTreeOperations(DynamoMetaStoreConfiguration metaStoreConfiguration,
                                                              MetadataOperations metadataOperations) {
    return metaStoreConfiguration.useAsynchronousOperationsForBucket()
           ? new AsynchronousMetadataTreeOperations(metadataOperations)
           : new SynchronousMetadataTreeOperations(metadataOperations);
  }

  @Override
  public void setConf(Configuration configuration) {
    hadoopConfiguration = configuration;
  }

  @Override
  public Configuration getConf() {
    return hadoopConfiguration;
  }
}

