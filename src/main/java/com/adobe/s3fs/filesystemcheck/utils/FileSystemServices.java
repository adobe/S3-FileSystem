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

package com.adobe.s3fs.filesystemcheck.utils;

import com.adobe.s3fs.common.ImplementationResolver;
import com.adobe.s3fs.common.configuration.FileSystemConfiguration;
import com.adobe.s3fs.common.configuration.HadoopKeyValueConfigurationFactory;
import com.adobe.s3fs.common.configuration.KeyValueConfigurationFactory;
import com.adobe.s3fs.common.context.ContextKeys;
import com.adobe.s3fs.common.context.ContextProvider;
import com.adobe.s3fs.common.context.DefaultContextProvider;
import com.adobe.s3fs.common.context.FileSystemContext;
import com.adobe.s3fs.common.runtime.FileSystemRuntime;
import com.adobe.s3fs.common.runtime.FileSystemRuntimeFactory;
import com.adobe.s3fs.metastore.api.*;
import com.adobe.s3fs.metastore.internal.dynamodb.DynamoDBMetadataStoreFactory;
import com.adobe.s3fs.operationlog.S3MetadataOperationLogFactory;
import com.adobe.s3fs.storage.internal.FileSystemPhysicalStorageConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

public final class FileSystemServices {

  private FileSystemServices() {
    throw new IllegalStateException("Non-instantiable class");
  }
  /**
   * Returns MetadataStoreExtended exposing extended operations
   * @param configuration Client's Hadoop Configuration
   * @param bucket S3 bucket
   * @return a MetadataStoreExtended object
   */
  public static MetadataStoreExtended createMetadataStore(
      Configuration configuration, String bucket) {
    ImplementationResolver implementationResolver = new ImplementationResolver(configuration);

    MetadataOperationLogFactory operationLogFactory =
        implementationResolver.resolve(
            MetastoreClasses.METADATA_OPERATION_LOG_FACTORY_CLASS.getKey(),
            S3MetadataOperationLogFactory.class);

    MetadataStoreFactory metadataStoreFactory =
        implementationResolver.resolve(
            MetastoreClasses.METADATA_STORE_FACTORY_CLASS.getKey(),
            DynamoDBMetadataStoreFactory.class);

    FileSystemContext fileSystemContext = createFileSystemContext(configuration, bucket);

    return (MetadataStoreExtended)
        metadataStoreFactory.create(
            fileSystemContext, operationLogFactory.create(fileSystemContext));
  }

  public static MetadataStoreExtended createMetadataStore(Configuration configuration, Path path) {
    return createMetadataStore(configuration, path.toUri().getHost());
  }

  public static MetadataOperationLogExtended createOperationLogExtended(Configuration configuration, String bucket) {
    ImplementationResolver implementationResolver = new ImplementationResolver(configuration);

    MetadataOperationLogFactory operationLogFactory =
        implementationResolver.resolve(
            MetastoreClasses.METADATA_OPERATION_LOG_EXTENDED_FACTORY_CLASS.getKey(),
            S3MetadataOperationLogFactory.class);

    FileSystemContext fileSystemContext = createFileSystemContext(configuration, bucket);

    return (MetadataOperationLogExtended)operationLogFactory.create(fileSystemContext);
  }

  /**
   * @param configuration Hadoop Configuration
   * @param bucket S3 Bucket
   * @return FileSystemPhysicalStorageConfiguration object holding various file system configs
   */
  public static FileSystemPhysicalStorageConfiguration createPhysicalStorageConfiguration(
      Configuration configuration, String bucket) {
    FileSystemConfiguration fileSystemConfiguration =
        createFileSystemConfiguration(configuration, bucket);
    return new FileSystemPhysicalStorageConfiguration(fileSystemConfiguration);
  }

  /**
   * @param configuration Hadoop Configuration
   * @param bucket S3 Bucket
   * @return FileSystemConfiguration configuration object for a specific file system instance
   */
  private static FileSystemConfiguration createFileSystemConfiguration(
      Configuration configuration, String bucket) {
    ImplementationResolver implementationResolver = new ImplementationResolver(configuration);

    KeyValueConfigurationFactory keyValueConfigurationFactory =
        implementationResolver.resolve(
            "fs.s3k.keyvalue.config.factory", HadoopKeyValueConfigurationFactory.class);

    ContextProvider contextProvider =
        implementationResolver.resolve(
            ContextKeys.CONTEXT_PROVIDER_IMPL_CLASS.getKey(), DefaultContextProvider.class);

    return new FileSystemConfiguration(
        bucket, keyValueConfigurationFactory.create(), contextProvider);
  }

  /**
   * @param configuration Hadoop Configuration
   * @param bucket S3 Bucket
   * @return FileSystemContext Information object specific for a file system instance
   */
  private static FileSystemContext createFileSystemContext(
      Configuration configuration, String bucket) {
    FileSystemConfiguration fileSystemConfiguration =
        createFileSystemConfiguration(configuration, bucket);
    FileSystemRuntime runtime = FileSystemRuntimeFactory.create(fileSystemConfiguration);
    return FileSystemContext.builder()
        .bucket(bucket)
        .configuration(fileSystemConfiguration)
        .runtime(runtime)
        .build();
  }
}
