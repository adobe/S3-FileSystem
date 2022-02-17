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

package com.adobe.s3fs.filesystem;

import com.adobe.s3fs.common.ImplementationResolver;
import com.adobe.s3fs.common.configuration.FileSystemConfiguration;
import com.adobe.s3fs.common.configuration.HadoopKeyValueConfigurationFactory;
import com.adobe.s3fs.common.configuration.KeyValueConfiguration;
import com.adobe.s3fs.common.configuration.KeyValueConfigurationFactory;
import com.adobe.s3fs.common.context.ContextKeys;
import com.adobe.s3fs.common.context.ContextProvider;
import com.adobe.s3fs.common.context.DefaultContextProvider;
import com.adobe.s3fs.common.context.FileSystemContext;
import com.adobe.s3fs.common.runtime.FileSystemRuntimeFactory;
import com.adobe.s3fs.common.runtime.FileSystemRuntimeImpl;
import com.adobe.s3fs.metastore.api.MetadataOperationLogFactory;
import com.adobe.s3fs.metastore.api.MetadataStore;
import com.adobe.s3fs.metastore.api.MetadataStoreFactory;
import com.adobe.s3fs.metastore.api.MetastoreClasses;
import com.adobe.s3fs.metastore.internal.dynamodb.DynamoDBMetadataStoreFactory;
import com.adobe.s3fs.metrics.data.filesystem.FileSystemClientMetrics;
import com.adobe.s3fs.metrics.data.filesystem.FileSystemClientMetricsFactory;
import com.adobe.s3fs.operationlog.S3MetadataOperationLogFactory;
import com.adobe.s3fs.storage.api.*;
import com.adobe.s3fs.storage.internal.FileSystemPhysicalStorageFactory;
import com.adobe.s3fs.storage.internal.ToRandomPathTranslatorFactory;

import com.google.common.base.Throwables;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.EnumSet;

/**
 * Main implementation of the {@link FileSystem}.
 */
@SuppressWarnings("squid:S2095") // MetdataStore and PhysicalStorage are closed by FileSystemImplementation
public class HadoopFileSystemAdapter extends FileSystem {

  private URI uri;
  private QualifyingFileSystemAdapter fileSystemAdapter;
  private FileSystemRuntimeImpl runtime;

  private static final Logger LOG = LoggerFactory.getLogger(HadoopFileSystemAdapter.class);

  @Override
  public void initialize(URI name, Configuration conf) throws IOException {
    super.initialize(name, conf);

    try {
      this.uri = new URI(name.getScheme(),
                         name.getUserInfo(),
                         name.getHost(),
                         name.getPort(),
                         null,
                         null,
                         null);
    } catch (URISyntaxException e) {
      throw new IOException(e);
    }

    ImplementationResolver implementationResolver = new ImplementationResolver(conf);

    KeyValueConfigurationFactory keyValueConfigurationFactory = implementationResolver.resolve("fs.s3k.keyvalue.config.factory",
                                                                                               HadoopKeyValueConfigurationFactory.class);
    KeyValueConfiguration keyValueConfiguration = keyValueConfigurationFactory.create();

    ContextProvider contextProvider = implementationResolver.resolve(ContextKeys.CONTEXT_PROVIDER_IMPL_CLASS.getKey(),
                                                                     DefaultContextProvider.class);

    FileSystemConfiguration fileSystemConfiguration = new FileSystemConfiguration(name.getHost(), keyValueConfiguration, contextProvider);

    this.runtime = FileSystemRuntimeFactory.create(fileSystemConfiguration);

    FileSystemContext context = FileSystemContext.builder()
        .bucket(name.getHost())
        .configuration(fileSystemConfiguration)
        .runtime(runtime)
        .build();

    MetadataStoreFactory metadataStoreFactory = implementationResolver.resolve(MetastoreClasses.METADATA_STORE_FACTORY_CLASS.getKey(),
                                                                               DynamoDBMetadataStoreFactory.class);

    PhysicalStorageFactory physicalStorageFactory = implementationResolver.resolve(StorageClasses.PHYSICAL_STORAGE_FACTORY.getKey(),
                                                                                   FileSystemPhysicalStorageFactory.class);


    PathTranslatorFactory pathTranslatorFactory = implementationResolver.resolve(StorageClasses.PATH_TRANSLATOR_FACTORY.getKey(),
                                                                                 ToRandomPathTranslatorFactory.class);
    PathTranslator pathTranslator = pathTranslatorFactory.create(context);

    MetadataOperationLogFactory operationLogFactory = implementationResolver.resolve(MetastoreClasses.METADATA_OPERATION_LOG_FACTORY_CLASS.getKey(),
                                                                                     S3MetadataOperationLogFactory.class);

    MetadataStore metadataStore = metadataStoreFactory.create(context, operationLogFactory.create(context));

    PhysicalStorage physicalStorage = physicalStorageFactory.create(context);

    FileSystemClientMetrics fsClientMetrics =
        FileSystemClientMetricsFactory.builder().build().createFileSystemClientMetrics();

    FileSystemImplementation fileSystemImplementation =
        new FileSystemImplementation(
            metadataStore, physicalStorage, pathTranslator, fsClientMetrics);

    this.fileSystemAdapter = new QualifyingFileSystemAdapter(uri, fileSystemImplementation, fsClientMetrics);
  }

  @Override
  public String getCanonicalServiceName() {
    return uri.getScheme() + "://" + uri.getHost();
  }

  @Override
  public String getScheme() {
    return uri.getScheme();
  }

  @Override
  public URI getUri() {
    return uri;
  }

  @Override
  public FSDataInputStream open(Path path, int bufferSize) throws IOException {
    checkPath(path);
    return fileSystemAdapter.open(path);
  }

  @Override
  public FSDataOutputStream create(Path path,
                                   FsPermission permission,
                                   boolean overwrite,
                                   int bufferSize,
                                   short replication,
                                   long blockSize,
                                   Progressable progress)
      throws IOException {
    checkPath(path);
    return fileSystemAdapter.create(path, overwrite);
  }

  @Override
  public FSDataOutputStream createNonRecursive(Path path,
                                               FsPermission permission,
                                               EnumSet<CreateFlag> flags,
                                               int bufferSize,
                                               short replication,
                                               long blockSize,
                                               Progressable progress) throws IOException {
    checkPath(path);
    return fileSystemAdapter.createNonRecursive(path, flags);
  }

  @Override
  public FSDataOutputStream append(Path path, int bufferSize, Progressable progressable) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    checkPath(src);
    checkPath(dst);
    return fileSystemAdapter.rename(src, dst);
  }

  @Override
  public boolean delete(Path path, boolean recursive) throws IOException {
    checkPath(path);
    return fileSystemAdapter.delete(path, recursive);
  }

  @Override
  public FileStatus[] listStatus(Path path) throws IOException {
    checkPath(path);
    return fileSystemAdapter.listStatus(path);
  }

  @Override
  public void setWorkingDirectory(Path path) {
    checkPath(path);
    fileSystemAdapter.setWorkingDirectory(path);
  }

  @Override
  public Path getWorkingDirectory() {
    return fileSystemAdapter.getWorkingDirectory();
  }

  @Override
  public boolean mkdirs(Path path, FsPermission fsPermission) throws IOException {
    checkPath(path);
    return fileSystemAdapter.mkdirs(path);
  }

  @Override
  public FileStatus getFileStatus(Path path) throws IOException {
    checkPath(path);
    return fileSystemAdapter.getFileStatus(path);
  }

  @Override
  public ContentSummary getContentSummary(Path path) throws IOException {
    checkPath(path);
    if (fileSystemAdapter.supportsSpecificContentSummaryComputation()) {
      return fileSystemAdapter.getContentSummary(path);
    }
    return super.getContentSummary(path);
  }

  @Override
  public void close() throws IOException {
    LOG.info("Closing S3FS");
    super.close();
    this.fileSystemAdapter.close();
    try {
      this.runtime.close();
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }
}
