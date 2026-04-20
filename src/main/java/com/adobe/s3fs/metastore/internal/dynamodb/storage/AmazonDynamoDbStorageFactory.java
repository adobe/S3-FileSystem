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
import com.google.common.base.Preconditions;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClientBuilder;

public class AmazonDynamoDbStorageFactory implements DynamoDBStorageFactory {

  private final DynamoDBStorageConfiguration configuration;

  public AmazonDynamoDbStorageFactory(DynamoDBStorageConfiguration configuration) {
    this.configuration = Preconditions.checkNotNull(configuration);
  }

  @Override
  public DynamoDBStorage create(String table, FileSystemContext context) {
    ApacheHttpClient.Builder httpClient = configuration.getApacheHttpClient();
    DynamoDbClientBuilder clientBuilder = DynamoDbClient.builder()
        .httpClientBuilder(httpClient)
        .overrideConfiguration(configuration.getClientOverrideConfiguration());

    configuration.getEndPointConfiguration().ifPresent(endpointConfiguration -> {
      clientBuilder.endpointOverride(endpointConfiguration.getServiceEndpoint());
      clientBuilder.region(endpointConfiguration.getSigningRegion());
    });
    configuration.getCredentialsProvider().ifPresent(clientBuilder::credentialsProvider);

    DynamoDbClient dynamoDbClient = clientBuilder.build();
    return new AmazonDynamoDBStorage(dynamoDbClient, table, context.runtime());
  }
}
