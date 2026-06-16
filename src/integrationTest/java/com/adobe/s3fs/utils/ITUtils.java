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

package com.adobe.s3fs.utils;

import com.adobe.s3fs.metastore.internal.dynamodb.storage.DynamoDBStorageConfiguration;
import com.adobe.s3fs.operationlog.S3MetadataOperationLogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

public final class ITUtils {

  /**
   * 4.14.0 is the latest version without authentication.
   */
  public static final DockerImageName LOCALSTACK_IMAGE =
      DockerImageName.parse("localstack/localstack:4.14.0");

  public static void createMetaTableIfNotExists(DynamoDbClient dynamoDbClient, String tableName) {
    try {
      dynamoDbClient.describeTable(DescribeTableRequest.builder()
              .tableName(tableName)
              .build());
    } catch (ResourceNotFoundException ex) {
      createMetaTable(dynamoDbClient, tableName);
    }
  }

  public static void createBucketIfNotExists(S3Client s3Client, String bucket) {
    try {
      s3Client.headBucket(req -> req.bucket(bucket));
    } catch (NoSuchBucketException e) {
      s3Client.createBucket(req -> req.bucket(bucket));
    }
  }

  public static void createMetaTable(DynamoDbClient dynamoDbClient, String tableName) {
    dynamoDbClient.createTable(CreateTableRequest.builder()
        .tableName(tableName)
        .keySchema(
            KeySchemaElement.builder().keyType(KeyType.HASH).attributeName("path").build(),
            KeySchemaElement.builder().keyType(KeyType.RANGE).attributeName("children").build())
        .attributeDefinitions(
            AttributeDefinition.builder().attributeName("path").attributeType(ScalarAttributeType.S).build(),
            AttributeDefinition.builder().attributeName("children").attributeType(ScalarAttributeType.S).build())
        .billingMode(BillingMode.PROVISIONED)
        .provisionedThroughput(ProvisionedThroughput.builder().readCapacityUnits(100L).writeCapacityUnits(100L).build())
        .build());
  }

  public static void deleteMetaTable(DynamoDbClient dynamoDbClient, String tableName) {
    dynamoDbClient.deleteTable(DeleteTableRequest.builder()
        .tableName(tableName)
        .build());
  }

  public static void setFileSystemContext(String context) {
    System.setProperty("fs.s3k.metastore.context.id", context);
  }

  public static void configureDynamoAccess(LocalStackContainer container, Configuration configuration, String bucket) {
    configuration.set(DynamoDBStorageConfiguration.AWS_ENDPOINT + "." + bucket,
                      container.getEndpoint().toString());
    configuration.set(DynamoDBStorageConfiguration.AWS_SIGNING_REGION + "." + bucket,
                      container.getRegion());

    configuration.set(DynamoDBStorageConfiguration.AWS_ACCESS_KEY_ID + "." + bucket,
                      container.getAccessKey());
    configuration.set(DynamoDBStorageConfiguration.AWS_SECRET_ACCESS_KEY + "." + bucket,
                      container.getSecretKey());
  }

  public static void configureS3OperationLog(Configuration configuration, String dataBucket, String operationLogBucket) {
    configuration.set(S3MetadataOperationLogFactory.OPERATION_LOG_BUCKET + "." + dataBucket, operationLogBucket);
  }

  public static void configureS3OperationLogAccess(LocalStackContainer container, Configuration configuration, String bucket) {
    configuration.set(S3MetadataOperationLogFactory.AWS_ENDPOINT + "." + bucket, container.getEndpoint().toString());
    configuration.set(S3MetadataOperationLogFactory.AWS_SIGNING_REGION + "." + bucket, container.getRegion());

    configuration.set(S3MetadataOperationLogFactory.AWS_ACCESS_KEY_ID + "." + bucket, container.getAccessKey());
    configuration.set(S3MetadataOperationLogFactory.AWS_SECRET_ACCESS_KEY + "." + bucket, container.getSecretKey());
  }

  public static void configureS3AAsUnderlyingFileSystem(LocalStackContainer container, Configuration configuration, String bucket,
                                                        String tmpPath) {
    configuration.set("fs.s3k.storage.underlying.filesystem.scheme." + bucket, "s3a");

    configuration.setClass("fs.s3a.impl", S3AFileSystem.class, FileSystem.class);
    configuration.set("fs.s3a.buffer.dir", tmpPath);

    configuration.set("fs.s3a.access.key", container.getAccessKey());
    configuration.set("fs.s3a.secret.key", container.getSecretKey());
    configuration.set("fs.s3a.endpoint", container.getEndpoint().toString());
  }

  public static void mapBucketToTable(Configuration configuration, String bucket, String table) {
    configuration.set(String.format("fs.s3k.metastore.dynamo.table.%s", bucket), table);
  }

  public static void configureSuffixCount(Configuration configuration, String bucket, int count) {
    configuration.setInt(String.format("%s.%s", "fs.s3k.metastore.dynamo.suffix.count", bucket), count);
  }

  public static List<S3Object> listFully(S3Client s3Client, String bucket) {
    List<S3Object> result = new ArrayList<>();

    ListObjectsV2Request request = ListObjectsV2Request.builder()
        .bucket(bucket)
        .build();

    ListObjectsV2Response response;
    do {
      response = s3Client.listObjectsV2(request);
      result.addAll(response.contents());

      request = ListObjectsV2Request.builder()
          .bucket(bucket)
          .continuationToken(response.nextContinuationToken())
          .build();
    } while (response.isTruncated());

    return result;
  }

  public static void configureAsyncOperations(Configuration configuration, String bucket, String context) {
    configuration.setBoolean("fs.s3k.metastore.operations.async." + bucket + "." + context, true);
  }

  public static S3Client s3Client(LocalStackContainer localStackContainer) {
    return S3Client.builder()
        .endpointOverride(URI.create(localStackContainer.getEndpoint().toString()))
        .region(Region.of(localStackContainer.getRegion()))
        .credentialsProvider(
            StaticCredentialsProvider.create(
                AwsBasicCredentials.create(
                    localStackContainer.getAccessKey(),
                    localStackContainer.getSecretKey())))
        .build();
  }

  public static DynamoDbClient amazonDynamoDB(LocalStackContainer localStackContainer) {
    return DynamoDbClient.builder()
        .endpointOverride(URI.create(localStackContainer.getEndpoint().toString()))
        .region(Region.of(localStackContainer.getRegion()))
        .credentialsProvider(
            StaticCredentialsProvider.create(
                AwsBasicCredentials.create(
                    localStackContainer.getAccessKey(),
                    localStackContainer.getSecretKey())))
        .build();
  }
}
