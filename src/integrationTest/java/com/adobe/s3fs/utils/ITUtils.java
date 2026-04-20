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
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.*;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.internal.SkipMd5CheckStrategy;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.ArrayList;
import java.util.List;

public final class ITUtils {

  /**
   * 4.14.0 is the latest version without authentication.
   */
  public static final DockerImageName LOCALSTACK_IMAGE =
      DockerImageName.parse("localstack/localstack:4.14.0");

  public static void createMetaTableIfNotExists(AmazonDynamoDB dynamoDB, String tableName) {
    try {
      DescribeTableResult ignored = dynamoDB.describeTable(tableName);
    } catch (ResourceNotFoundException ex) {
      createMetaTable(dynamoDB, tableName);
    }
  }

  public static void createBucketIfNotExists(AmazonS3 amazonS3, String bucket) {
    if (!amazonS3.doesBucketExistV2(bucket)) {
      amazonS3.createBucket(bucket);
    }
  }

  public static void createMetaTable(AmazonDynamoDB dynamoDB, String tableName) {
    dynamoDB.createTable(new CreateTableRequest()
                             .withTableName(tableName)
                             .withKeySchema(new KeySchemaElement().withKeyType(KeyType.HASH).withAttributeName("path"),
                                            new KeySchemaElement().withKeyType(KeyType.RANGE).withAttributeName("children"))
                             .withAttributeDefinitions(new AttributeDefinition("path", ScalarAttributeType.S),
                                                       new AttributeDefinition("children", ScalarAttributeType.S))
                             .withProvisionedThroughput(new ProvisionedThroughput(100L, 100L)));
  }

  public static void deleteMetaTable(AmazonDynamoDB dynamoDB, String tableName) {
    dynamoDB.deleteTable(tableName);
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
    System.setProperty(SkipMd5CheckStrategy.DISABLE_GET_OBJECT_MD5_VALIDATION_PROPERTY, "true");
    System.setProperty(SkipMd5CheckStrategy.DISABLE_PUT_OBJECT_MD5_VALIDATION_PROPERTY, "true");

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

  public static List<S3ObjectSummary> listFully(AmazonS3 s3, String bucket) {
    List<S3ObjectSummary> result = new ArrayList<>();

    ObjectListing objectListing = s3.listObjects(new ListObjectsRequest().withBucketName(bucket));
    result.addAll(objectListing.getObjectSummaries());

    while (objectListing.isTruncated()) {
      objectListing = s3.listNextBatchOfObjects(objectListing);
      result.addAll(objectListing.getObjectSummaries());
    }

    return result;
  }

  public static void configureAsyncOperations(Configuration configuration, String bucket, String context) {
    configuration.setBoolean("fs.s3k.metastore.operations.async." + bucket + "." + context, true);
  }

  public static AmazonS3 amazonS3(LocalStackContainer localStackContainer) {
    return AmazonS3ClientBuilder.standard()
        .withEndpointConfiguration(
            new AwsClientBuilder.EndpointConfiguration(
                localStackContainer.getEndpoint().toString(),
                localStackContainer.getRegion()))
        .withCredentials(
            new AWSStaticCredentialsProvider(
                new BasicAWSCredentials(
                    localStackContainer.getAccessKey(),
                    localStackContainer.getSecretKey())))
        .build();
  }

  public static AmazonDynamoDB amazonDynamoDB(LocalStackContainer localStackContainer) {
    return AmazonDynamoDBClientBuilder.standard()
        .withEndpointConfiguration(
            new AwsClientBuilder.EndpointConfiguration(
                localStackContainer.getEndpoint().toString(),
                localStackContainer.getRegion()))
        .withCredentials(
            new AWSStaticCredentialsProvider(
                new BasicAWSCredentials(
                    localStackContainer.getAccessKey(),
                    localStackContainer.getSecretKey())))
        .build();
  }
}
