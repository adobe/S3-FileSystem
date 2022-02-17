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

import com.adobe.s3fs.common.configuration.FileSystemConfiguration;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.retry.PredefinedBackoffStrategies;
import com.amazonaws.retry.PredefinedRetryPolicies;
import com.amazonaws.retry.RetryPolicy;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import java.util.Optional;

public class DynamoDBStorageConfiguration {

  public static final String BASE_EXPONENTIAL_DELAY_PROP = "fs.s3k.metastore.dynamo.base.exponential.delay";
  public static final String MAX_EXPONENTIAL_DELAY = "fs.s3k.metastore.dynamo.max.exponential.delay";
  public static final String USE_FULL_JITTER_BACKOFF = "fs.s3k.metastore.dynamo.backoff.full.jitter";
  public static final String MAX_RETRIES = "fs.s3k.metastore.dynamo.max.retries";
  public static final String MAX_HTTP_CONNECTIONS = "fs.s3k.metastore.dynamo.max.http.conn";
  public static final String AWS_ACCESS_KEY_ID = "fs.s3k.metastore.dynamo.access";
  public static final String AWS_SECRET_ACCESS_KEY = "fs.s3k.metastore.dynamo.secret";
  public static final String AWS_ENDPOINT = "fs.s3k.metastore.dynamo.endpoint";
  public static final String AWS_SIGNING_REGION = "fs.s3k.metastore.dynamo.signing.region";

  public static final int DEFAULT_BASE_EXPONENTIAL_DELAY = 80;
  public static final int DEFAULT_MAX_EXPONENTIAL_DELAY = 60000;
  public static final boolean DEFAULT_USE_FULL_JITTER = true;
  public static final int DEFAULT_MAX_RETRIES = 50;
  public static final int DEFAULT_MAX_HTTP_CONNECTIONS = 50;

  private final FileSystemConfiguration configuration;

  public DynamoDBStorageConfiguration(FileSystemConfiguration configuration) {
    this.configuration = Preconditions.checkNotNull(configuration);
  }

  public ClientConfiguration getClientConfigurationForTable() {
    int baseExponentialDelay = configuration.contextAware().getInt(BASE_EXPONENTIAL_DELAY_PROP, DEFAULT_BASE_EXPONENTIAL_DELAY);
    int maxExponentialDelay = configuration.contextAware().getInt(MAX_EXPONENTIAL_DELAY, DEFAULT_MAX_EXPONENTIAL_DELAY);

    boolean useFullJitter = configuration.contextAware().getBoolean(USE_FULL_JITTER_BACKOFF, DEFAULT_USE_FULL_JITTER);

    RetryPolicy.BackoffStrategy backoffStrategy;

    if (useFullJitter) {
      backoffStrategy = new PredefinedBackoffStrategies.FullJitterBackoffStrategy(baseExponentialDelay, maxExponentialDelay);
    } else {
      backoffStrategy = new PredefinedBackoffStrategies.EqualJitterBackoffStrategy(baseExponentialDelay, maxExponentialDelay);
    }

    int maxErrorRetries = configuration.contextAware().getInt(MAX_RETRIES, DEFAULT_MAX_RETRIES);

    RetryPolicy retryPolicy = new RetryPolicy(PredefinedRetryPolicies.DEFAULT_RETRY_CONDITION,
                                              backoffStrategy,
                                              maxErrorRetries,
                                              true);

    return new ClientConfiguration()
        .withRetryPolicy(retryPolicy)
        .withMaxConnections(configuration.contextAware().getInt(MAX_HTTP_CONNECTIONS, DEFAULT_MAX_HTTP_CONNECTIONS));
  }

  public Optional<AwsClientBuilder.EndpointConfiguration> getEndPointConfiguration() {
    String endpoint = configuration.getString(AWS_ENDPOINT, "");
    String signingRegion = configuration.getString(AWS_SIGNING_REGION, "");

    if (!Strings.isNullOrEmpty(endpoint) && !Strings.isNullOrEmpty(signingRegion)) {
      return Optional.of(new AwsClientBuilder.EndpointConfiguration(endpoint, signingRegion));
    }

    return Optional.empty();
  }

  public Optional<AWSCredentialsProvider> getCredentialsProvider() {
    String access = configuration.getString(AWS_ACCESS_KEY_ID, "");
    String secret = configuration.getString(AWS_SECRET_ACCESS_KEY, "");

    if (!Strings.isNullOrEmpty(access) && !Strings.isNullOrEmpty(secret)) {
      return Optional.of(new AWSStaticCredentialsProvider(new BasicAWSCredentials(access, secret)));
    }

    return Optional.empty();
  }

  public int getUpdateObjectRetries() {
    return configuration.getInt("fs.s3k.metastore.update.object.retries", 25);
  }

  public int getUpdateObjectDelayBetweenRetries() {
    return configuration.getInt("fs.s3k.metastore.update.object.delay.between.retries", 200);
  }
}
