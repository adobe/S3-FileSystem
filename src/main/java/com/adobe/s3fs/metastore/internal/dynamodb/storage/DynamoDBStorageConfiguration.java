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
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.core.retry.backoff.BackoffStrategy;
import software.amazon.awssdk.core.retry.backoff.EqualJitterBackoffStrategy;
import software.amazon.awssdk.core.retry.backoff.FullJitterBackoffStrategy;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;

import java.net.URI;
import java.time.Duration;
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

  public ApacheHttpClient.Builder getApacheHttpClient() {
    int maxConnections = configuration.contextAware().getInt(MAX_HTTP_CONNECTIONS, DEFAULT_MAX_HTTP_CONNECTIONS);
    return ApacheHttpClient.builder().maxConnections(maxConnections);
  }

  public ClientOverrideConfiguration getClientOverrideConfiguration() {
    RetryPolicy retryPolicy = getRetryPolicy();
    return ClientOverrideConfiguration.builder()
        .retryPolicy(retryPolicy)
        .build();
  }

  public RetryPolicy getRetryPolicy() {
    int baseExponentialDelay = configuration.contextAware().getInt(BASE_EXPONENTIAL_DELAY_PROP, DEFAULT_BASE_EXPONENTIAL_DELAY);
    int maxExponentialDelay = configuration.contextAware().getInt(MAX_EXPONENTIAL_DELAY, DEFAULT_MAX_EXPONENTIAL_DELAY);
    boolean useFullJitter = configuration.contextAware().getBoolean(USE_FULL_JITTER_BACKOFF, DEFAULT_USE_FULL_JITTER);

    BackoffStrategy backoffStrategy;
    if (useFullJitter) {
      backoffStrategy = FullJitterBackoffStrategy.builder()
          .baseDelay(Duration.ofMillis(baseExponentialDelay))
          .maxBackoffTime(Duration.ofMillis(maxExponentialDelay))
          .build();
    } else {
      backoffStrategy = EqualJitterBackoffStrategy.builder()
          .baseDelay(Duration.ofMillis(baseExponentialDelay))
          .maxBackoffTime(Duration.ofMillis(maxExponentialDelay))
          .build();
    }

    int maxErrorRetries = configuration.contextAware().getInt(MAX_RETRIES, DEFAULT_MAX_RETRIES);

    return RetryPolicy.builder()
        .backoffStrategy(backoffStrategy)
        .throttlingBackoffStrategy(backoffStrategy)
        .numRetries(maxErrorRetries)
        .build();
  }

  public Optional<EndpointConfiguration> getEndPointConfiguration() {
    String endpoint = configuration.getString(AWS_ENDPOINT, "");
    String signingRegion = configuration.getString(AWS_SIGNING_REGION, "");

    if (!Strings.isNullOrEmpty(endpoint) && !Strings.isNullOrEmpty(signingRegion)) {
        return Optional.of(new EndpointConfiguration(URI.create(endpoint), Region.of(signingRegion)));
    }

    return Optional.empty();
  }

  public static class EndpointConfiguration {
    private final URI serviceEndpoint;
    private final Region signingRegion;

    EndpointConfiguration(URI serviceEndpoint, Region signingRegion) {
      this.serviceEndpoint = serviceEndpoint;
      this.signingRegion = signingRegion;
    }

    public URI getServiceEndpoint() {
      return serviceEndpoint;
    }

    public Region getSigningRegion() {
      return signingRegion;
    }
  }

  public Optional<AwsCredentialsProvider> getCredentialsProvider() {
    String access = configuration.getString(AWS_ACCESS_KEY_ID, "");
    String secret = configuration.getString(AWS_SECRET_ACCESS_KEY, "");

    if (!Strings.isNullOrEmpty(access) && !Strings.isNullOrEmpty(secret)) {
      return Optional.of(StaticCredentialsProvider.create(AwsBasicCredentials.create(access, secret)));
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
