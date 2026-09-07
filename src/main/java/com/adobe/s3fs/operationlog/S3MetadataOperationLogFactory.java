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

package com.adobe.s3fs.operationlog;

import com.adobe.s3fs.common.configuration.FileSystemConfiguration;
import com.adobe.s3fs.common.context.FileSystemContext;
import com.adobe.s3fs.metastore.api.MetadataOperationLog;
import com.adobe.s3fs.metastore.api.MetadataOperationLogFactory;
import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.core.retry.backoff.BackoffStrategy;
import software.amazon.awssdk.core.retry.backoff.EqualJitterBackoffStrategy;
import software.amazon.awssdk.core.retry.backoff.FullJitterBackoffStrategy;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;

import java.net.URI;
import java.time.Duration;

public class S3MetadataOperationLogFactory implements MetadataOperationLogFactory, Configurable {

  public static final String BASE_EXPONENTIAL_DELAY_PROP = "fs.s3k.operationlog.s3.base.exponential.delay";
  public static final String MAX_EXPONENTIAL_DELAY = "fs.s3k.operationlog.s3.max.exponential.delay";
  public static final String USE_FULL_JITTER_BACKOFF = "fs.s3k.operationlog.s3.backoff.full.jitter";
  public static final String MAX_RETRIES = "fs.s3k.operationlog.s3.max.retries";
  public static final String MAX_HTTP_CONNECTIONS = "fs.s3k.operationlog.s3.max.http.conn";
  public static final String AWS_ACCESS_KEY_ID = "fs.s3k.operationlog.s3.access";
  public static final String AWS_SECRET_ACCESS_KEY = "fs.s3k.operationlog.s3.secret";
  public static final String AWS_ENDPOINT = "fs.s3k.operationlog.s3.endpoint";
  public static final String AWS_SIGNING_REGION = "fs.s3k.operationlog.s3.signing.region";
  public static final String OPERATION_LOG_BUCKET = "fs.s3k.operationlog.s3.bucket";
  public static final String OPERATION_LOG_REGION = "fs.s3k.operationlog.s3.region";

  public static final int DEFAULT_BASE_EXPONENTIAL_DELAY = 10;
  public static final int DEFAULT_MAX_EXPONENTIAL_DELAY = 30000;
  public static final boolean DEFAULT_USE_FULL_JITTER = true;
  public static final int DEFAULT_MAX_RETRIES = 50;
  public static final int DEFAULT_MAX_HTTP_CONNECTIONS = 220;

  private Configuration configuration;

  @Override
  public MetadataOperationLog create(FileSystemContext context) {
    S3Client s3Client = createS3Client(context.configuration());
    String bucket = context.configuration().getString(OPERATION_LOG_BUCKET);
    return new S3MetadataOperationLog(s3Client, bucket, context.runtime());
  }

  @Override
  public void setConf(Configuration conf) {
    this.configuration = conf;
  }

  @Override
  public Configuration getConf() {
    return configuration;
  }

  private S3Client createS3Client(FileSystemConfiguration configuration) {
    S3ClientBuilder clientBuilder = S3Client.builder();

    int baseDelay = configuration.contextAware().getInt(BASE_EXPONENTIAL_DELAY_PROP, DEFAULT_BASE_EXPONENTIAL_DELAY);
    int maxDelay = configuration.contextAware().getInt(MAX_EXPONENTIAL_DELAY, DEFAULT_MAX_EXPONENTIAL_DELAY);
    int maxRetries = configuration.contextAware().getInt(MAX_RETRIES, DEFAULT_MAX_RETRIES);
    int maxConnections = configuration.contextAware().getInt(MAX_HTTP_CONNECTIONS, DEFAULT_MAX_HTTP_CONNECTIONS);
    boolean useFullJitter = configuration.contextAware().getBoolean(USE_FULL_JITTER_BACKOFF, DEFAULT_USE_FULL_JITTER);

    BackoffStrategy backoffStrategy;
    if (useFullJitter) {
      backoffStrategy = FullJitterBackoffStrategy.builder()
          .baseDelay(Duration.ofMillis(baseDelay))
          .maxBackoffTime(Duration.ofMillis(maxDelay))
          .build();
    } else {
      backoffStrategy = EqualJitterBackoffStrategy.builder()
          .baseDelay(Duration.ofMillis(baseDelay))
          .maxBackoffTime(Duration.ofMillis(maxDelay))
          .build();
    }

    RetryPolicy retryPolicy = RetryPolicy.builder()
        .backoffStrategy(backoffStrategy)
        .throttlingBackoffStrategy(backoffStrategy)
        .numRetries(maxRetries)
        .build();

    clientBuilder.overrideConfiguration(config -> config.retryPolicy(retryPolicy));
    clientBuilder.httpClientBuilder(ApacheHttpClient.builder().maxConnections(maxConnections));

    String accessKey = configuration.getString(AWS_ACCESS_KEY_ID, "");
    String secretKey = configuration.getString(AWS_SECRET_ACCESS_KEY, "");
    if (!"".equals(accessKey) && !"".equals(secretKey)) {
      clientBuilder.credentialsProvider(
          StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey)));
    }

    String region = configuration.getString(OPERATION_LOG_REGION, "");
    if (!"".equals(region)) {
      clientBuilder.region(Region.of(region));
    }

    String endpoint = configuration.getString(AWS_ENDPOINT, "");
    String signingRegion = configuration.getString(AWS_SIGNING_REGION, "");
    if (!"".equals(endpoint)) {
      Preconditions.checkArgument(!"".equals(signingRegion),
          "%s must be set when %s is set", AWS_SIGNING_REGION, AWS_ENDPOINT);
      clientBuilder.endpointOverride(URI.create(endpoint));
      clientBuilder.region(Region.of(signingRegion));
    }

    return clientBuilder.build();
  }
}
