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

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.retry.PredefinedBackoffStrategies;
import com.amazonaws.retry.PredefinedRetryPolicies;
import com.amazonaws.retry.RetryPolicy;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;

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

  public static final int DEFAULT_BASE_EXPONENTIAL_DELAY = 10;
  public static final int DEFAULT_MAX_EXPONENTIAL_DELAY = 30000;
  public static final boolean DEFAULT_USE_FULL_JITTER = true;
  public static final int DEFAULT_MAX_RETRIES = 50;
  public static final int DEFAULT_MAX_HTTP_CONNECTIONS = 220;

  private Configuration configuration;

  @Override
  public MetadataOperationLog create(FileSystemContext context) {
    AmazonS3 amazonS3 = createS3Client(context.configuration());
    String bucket = context.configuration().getString(OPERATION_LOG_BUCKET);
    return new S3MetadataOperationLog(amazonS3, bucket, context.runtime());
  }

  @Override
  public void setConf(Configuration conf) {
    this.configuration = conf;
  }

  @Override
  public Configuration getConf() {
    return configuration;
  }

  private AmazonS3 createS3Client(FileSystemConfiguration configuration) {
    AmazonS3ClientBuilder clientBuilder = AmazonS3ClientBuilder.standard();

    int baseDelay = configuration.contextAware().getInt(BASE_EXPONENTIAL_DELAY_PROP, DEFAULT_BASE_EXPONENTIAL_DELAY);
    int maxDelay = configuration.contextAware().getInt(MAX_EXPONENTIAL_DELAY, DEFAULT_MAX_EXPONENTIAL_DELAY);
    int maxRetries = configuration.contextAware().getInt(MAX_RETRIES, DEFAULT_MAX_RETRIES);
    int maxConnections = configuration.contextAware().getInt(MAX_HTTP_CONNECTIONS, DEFAULT_MAX_HTTP_CONNECTIONS);
    boolean useFullJitter = configuration.contextAware().getBoolean(USE_FULL_JITTER_BACKOFF, DEFAULT_USE_FULL_JITTER);

    RetryPolicy.BackoffStrategy backoffStrategy = null;
    if (useFullJitter) {
      backoffStrategy = new PredefinedBackoffStrategies.FullJitterBackoffStrategy(baseDelay, maxDelay);
    } else {
      backoffStrategy = new PredefinedBackoffStrategies.EqualJitterBackoffStrategy(baseDelay, maxDelay);
    }

    RetryPolicy retryPolicy = new RetryPolicy(PredefinedRetryPolicies.DEFAULT_RETRY_CONDITION,
                                              backoffStrategy,
                                              maxRetries,
                                              true);

    String accessKey = configuration.getString(AWS_ACCESS_KEY_ID, "");
    String secretKey = configuration.getString(AWS_SECRET_ACCESS_KEY, "");
    if (!"".equals(accessKey) && !"".equals(secretKey)) {
      clientBuilder.withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKey, secretKey)));
    }

    String endpoint = configuration.getString(AWS_ENDPOINT, "");
    String signingRegion = configuration.getString(AWS_SIGNING_REGION, "");
    if (!"".equals(endpoint) && !"".equals(signingRegion)) {
      clientBuilder.withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(endpoint, signingRegion));
    }

    return clientBuilder.withClientConfiguration(new ClientConfiguration()
                                     .withRetryPolicy(retryPolicy)
                                     .withMaxConnections(maxConnections))
        .build();
  }
}
