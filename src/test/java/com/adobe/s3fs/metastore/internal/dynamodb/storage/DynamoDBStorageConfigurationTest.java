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
import com.adobe.s3fs.common.configuration.KeyValueConfiguration;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.core.retry.backoff.EqualJitterBackoffStrategy;
import software.amazon.awssdk.core.retry.backoff.FullJitterBackoffStrategy;

import java.net.URI;
import java.util.Optional;

import static org.junit.Assert.*;
import static org.mockito.Mockito.when;

public class DynamoDBStorageConfigurationTest {

  private DynamoDBStorageConfiguration dynamoDBStorageConfiguration;

  @Mock
  private FileSystemConfiguration mockConfiguration;

  @Mock
  private KeyValueConfiguration mockContextAwareConfiguration;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
    this.dynamoDBStorageConfiguration = new DynamoDBStorageConfiguration(mockConfiguration);
    when(mockConfiguration.contextAware()).thenReturn(mockContextAwareConfiguration);
  }

  @Test
  public void testCorrectRetryPolicyIsConfiguredEqualJitter() {
    when(mockContextAwareConfiguration.getBoolean(DynamoDBStorageConfiguration.USE_FULL_JITTER_BACKOFF,
            DynamoDBStorageConfiguration.DEFAULT_USE_FULL_JITTER))
            .thenReturn(false);

    RetryPolicy retryPolicy = dynamoDBStorageConfiguration.getRetryPolicy();
    assertNotNull(retryPolicy);
    assertTrue(retryPolicy.backoffStrategy() instanceof EqualJitterBackoffStrategy);
  }

  @Test
  public void testCorrectRetryPolicyIsConfiguredFullJitter() {
    when(mockContextAwareConfiguration.getBoolean(DynamoDBStorageConfiguration.USE_FULL_JITTER_BACKOFF,
            DynamoDBStorageConfiguration.DEFAULT_USE_FULL_JITTER))
            .thenReturn(true);

    RetryPolicy retryPolicy = dynamoDBStorageConfiguration.getRetryPolicy();
    assertNotNull(retryPolicy);
    assertTrue(retryPolicy.backoffStrategy() instanceof FullJitterBackoffStrategy);
  }

  @Test
  public void testCorrectMaxRetriesIsConfigured() {
    Integer randomMaxRetries = 99;
    when(mockContextAwareConfiguration.getInt(DynamoDBStorageConfiguration.MAX_RETRIES,
            DynamoDBStorageConfiguration.DEFAULT_MAX_RETRIES))
            .thenReturn(randomMaxRetries);

    RetryPolicy retryPolicy = dynamoDBStorageConfiguration.getRetryPolicy();
    assertEquals(randomMaxRetries, retryPolicy.numRetries());
  }

  @Test
  public void testEndpointConfiguration() {
    when(mockConfiguration.getString(DynamoDBStorageConfiguration.AWS_ENDPOINT, ""))
        .thenReturn("http://localhost:8000");
    when(mockConfiguration.getString(DynamoDBStorageConfiguration.AWS_SIGNING_REGION, ""))
            .thenReturn("us-west-1");

    Optional<DynamoDBStorageConfiguration.EndpointConfiguration> endpoint =
            dynamoDBStorageConfiguration.getEndPointConfiguration();
    assertTrue(endpoint.isPresent());
    DynamoDBStorageConfiguration.EndpointConfiguration endpointConfiguration = endpoint.get();
    assertEquals(URI.create("http://localhost:8000"), endpointConfiguration.getServiceEndpoint());
    assertEquals("us-west-1", endpointConfiguration.getSigningRegion().id());
  }

  @Test
  public void testEndpointConfigurationReturnsEmptyWhenEndpointNotSet() {
    when(mockConfiguration.getString(DynamoDBStorageConfiguration.AWS_ENDPOINT, ""))
        .thenReturn("");

    Optional<DynamoDBStorageConfiguration.EndpointConfiguration> endpoint =
            dynamoDBStorageConfiguration.getEndPointConfiguration();
    assertFalse(endpoint.isPresent());
  }

  @Test
  public void testEndpointConfigurationReturnsEmptyWhenRegionNotSet() {
    when(mockConfiguration.getString(DynamoDBStorageConfiguration.AWS_ENDPOINT, ""))
            .thenReturn("http://localhost:8000");

    Optional<DynamoDBStorageConfiguration.EndpointConfiguration> endpoint =
            dynamoDBStorageConfiguration.getEndPointConfiguration();
    assertFalse(endpoint.isPresent());
  }

  @Test
  public void testCredentialsProviderConfiguration() {
    when(mockConfiguration.getString(DynamoDBStorageConfiguration.AWS_ACCESS_KEY_ID, ""))
        .thenReturn("testAccessKey");
    when(mockConfiguration.getString(DynamoDBStorageConfiguration.AWS_SECRET_ACCESS_KEY, ""))
        .thenReturn("testSecretKey");

    Optional<AwsCredentialsProvider> credentialsProvider = dynamoDBStorageConfiguration.getCredentialsProvider();
    assertTrue(credentialsProvider.isPresent());
    assertNotNull(credentialsProvider.get().resolveCredentials());
    assertEquals("testAccessKey", credentialsProvider.get().resolveCredentials().accessKeyId());
    assertEquals("testSecretKey", credentialsProvider.get().resolveCredentials().secretAccessKey());
  }

  @Test
  public void testCredentialsProviderReturnsEmptyWhenNotSet() {
    when(mockConfiguration.getString(DynamoDBStorageConfiguration.AWS_ACCESS_KEY_ID, ""))
        .thenReturn("");
    when(mockConfiguration.getString(DynamoDBStorageConfiguration.AWS_SECRET_ACCESS_KEY, ""))
        .thenReturn("");

    Optional<AwsCredentialsProvider> credentialsProvider = dynamoDBStorageConfiguration.getCredentialsProvider();
    assertFalse(credentialsProvider.isPresent());
  }
}
