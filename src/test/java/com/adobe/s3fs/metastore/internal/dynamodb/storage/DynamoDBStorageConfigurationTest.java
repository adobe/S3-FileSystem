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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import com.adobe.s3fs.common.configuration.FileSystemConfiguration;
import com.adobe.s3fs.common.configuration.KeyValueConfiguration;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.retry.PredefinedBackoffStrategies;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Random;

public class DynamoDBStorageConfigurationTest {

  private DynamoDBStorageConfiguration dynamoDBStorageConfiguration;

  @Mock
  private FileSystemConfiguration mockConfiguration;

  @Mock
  private KeyValueConfiguration mockContextAwareConfiguration;

  private Random random = new Random(0);

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
    this.dynamoDBStorageConfiguration = new DynamoDBStorageConfiguration(mockConfiguration);

    when(mockConfiguration.contextAware()).thenReturn(mockContextAwareConfiguration);

    when
        (mockContextAwareConfiguration.getInt(DynamoDBStorageConfiguration.BASE_EXPONENTIAL_DELAY_PROP,
                                  DynamoDBStorageConfiguration.DEFAULT_BASE_EXPONENTIAL_DELAY))
        .thenReturn(Math.abs(random.nextInt()));
    when
        (mockContextAwareConfiguration.getInt(DynamoDBStorageConfiguration.MAX_EXPONENTIAL_DELAY,
                                  DynamoDBStorageConfiguration.DEFAULT_MAX_EXPONENTIAL_DELAY))
        .thenReturn(Math.abs(random.nextInt()));
  }

  @Test
  public void testCorrectRetryPolicyIsConfiguredEqualJitter() {
    when
        (mockContextAwareConfiguration.getInt(DynamoDBStorageConfiguration.MAX_RETRIES,
                                              DynamoDBStorageConfiguration.DEFAULT_MAX_RETRIES))
        .thenReturn(Math.abs(random.nextInt()));
    when
        (mockContextAwareConfiguration.getInt(DynamoDBStorageConfiguration.MAX_HTTP_CONNECTIONS,
                                  DynamoDBStorageConfiguration.DEFAULT_MAX_HTTP_CONNECTIONS))
        .thenReturn(Math.abs(random.nextInt()));
    when(
        mockContextAwareConfiguration.getBoolean(DynamoDBStorageConfiguration.USE_FULL_JITTER_BACKOFF ,
                                     DynamoDBStorageConfiguration.DEFAULT_USE_FULL_JITTER))
        .thenReturn(false);

    ClientConfiguration clientConfiguration = dynamoDBStorageConfiguration.getClientConfigurationForTable();

    assertTrue(clientConfiguration.getRetryPolicy().getBackoffStrategy()
                   instanceof PredefinedBackoffStrategies.EqualJitterBackoffStrategy);
  }

  @Test
  public void testCorrectRetryPolicyIsConfiguredFullJitter() {
    when
        (mockContextAwareConfiguration.getInt(DynamoDBStorageConfiguration.MAX_RETRIES,
                                  DynamoDBStorageConfiguration.DEFAULT_MAX_RETRIES))
        .thenReturn(Math.abs(random.nextInt()));
    when
        (mockContextAwareConfiguration.getInt(DynamoDBStorageConfiguration.MAX_HTTP_CONNECTIONS,
                                  DynamoDBStorageConfiguration.DEFAULT_MAX_HTTP_CONNECTIONS))
        .thenReturn(Math.abs(random.nextInt()));
    when(
        mockContextAwareConfiguration.getBoolean(DynamoDBStorageConfiguration.USE_FULL_JITTER_BACKOFF,
                                     DynamoDBStorageConfiguration.DEFAULT_USE_FULL_JITTER))
        .thenReturn(true);

    ClientConfiguration clientConfiguration = dynamoDBStorageConfiguration.getClientConfigurationForTable();

    assertTrue(clientConfiguration.getRetryPolicy().getBackoffStrategy()
                   instanceof PredefinedBackoffStrategies.FullJitterBackoffStrategy);
  }

  @Test
  public void testCorrectMaxRetriesIsConfigured() {
    int randMaxRetries = Math.abs(random.nextInt());
    when
        (mockContextAwareConfiguration.getInt(DynamoDBStorageConfiguration.MAX_RETRIES ,
                                  DynamoDBStorageConfiguration.DEFAULT_MAX_RETRIES))
        .thenReturn(randMaxRetries);
    when
        (mockContextAwareConfiguration.getInt(DynamoDBStorageConfiguration.MAX_HTTP_CONNECTIONS ,
                                  DynamoDBStorageConfiguration.DEFAULT_MAX_HTTP_CONNECTIONS))
        .thenReturn(Math.abs(random.nextInt()));

    ClientConfiguration clientConfiguration = dynamoDBStorageConfiguration.getClientConfigurationForTable();

    assertEquals(randMaxRetries, clientConfiguration.getRetryPolicy().getMaxErrorRetry());
    assertTrue(clientConfiguration.getRetryPolicy().isMaxErrorRetryInClientConfigHonored());
  }

  @Test
  public void testCorrectMaxHTTPConnectionsIsConfigured() {
    int randMaxHTTPConnections = Math.abs(random.nextInt());
    when
        (mockContextAwareConfiguration.getInt(DynamoDBStorageConfiguration.MAX_HTTP_CONNECTIONS ,
                                  DynamoDBStorageConfiguration.DEFAULT_MAX_HTTP_CONNECTIONS))
        .thenReturn(randMaxHTTPConnections);
    when
        (mockContextAwareConfiguration.getInt(DynamoDBStorageConfiguration.MAX_RETRIES ,
                                  DynamoDBStorageConfiguration.DEFAULT_MAX_RETRIES))
        .thenReturn(Math.abs(random.nextInt()));

    ClientConfiguration clientConfiguration = dynamoDBStorageConfiguration.getClientConfigurationForTable();

    assertEquals(randMaxHTTPConnections, clientConfiguration.getMaxConnections());
  }
}
