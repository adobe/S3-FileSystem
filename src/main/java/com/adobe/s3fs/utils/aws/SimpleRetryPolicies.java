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

package com.adobe.s3fs.utils.aws;

import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.core.retry.backoff.BackoffStrategy;
import software.amazon.awssdk.core.retry.backoff.FullJitterBackoffStrategy;

import java.time.Duration;

public final class SimpleRetryPolicies {

  private SimpleRetryPolicies() {}

  public static RetryPolicy fullJitter(int baseDelay, int maxDelay, int maxRetries) {
    BackoffStrategy backoffStrategy =
        new LoggingBackoffStrategy(
            FullJitterBackoffStrategy.builder()
                .baseDelay(Duration.ofMillis(baseDelay))
                .maxBackoffTime(Duration.ofMillis(maxDelay))
                .build());
    RetryPolicy retryPolicy = RetryPolicy.builder()
        .backoffStrategy(backoffStrategy)
        .numRetries(maxRetries)
        .build();
    return retryPolicy;
  }
}
