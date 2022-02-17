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

import com.amazonaws.retry.PredefinedBackoffStrategies;
import com.amazonaws.retry.PredefinedRetryPolicies;
import com.amazonaws.retry.RetryPolicy;

public final class SimpleRetryPolicies {

  private SimpleRetryPolicies() {}

  public static RetryPolicy fullJitter(int baseDelay, int maxDelay, int maxRetries) {
    RetryPolicy.BackoffStrategy backoffStrategy =
        new LoggingBackoffStrategy(
            new PredefinedBackoffStrategies.FullJitterBackoffStrategy(baseDelay, maxDelay));
    RetryPolicy retryPolicy =
        new RetryPolicy(
            PredefinedRetryPolicies.DEFAULT_RETRY_CONDITION, backoffStrategy, maxRetries, true);
    return retryPolicy;
  }
}
