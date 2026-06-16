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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.retry.RetryPolicyContext;
import software.amazon.awssdk.core.retry.backoff.BackoffStrategy;

import java.time.Duration;
import java.util.Objects;

/**
 * A backoff strategy which logs its arguments before delegating the work to the injected backoff
 * strategy
 */
public class LoggingBackoffStrategy implements BackoffStrategy {

  private final Logger logger = LoggerFactory.getLogger(LoggingBackoffStrategy.class);

  private final BackoffStrategy underlyingBackoffStrategy;

  public LoggingBackoffStrategy(BackoffStrategy underlyingBackoffStrategy) {
    this.underlyingBackoffStrategy = Objects.requireNonNull(underlyingBackoffStrategy);
  }

  @Override
  public Duration computeDelayBeforeNextRetry(RetryPolicyContext context) {
    logger.info("computeDelayBeforeNextRetry retries {}, exception {}",
        context.retriesAttempted(), context.exception().toString());
    return underlyingBackoffStrategy.computeDelayBeforeNextRetry(context);
  }
}
