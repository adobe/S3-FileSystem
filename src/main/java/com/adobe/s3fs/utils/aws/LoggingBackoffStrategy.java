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

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.retry.RetryPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * A backoff strategy which logs its arguments before delegating the work to the injected backoff
 * strategy
 */
public class LoggingBackoffStrategy implements RetryPolicy.BackoffStrategy {

  private final Logger logger = LoggerFactory.getLogger(LoggingBackoffStrategy.class);

  private final RetryPolicy.BackoffStrategy underlyingBackoffStrategy;

  public LoggingBackoffStrategy(RetryPolicy.BackoffStrategy underlyingBackoffStrategy) {
    this.underlyingBackoffStrategy = Objects.requireNonNull(underlyingBackoffStrategy);
  }

  @Override
  public long delayBeforeNextRetry(
      AmazonWebServiceRequest originalRequest, AmazonClientException ex, int retries) {
    logger.info("delayBeforeNextRetry retries {}, exception {}", retries, ex);
    return underlyingBackoffStrategy.delayBeforeNextRetry(originalRequest, ex, retries);
  }
}
