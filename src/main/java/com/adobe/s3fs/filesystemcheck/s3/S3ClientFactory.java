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

package com.adobe.s3fs.filesystemcheck.s3;

import com.amazonaws.retry.RetryPolicy;
import com.amazonaws.services.s3.AmazonS3;

/** Factory for creation of {@link AmazonS3} client instances. */
public interface S3ClientFactory {
  /**
   * @param retryPolicy Policy used during retries
   * @param maxConnections maximum number of connections
   * @return S3 client
   */
  AmazonS3 newS3Client(RetryPolicy retryPolicy, int maxConnections);
}
