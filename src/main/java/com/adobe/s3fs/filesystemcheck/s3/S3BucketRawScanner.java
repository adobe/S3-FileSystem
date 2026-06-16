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

import com.adobe.s3fs.utils.aws.s3.StreamingPrefixKeysIterator;
import com.adobe.s3fs.utils.aws.s3.model.S3ObjectLocation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3Client;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Scans all objects in an S3 bucket managed by S3FS (assumptions are made about the key layout).
 */
public class S3BucketRawScanner {
  protected static final Logger LOG = LoggerFactory.getLogger(S3BucketRawScanner.class);
  // S3 bucket
  private final String bucket;
  // S3 prefixes to query
  private final S3Partitioner s3Partitioner;
  // AWS S3 client
  private final S3Client s3Client;

  public S3BucketRawScanner(String bucket, S3Partitioner s3Partitioner, S3Client s3Client) {
    this.bucket = Objects.requireNonNull(bucket);
    this.s3Partitioner = Objects.requireNonNull(s3Partitioner);
    this.s3Client = Objects.requireNonNull(s3Client);
  }

  public List<Iterable<S3ObjectLocation>> scan() {
    return s3Partitioner.prefixes().stream()
        .map(this::partition)
        .collect(Collectors.toList());
  }

  private Iterable<S3ObjectLocation> partition(String prefix) {
    return () -> new StreamingPrefixKeysIterator(s3Client, bucket, prefix);
  }
}
