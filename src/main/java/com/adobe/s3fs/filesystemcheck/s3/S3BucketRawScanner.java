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
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  private final AmazonS3 amazonS3;

  public S3BucketRawScanner(String bucket, S3Partitioner s3Partitioner, AmazonS3 amazonS3) {
    this.bucket = Objects.requireNonNull(bucket);
    this.s3Partitioner = Objects.requireNonNull(s3Partitioner);
    this.amazonS3 = Objects.requireNonNull(amazonS3);
  }

  public List<Iterable<S3ObjectSummary>> scan() {
    return s3Partitioner.prefixes().stream()
        .map(this::partition)
        .collect(Collectors.toList());
  }

  private Iterable<S3ObjectSummary> partition(String prefix) {
    return () -> new StreamingPrefixKeysIterator(amazonS3, bucket, prefix);
  }
}
