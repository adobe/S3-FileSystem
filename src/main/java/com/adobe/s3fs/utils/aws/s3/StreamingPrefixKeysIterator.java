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

package com.adobe.s3fs.utils.aws.s3;

import com.adobe.s3fs.utils.aws.s3.model.S3ObjectLocation;
import com.google.common.collect.FluentIterable;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;

public class StreamingPrefixKeysIterator implements Iterator<S3ObjectLocation> {
  private final S3Client s3Client;
  private final String bucket;
  private final String rootPrefix;

  private ListObjectsV2Response currentListing;
  private Iterator<S3ObjectLocation> currentBatch;

  public StreamingPrefixKeysIterator(S3Client s3Client, String bucket, String rootPrefix) {
    this.s3Client = Objects.requireNonNull(s3Client);
    this.bucket = Objects.requireNonNull(bucket);
    this.rootPrefix = Objects.requireNonNull(rootPrefix);
  }

  @Override
  public boolean hasNext() {
    if (currentListing == null) {
      ListObjectsV2Request listObjectsRequest = ListObjectsV2Request.builder()
          .bucket(bucket)
          .maxKeys(Integer.MAX_VALUE)
          .prefix(rootPrefix)
          .build();
      this.currentListing = s3Client.listObjectsV2(listObjectsRequest);
      this.currentBatch = toBucketAwareIterator(this.currentListing.contents());
    }

    boolean inCurrentBatch = this.currentBatch.hasNext();
    if (inCurrentBatch) {
      return true;
    } else if (!this.currentListing.isTruncated()) {
      return false;
    } else {
      ListObjectsV2Request nextRequest = ListObjectsV2Request.builder()
          .bucket(bucket)
          .maxKeys(Integer.MAX_VALUE)
          .prefix(rootPrefix)
          .continuationToken(this.currentListing.nextContinuationToken())
          .build();
      this.currentListing = s3Client.listObjectsV2(nextRequest);
      this.currentBatch = toBucketAwareIterator(this.currentListing.contents());
      return this.currentBatch.hasNext();
    }
  }

  @Override
  public S3ObjectLocation next() {
    if (!this.hasNext()) {
      throw new NoSuchElementException();
    } else {
      return this.currentBatch.next();
    }
  }

  private Iterator<S3ObjectLocation> toBucketAwareIterator(Iterable<S3Object> contents) {
    return FluentIterable.from(contents)
            .transform(s3Object -> S3ObjectLocation.builder().bucket(bucket).s3Object(s3Object).build())
            .iterator();
  }
}
