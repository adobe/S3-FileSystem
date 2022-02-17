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

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;

public class StreamingPrefixKeysIterator implements Iterator<S3ObjectSummary> {
  private final AmazonS3 s3Client;
  private final String bucket;
  private final String rootPrefix;

  private ObjectListing currentListing;
  private Iterator<S3ObjectSummary> currentBatch;

  public StreamingPrefixKeysIterator(AmazonS3 s3Client, String bucket, String rootPrefix) {
    this.s3Client = Objects.requireNonNull(s3Client);
    this.bucket = Objects.requireNonNull(bucket);
    this.rootPrefix = Objects.requireNonNull(rootPrefix);
  }

  @Override
  public boolean hasNext() {
    if (currentListing == null) {
      ListObjectsRequest listObjectsRequest = new ListObjectsRequest().withBucketName(bucket)
          .withMaxKeys(Integer.MAX_VALUE).withPrefix(rootPrefix);
      this.currentListing = s3Client.listObjects(listObjectsRequest);
      this.currentBatch = this.currentListing.getObjectSummaries().iterator();
    }

    boolean inCurrentBatch = this.currentBatch.hasNext();
    if (inCurrentBatch) {
      return true;
    } else if (!this.currentListing.isTruncated()) {
      return false;
    } else {
      this.currentListing = s3Client.listNextBatchOfObjects(this.currentListing);
      this.currentBatch = this.currentListing.getObjectSummaries().iterator();
      return this.currentBatch.hasNext();
    }
  }

  @Override
  public S3ObjectSummary next() {
    if (!this.hasNext()) {
      throw new NoSuchElementException();
    } else {
      return this.currentBatch.next();
    }
  }
}
