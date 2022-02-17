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

package com.adobe.s3fs.utils;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.*;

import org.junit.rules.ExternalResource;

import java.util.concurrent.atomic.AtomicLong;

public class S3Bucket extends ExternalResource {

  private final String bucket;
  private final AmazonS3 amazonS3;
  private static final AtomicLong counter = new AtomicLong();

  public S3Bucket(AmazonS3 amazonS3) {
    this.bucket = "bucket" + counter.incrementAndGet();
    this.amazonS3 = amazonS3;
  }

  @Override
  protected void before() throws Throwable {
    amazonS3.createBucket(bucket);
  }

  @Override
  protected void after() {
    for (S3ObjectSummary objectSummary : ITUtils.listFully(amazonS3, bucket)) {
      amazonS3.deleteObject(new DeleteObjectRequest(bucket, objectSummary.getKey()));
    }
    amazonS3.deleteBucket(bucket);
  }

  public String getBucket() {
    return bucket;
  }
}
