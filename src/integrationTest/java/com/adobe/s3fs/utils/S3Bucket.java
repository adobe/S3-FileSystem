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

import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Object;

import org.junit.rules.ExternalResource;

import java.util.concurrent.atomic.AtomicLong;

public class S3Bucket extends ExternalResource {

  private final String bucket;
  private final S3Client s3Client;
  private static final AtomicLong counter = new AtomicLong();

  public S3Bucket(S3Client s3Client) {
    this.bucket = "bucket" + counter.incrementAndGet();
    this.s3Client = s3Client;
  }

  @Override
  protected void before() {
    s3Client.createBucket(CreateBucketRequest.builder().bucket(bucket).build());
  }

  @Override
  protected void after() {
    for (S3Object s3Object : ITUtils.listFully(s3Client, bucket)) {
      s3Client.deleteObject(DeleteObjectRequest.builder()
          .bucket(bucket)
          .key(s3Object.key())
          .build());
    }
    s3Client.deleteBucket(DeleteBucketRequest.builder().bucket(bucket).build());
  }

  public String getBucket() {
    return bucket;
  }
}
