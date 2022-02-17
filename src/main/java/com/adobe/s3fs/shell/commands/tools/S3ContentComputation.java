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

package com.adobe.s3fs.shell.commands.tools;

import com.adobe.s3fs.filesystemcheck.s3.S3BucketRawScanner;
import com.adobe.s3fs.filesystemcheck.s3.S3Partitioner;
import com.adobe.s3fs.utils.stream.StreamUtils;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.MoreExecutors;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class S3ContentComputation {
  private final AmazonS3 amazonS3;
  private final S3Partitioner partitioner;

  public S3ContentComputation(AmazonS3 amazonS3, S3Partitioner partitioner) {
    this.amazonS3 = Preconditions.checkNotNull(amazonS3);
    this.partitioner = Preconditions.checkNotNull(partitioner);
  }

  public Content compute(String bucket) {
    List<Iterable<S3ObjectSummary>> scannedPartitions = new S3BucketRawScanner(bucket, partitioner, amazonS3).scan();
    ExecutorService executor = Executors.newFixedThreadPool(scannedPartitions.size());

    try {
      List<Future<Content>> futures = new ArrayList<>(scannedPartitions.size());

      for (final Iterable<S3ObjectSummary> partition : scannedPartitions) {
        futures.add(executor.submit(() -> {
          long partitionSize = 0;
          long objectCount = 0;
          for (S3ObjectSummary objectSummary : partition) {
            partitionSize += objectSummary.getSize();
            objectCount++;
          }
          return new Content(objectCount, partitionSize);
        }));
      }

      return futures.stream().map(StreamUtils.uncheckedFunction(Future::get)).reduce(new Content(0, 0), Content::sum);

    } finally {
      MoreExecutors.shutdownAndAwaitTermination(executor, 10, TimeUnit.SECONDS);
    }
  }

  public static class Content {
    private final long objectCount;
    private final long size;

    public Content(long objectCount, long size) {
      this.objectCount = objectCount;
      this.size = size;
    }

    public long getSize() {
      return size;
    }

    public long getObjectCount() {
      return objectCount;
    }

    public static Content sum(Content left, Content right) {
      return new Content(left.objectCount + right.objectCount, left.size + right.size);
    }
  }
}
