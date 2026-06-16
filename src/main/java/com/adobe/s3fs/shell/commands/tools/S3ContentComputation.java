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
import com.adobe.s3fs.utils.aws.s3.model.S3ObjectLocation;
import com.adobe.s3fs.utils.stream.StreamUtils;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.MoreExecutors;
import software.amazon.awssdk.services.s3.S3Client;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class S3ContentComputation {
  private final S3Client s3Client;
  private final S3Partitioner partitioner;

  public S3ContentComputation(S3Client s3Client, S3Partitioner partitioner) {
    this.s3Client = Preconditions.checkNotNull(s3Client);
    this.partitioner = Preconditions.checkNotNull(partitioner);
  }

  public Content compute(String bucket) {
    List<Iterable<S3ObjectLocation>> scannedPartitions = new S3BucketRawScanner(bucket, partitioner, s3Client).scan();
    ExecutorService executor = Executors.newFixedThreadPool(scannedPartitions.size());

    try {
      List<Future<Content>> futures = new ArrayList<>(scannedPartitions.size());

      for (final Iterable<S3ObjectLocation> partition : scannedPartitions) {
        futures.add(executor.submit(() -> {
          long partitionSize = 0;
          long objectCount = 0;
          for (S3ObjectLocation s3ObjectLocation : partition) {
            partitionSize += s3ObjectLocation.s3Object().size();
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
