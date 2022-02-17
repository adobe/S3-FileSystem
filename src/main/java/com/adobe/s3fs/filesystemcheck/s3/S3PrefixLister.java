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

import com.adobe.s3fs.utils.exceptions.UncheckedException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ExecutionException;

/**
 * Use this class in conjunction with S3PrefixPartitioner in order to list a given bucket by storing
 * all S3 key prefixes belonging to a given partition into a file (corresponding to a partition)
 * stored into a S3 directory
 */
public class S3PrefixLister implements S3Lister {

  private static final Logger LOG = LoggerFactory.getLogger(S3PrefixLister.class);

  private final S3Partitioner s3Partitioner;
  private final Configuration config;
  private final AmazonS3 amazonS3;

  /**
   * @param s3Partitioner Prefix partitioner
   * @param config Hadoop configuration
   */
  public S3PrefixLister(S3Partitioner s3Partitioner, Configuration config, AmazonS3 amazonS3) {
    this.s3Partitioner = Objects.requireNonNull(s3Partitioner);
    this.config = new Configuration(Objects.requireNonNull(config)); // create a copy
    this.amazonS3 = Objects.requireNonNull(amazonS3);
  }

  /**
   * List into the S3 directory location for each partition provided by s3PrefixPartitioner the S3
   * key prefixes belonging to a given partition
   *
   * @return true if the operation succeeds. In case of a failure this method will throw an
   *     unchecked exception
   */
  @Override
  public boolean list(String bucket, String bufferDir) {
    Objects.requireNonNull(bucket);
    Objects.requireNonNull(bufferDir);

    List<Iterable<S3ObjectSummary>> listingPartitions =
        new S3BucketRawScanner(bucket, s3Partitioner, amazonS3).scan();

    ExecutorService executorService = Executors.newFixedThreadPool(listingPartitions.size());

    Path bufferPath = new Path(bufferDir);
    FileSystem fileSystem = getFs(bufferPath);

    List<Future<?>> futures = new ArrayList<>();
    try {
      for (Iterable<S3ObjectSummary> partition : listingPartitions) {
        futures.add(executorService.submit(downloadPartition(fileSystem, bufferPath, partition)));
      }
      for (Future<?> f : futures) {
        f.get();
      }
    } catch (InterruptedException e) {
      LOG.error("InterruptedException thrown while processing a partition!", e);
      Thread.currentThread().interrupt();
      throw new UncheckedException(e);
    } catch (ExecutionException e) {
      LOG.error("ExecutionException thrown while processing a partition!", e);
      throw new UncheckedException(e);
    } finally {
      MoreExecutors.shutdownAndAwaitTermination(
          executorService, Integer.MAX_VALUE, TimeUnit.MILLISECONDS);
    }
    return true;
  }

  private FileSystem getFs(Path path) {
    try {
      return path.getFileSystem(config);
    } catch (IOException ioe) {
      LOG.error("Exception thrown while retrieving file system!", ioe);
      throw new UncheckedIOException(ioe);
    }
  }

  private Runnable downloadPartition(
      FileSystem fs, Path rootDownloadPath, Iterable<S3ObjectSummary> partition) {
    return () -> {
      // Verify arguments
      Objects.requireNonNull(fs);
      Objects.requireNonNull(rootDownloadPath);
      Objects.requireNonNull(partition);

      // Early exist if current partition is empty
      Iterator<S3ObjectSummary> it = partition.iterator();
      if (!it.hasNext()) {
        return; // Early return
      }

      Path partitionDownloadPath = new Path(rootDownloadPath, UUID.randomUUID().toString());
      try (BufferedWriter writer =
          new BufferedWriter(new OutputStreamWriter(fs.create(partitionDownloadPath)))) {
        for (S3ObjectSummary objectSummary : partition) {
          writer.write("s3://" + objectSummary.getBucketName() + "/" + objectSummary.getKey());
          writer.newLine();
        }
      } catch (IOException ioe) {
        LOG.error("IOException thrown while writing prefixes for S3 partition!", ioe);
        throw new UncheckedIOException(ioe);
      }
    };
  }
}
