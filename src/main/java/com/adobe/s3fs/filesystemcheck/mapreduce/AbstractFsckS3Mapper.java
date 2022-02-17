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

package com.adobe.s3fs.filesystemcheck.mapreduce;

import com.adobe.s3fs.filesystemcheck.mapreduce.data.LogicalObjectWritable;
import com.adobe.s3fs.filesystemcheck.mapreduce.multioutputs.MultiOutputs;
import com.adobe.s3fs.filesystemcheck.mapreduce.multioutputs.MultiOutputsAdapter;
import com.adobe.s3fs.filesystemcheck.mapreduce.multioutputs.MultiOutputsFactory;
import com.adobe.s3fs.filesystemcheck.s3.S3ClientFactory;
import com.adobe.s3fs.filesystemcheck.utils.UriMetadataPair;
import com.adobe.s3fs.operationlog.LogicalFileMetadataV2;
import com.adobe.s3fs.operationlog.ObjectMetadataSerialization;
import com.adobe.s3fs.utils.aws.LoggingBackoffStrategy;
import com.adobe.s3fs.utils.mapreduce.SerializableVoid;
import com.amazonaws.retry.PredefinedBackoffStrategies;
import com.amazonaws.retry.PredefinedRetryPolicies;
import com.amazonaws.retry.RetryPolicy;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Objects;
import java.util.Optional;

import static com.adobe.s3fs.filesystemcheck.mapreduce.FileSystemMRJobConfig.*;
import static com.adobe.s3fs.operationlog.S3MetadataOperationLog.INFO_SUFFIX;

public abstract class AbstractFsckS3Mapper extends Mapper<SerializableVoid, S3ObjectSummary, Text, LogicalObjectWritable> {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractFsckS3Mapper.class);

  private static final String PHYSICAL_DATA_MARKER = ".id=";
  // deleteObject marker
  protected static final Text DELETE_OBJECT = new Text("deleteObject");
  // S3 client factory
  private final S3ClientFactory s3ClientFactory;
  // Mos factory
  private final MultiOutputsFactory mosFactory;
  // S3 client
  protected AmazonS3 s3Client;
  // Map context
  protected Mapper<SerializableVoid, S3ObjectSummary, Text, LogicalObjectWritable>.Context context;
  // Logical root path (from metastore)
  protected String rootPath;
  // Multiple outputs adapter
  protected MultiOutputs mosAdapter;

  @VisibleForTesting
  public AbstractFsckS3Mapper(
      S3ClientFactory s3ClientFactory, MultiOutputsFactory mosFactory) {
    this.s3ClientFactory = Objects.requireNonNull(s3ClientFactory);
    this.mosFactory = Objects.requireNonNull(mosFactory);
  }

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);

    this.context = context;

    Configuration config = context.getConfiguration();
    this.rootPath = new Path(config.get(LOGICAL_ROOT_PATH)).toString();

    int baseDelay = config.getInt(S3_BACKOFF_BASE_DELAY, 5);
    int maxDelay = config.getInt(S3_BACKOFF_MAX_DELAY, 1000);
    int retries = config.getInt(S3_RETRIES, 5);
    int maxS3Connections = config.getInt(S3_DOWNLOAD_BATCH_SIZE, 10);

    RetryPolicy.BackoffStrategy backoffStrategy =
        new LoggingBackoffStrategy(
            new PredefinedBackoffStrategies.FullJitterBackoffStrategy(baseDelay, maxDelay));
    RetryPolicy retryPolicy =
        new RetryPolicy(
            PredefinedRetryPolicies.DEFAULT_RETRY_CONDITION, backoffStrategy, retries, true);
    this.s3Client = this.s3ClientFactory.newS3Client(retryPolicy, maxS3Connections);

    this.mosAdapter =
        MultiOutputsAdapter.createInstance(this.mosFactory.newMultipleOutputs(context));
  }

  @Override
  protected void map(SerializableVoid key, S3ObjectSummary value, Context context) throws IOException, InterruptedException {

    String bucketName = value.getBucketName();
    String prefix = value.getKey();
    URI sourceUri = new Path("s3://" + bucketName + "/", prefix).toUri();

    // Physical data format: s3://bucket/<radomUUID>.id=<objHandleId>"
    int lastIndex;
    if ((lastIndex = prefix.lastIndexOf(PHYSICAL_DATA_MARKER)) != -1) {
      String objectHandleId = prefix.substring(lastIndex + PHYSICAL_DATA_MARKER.length());
      LogicalObjectWritable writable = LogicalObjectWritable.fromS3Writable(sourceUri.toString());
      context.write(new Text(objectHandleId), writable);
      context.getCounter(FsckCounters.PARTIAL_RESTORE_S3_PHY_DATA_SCAN).increment(1L);
    } else if (prefix.lastIndexOf(INFO_SUFFIX) != -1) {
      // Operation log format: s3://<objHandleId>.info
      UriMetadataPair pair = download(bucketName, prefix);
      write(pair);
    }
  }

  private Optional<LogicalFileMetadataV2> deserializeOperationLog(String bucket, String prefix, InputStream is) throws IOException, InterruptedException {
    try {
      return Optional.ofNullable(ObjectMetadataSerialization.deserializeFromV2(is));
    } catch (Exception e) {
      LOG.warn("Exception caught while deserialization!", e);
      mosAdapter.write(
          S3_OUTPUT_NAME, DELETE_OBJECT, FsckUtils.mosS3ValueWritable(bucket, prefix), S3_OUTPUT_SUFFIX);
    }
    return Optional.empty();
  }

  @Override
  protected void cleanup(Context context) throws IOException, InterruptedException {
    super.cleanup(context);
    mosAdapter.close();
  }

  private UriMetadataPair download(String bucket, String prefix) {
    Optional<LogicalFileMetadataV2> metadata = Optional.empty();
    URI sourceUri = new Path("s3://" + bucket + "/", prefix).toUri();
    try (InputStream inputStream = s3Client.getObject(bucket, prefix).getObjectContent()) {
      metadata = deserializeOperationLog(bucket, prefix, inputStream);
    } catch (IOException ioe) {
      LOG.warn("Exception caught while getting operation log!", ioe);
    } catch (InterruptedException intrEx) {
      LOG.warn("Exception caught due to current thread being interrupted!", intrEx);
      Thread.currentThread().interrupt();
    }
    return UriMetadataPair.builder().metadata(metadata).uri(sourceUri).build();
  }

  /**
   * Each concrete implementation will have their own custom logic when handling the contents of operation log
   * @param uriMetadataPair pair containing URI and the deserialized contents of operation log
   * @throws IOException
   * @throws InterruptedException
   */
  protected abstract void write(UriMetadataPair uriMetadataPair) throws IOException, InterruptedException;
}
