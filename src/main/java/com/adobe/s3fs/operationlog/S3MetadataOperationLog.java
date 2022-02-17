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

package com.adobe.s3fs.operationlog;

import com.adobe.s3fs.common.runtime.FileSystemRuntime;
import com.adobe.s3fs.metastore.api.MetadataOperationLogExtended;
import com.adobe.s3fs.metastore.api.OperationLogEntryState;
import com.adobe.s3fs.metastore.api.ObjectOperationType;
import com.adobe.s3fs.metastore.api.VersionedObjectHandle;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

public class S3MetadataOperationLog implements MetadataOperationLogExtended {

  private final AmazonS3 amazonS3;
  private final FileSystemRuntime runtime;
  private final String bucket;

  public static final String INFO_SUFFIX = ".info";
  private static final Logger LOG = LoggerFactory.getLogger(S3MetadataOperationLog.class);

  public S3MetadataOperationLog(AmazonS3 amazonS3, String bucket, FileSystemRuntime runtime) {
    this.amazonS3 = Preconditions.checkNotNull(amazonS3);
    this.runtime = Preconditions.checkNotNull(runtime);
    this.bucket = Preconditions.checkNotNull(bucket);
  }

  private void putS3Object(String bucket, String key, ZeroCopyByteArrayOutputStream outputStream) {
    ObjectMetadata s3ObjectMetadata = new ObjectMetadata();
    s3ObjectMetadata.setContentLength(outputStream.size());
    amazonS3.putObject(bucket,
        key,
        new ByteArrayInputStream(outputStream.zeroCopyBuffer()),
        s3ObjectMetadata);
  }

  private boolean writeCommittedOpLogToStorage(
      com.adobe.s3fs.metastore.api.ObjectMetadata objectMetadata,
      String bucket,
      String key,
      UUID objectHandleId,
      int version,
      ObjectOperationType opType) {
    try {
      ZeroCopyByteArrayOutputStream outputStream = new ZeroCopyByteArrayOutputStream();
      ObjectMetadataSerialization.serializeObjectMetadataToV2(
          objectMetadata, objectHandleId, version, OperationLogEntryState.COMMITTED, opType, outputStream);
      putS3Object(bucket, key, outputStream);
      return true;
    } catch (Exception e) {
      LOG.error("Error persisting " + bucket + "/" + key, e);
      return false;
    }
  }

  private boolean writeToStorage(VersionedObjectHandle object,
                                 String  bucket,
                                 String key,
                                 OperationLogEntryState state,
                                 ObjectOperationType type) {
    try {
      ZeroCopyByteArrayOutputStream outputStream = new ZeroCopyByteArrayOutputStream();
      ObjectMetadataSerialization.serializeToV2(object, state, type, outputStream);
      putS3Object(bucket, key, outputStream);
      return true;
    } catch (Exception e) {
      LOG.error("Error persisting " + bucket + "/" + key, e);
      return false;
    }
  }

  @Override
  public void close() throws IOException {
    amazonS3.shutdown();
  }

  @Override
  public Optional<LogEntryHandle> logCreateOperation(VersionedObjectHandle object) {
    if (object.metadata().isDirectory()) {
      throw new IllegalArgumentException();
    }

    String key = entryKey(object);

    if (!writeToStorage(object, bucket, key, OperationLogEntryState.PENDING, ObjectOperationType.CREATE)) {
      return Optional.empty();
    }

    LogEntryHandle logEntryHandle = new LogEntryHandle() {
      @Override
      public boolean rollback() {
        return deleteFromS3(bucket, key);
      }

      @Override
      public boolean commit() {
        return writeToStorage(object, bucket, key, OperationLogEntryState.COMMITTED, ObjectOperationType.CREATE);
      }
    };

    return Optional.of(logEntryHandle);
  }

  @Override
  public CompletableFuture<Optional<LogEntryHandle>> logCreateOperationAsync(VersionedObjectHandle object) {
    return runtime.async(() -> logCreateOperation(object));
  }

  @Override
  public Optional<LogEntryHandle> logUpdateOperation(VersionedObjectHandle sourceObject, VersionedObjectHandle destinationObject) {
    if (sourceObject.metadata().isDirectory()) {
      throw new IllegalArgumentException();
    }

    String key = entryKey(sourceObject);

    if (!writeToStorage(destinationObject, bucket, key, OperationLogEntryState.PENDING, ObjectOperationType.UPDATE)) {
      return Optional.empty();
    }

    LogEntryHandle logEntryHandle = new LogEntryHandle() {
      @Override
      public boolean rollback() {
        ObjectOperationType type = previousTypeFromObject(sourceObject);
        return writeToStorage(sourceObject, bucket, key, OperationLogEntryState.COMMITTED, type);
      }

      @Override
      public boolean commit() {
        return writeToStorage(destinationObject, bucket, key, OperationLogEntryState.COMMITTED, ObjectOperationType.UPDATE);
      }
    };

    return Optional.of(logEntryHandle);
  }

  @Override
  public CompletableFuture<Optional<LogEntryHandle>> logUpdateOperationAsync(VersionedObjectHandle sourceObject, VersionedObjectHandle destinationObject) {
    return runtime.async(() -> logUpdateOperation(sourceObject, destinationObject));
  }

  @Override
  public Optional<LogEntryHandle> logDeleteOperation(VersionedObjectHandle object) {
    String key = entryKey(object);

    if (!writeToStorage(object, bucket, key, OperationLogEntryState.PENDING, ObjectOperationType.DELETE)) {
      return Optional.empty();
    }

    return Optional.of(new LogEntryHandle() {
      @Override
      public boolean rollback() {
        ObjectOperationType type = previousTypeFromObject(object);

        return writeToStorage(object, bucket, key, OperationLogEntryState.COMMITTED, type);
      }

      @Override
      public boolean commit() {
        if (!writeToStorage(object, bucket, key, OperationLogEntryState.COMMITTED, ObjectOperationType.DELETE)) {
          return false;
        }
        return deleteFromS3(bucket, key);
      }
    });
  }

  @Override
  public CompletableFuture<Optional<LogEntryHandle>> logDeleteOperationAsync(VersionedObjectHandle object) {
    return runtime.async(() -> logDeleteOperation(object));
  }

  private String entryKey(VersionedObjectHandle objectHandle) {
    return String.format("%s%s", objectHandle.id().toString(), INFO_SUFFIX);
  }

  @Override
  public boolean amendObject(
      com.adobe.s3fs.metastore.api.ObjectMetadata objMetadata,
      UUID objId,
      int version,
      ObjectOperationType opType) {
    Preconditions.checkNotNull(objMetadata); // NOSONAR
    Preconditions.checkNotNull(objId); // NOSONAR
    Preconditions.checkNotNull(opType); // NOSONAR

    String prefix = String.format("%s%s", objId.toString(), INFO_SUFFIX);

    return writeCommittedOpLogToStorage(objMetadata, bucket, prefix, objId, version, opType);
  }

  private static ObjectOperationType previousTypeFromObject(VersionedObjectHandle objectHandle) {
    return objectHandle.version() == 1 ? ObjectOperationType.CREATE : ObjectOperationType.UPDATE;
  }

  private boolean deleteFromS3(String bucket, String key) {
    try {
      amazonS3.deleteObject(bucket, key);
      return true;
    } catch (Exception e) {
      LOG.error("Error deleting. Metadata is still stored at {}", key);
      LOG.error("Error:", e);
      return false;
    }
  }

  private static class ZeroCopyByteArrayOutputStream extends ByteArrayOutputStream {

    public byte[] zeroCopyBuffer() {
      return buf;
    }
  }
}
