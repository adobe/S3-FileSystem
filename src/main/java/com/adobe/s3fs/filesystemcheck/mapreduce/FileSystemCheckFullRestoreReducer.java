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
import com.adobe.s3fs.filesystemcheck.mapreduce.data.SourceType;
import com.adobe.s3fs.filesystemcheck.mapreduce.data.VersionedObjectWritable;
import com.adobe.s3fs.filesystemcheck.mapreduce.multioutputs.MultiOutputs;
import com.adobe.s3fs.filesystemcheck.mapreduce.multioutputs.MultiOutputsAdapter;
import com.adobe.s3fs.filesystemcheck.mapreduce.multioutputs.MultiOutputsFactory;
import com.adobe.s3fs.utils.aws.s3.S3Helpers;
import com.adobe.s3fs.metastore.internal.dynamodb.versioning.VersionedObject;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import static com.adobe.s3fs.filesystem.FileSystemImplementation.UNCOMMITED_PATH_MARKER;
import static com.adobe.s3fs.filesystemcheck.mapreduce.FileSystemMRJobConfig.*;
import static com.adobe.s3fs.filesystemcheck.mapreduce.FsckCounters.*;
import static com.adobe.s3fs.operationlog.S3MetadataOperationLog.INFO_SUFFIX;

public class FileSystemCheckFullRestoreReducer
    extends Reducer<Text, LogicalObjectWritable, Text, Text> {

  private static final Logger LOG =
      LoggerFactory.getLogger(FileSystemCheckFullRestoreReducer.class);
  private static final Text DELETE_OBJECT = new Text("deleteObject");
  private static final Text RESTORE_OBJECT = new Text("restoreObject");
  private static final Text OP_LOG = new Text("opLog");
  private static final Text PHY_DATA = new Text("phyData");
  private final MultiOutputsFactory multiOutputsFactory;

  // S3 bucket
  private String bucket;
  // Consumer of pending operation log
  private PendingStateConsumer pendingStateConsumer;
  // Mos adapter
  private MultiOutputs mosAdapter;

  // No-arg constructor needed by Hadoop Framework
  public FileSystemCheckFullRestoreReducer() {
    this.multiOutputsFactory = MultipleOutputs::new;
  }

  @VisibleForTesting
  public FileSystemCheckFullRestoreReducer(MultiOutputsFactory multiOutputsFactory) {
    this.multiOutputsFactory = Objects.requireNonNull(multiOutputsFactory);
  }

  private static String toOpLogEntry(String bucket, String objHandleId) {
    return String.format("s3://%s/%s", bucket, objHandleId + INFO_SUFFIX);
  }

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);

    String logicalRootPath = context.getConfiguration().get(LOGICAL_ROOT_PATH);
    Objects.requireNonNull(logicalRootPath);
    LOG.info("[Reducer]: Logical root path = {}", logicalRootPath);

    this.bucket = Objects.requireNonNull(S3Helpers.getBucket(logicalRootPath));
    LOG.info("[Reducer]: Bucket = {}", bucket);

    this.mosAdapter =
        MultiOutputsAdapter.createInstance(multiOutputsFactory.newMultipleOutputs(context));
    this.pendingStateConsumer = new RecordPendingStateConsumer(mosAdapter);
  }

  @Override
  protected void reduce(Text key, Iterable<LogicalObjectWritable> values, Context context)
      throws IOException, InterruptedException {
    String objectHandleId = key.toString();

    /*
     * We are using a list instead of a scalar value
     * because there might be situations where multiple
     * S3 physical data might exist for the same object handle id
     * (e.g: multiple create(overwrite=true) failures). However
     * only one element from this is list is the current active
     * physical data and this is pointed by the physical data
     * stored in the LogicalObjectWritable associated with the
     * operation log.
     */
    List<LogicalObjectWritable> fromS3List = new ArrayList<>();
    LogicalObjectWritable fromOpLog = null;

    for (LogicalObjectWritable value : values) {
      if (value.getSourceType() == SourceType.FROM_OPLOG) {
        fromOpLog = LogicalObjectWritable.copyOf(value);
      } else if (value.getSourceType() == SourceType.FROM_S3) {
        fromS3List.add(LogicalObjectWritable.copyOf(value));
      }
    }

    /*
     * Will write to a multiple output the operation log and physical data
     * corresponding to an object handle id whose operation log is in
     * pending state. Pending state isn't a "strong" enough state
     * (like committed) in order to restore the object from MetadataStore
     */
    if (!fromS3List.isEmpty() && fromOpLog != null && fromOpLog.isPendingState()) {
      pendingStateConsumer.consumePendingOpLog(fromS3List, fromOpLog, objectHandleId, context);
    } else if (!fromS3List.isEmpty() && fromOpLog != null) {
      // write to metastore
      restoreVersionedObject(fromS3List, fromOpLog, objectHandleId, context);
    } else if (fromOpLog != null) {
      // We have an op-log but there is no associated phy-data: we delete the op-log
      deleteOpLog(objectHandleId, context);
    } else if (!fromS3List.isEmpty()) {
      // We have an some phy-data entries but there is no associated op-log: we delete the orphaned
      // phy-data
      deleteAllS3Data(fromS3List, context);
    }
  }

  @Override
  protected void cleanup(Context context) throws IOException, InterruptedException {
    super.cleanup(context);
    mosAdapter.close();
  }

  /**
   * In case there is no operation log then it is safe to delete all remaining S3 physical data
   *
   * @param s3DataList List of S3 physical data from LogicalObjectWritable
   * @param context Context
   */
  private void deleteAllS3Data(List<LogicalObjectWritable> s3DataList, Context context)
      throws IOException, InterruptedException {
    Preconditions.checkState(s3DataList != null && !s3DataList.isEmpty());
    Preconditions.checkNotNull(context); // NOSONAR
    // Delete s3 data
    List<String> prefixes =
        s3DataList.stream()
            .map(LogicalObjectWritable::getPhysicalPath)
            .map(S3Helpers::getPrefix)
            .collect(Collectors.toList());
    for (String prefix : prefixes) {
      writeDeleteMarkerToS3Mos(bucket, prefix);
    }
    context.getCounter(NUM_PHYSICAL_DATA_DELETED).increment(prefixes.size());
  }

  /**
   * @param s3DataList S3 Physical data writables
   * @param opLog Operation log
   * @param objectHandleId UUID
   * @param context MR Context
   */
  private void restoreVersionedObject(
      List<LogicalObjectWritable> s3DataList,
      LogicalObjectWritable opLog,
      String objectHandleId,
      Context context)
      throws IOException, InterruptedException {
    Preconditions.checkState(s3DataList != null && !s3DataList.isEmpty());
    Preconditions.checkNotNull(opLog); // NOSONAR
    Preconditions.checkState(!Strings.isNullOrEmpty(objectHandleId));
    Preconditions.checkNotNull(context); // NOSONAR
    Optional<LogicalObjectWritable> activePhyPath = findCurrentPhyPath(s3DataList, opLog);
    if (activePhyPath.isPresent()) {
      writeRestoreObjectMarker(FsckUtils.writableToVersionedObject(opLog, objectHandleId));
      context.getCounter(NUM_OBJECT_META_RESTORE).increment(1L);
      deleteInactivePhyData(s3DataList, activePhyPath.get(), context);
    } else {
      writeNoActiveS3Data(s3DataList, objectHandleId);
      context.getCounter(NUM_NO_ACTIVE_PHYSICAL_DATA).increment(1L);
    }
  }

  private void writeNoActiveS3Data(List<LogicalObjectWritable> s3DataList, String objHandleId) throws IOException, InterruptedException {
    Preconditions.checkState(s3DataList != null && !s3DataList.isEmpty());
    Preconditions.checkState(objHandleId != null && !objHandleId.isEmpty());
    for (LogicalObjectWritable s3Data : s3DataList) {
      mosAdapter.write(
          NO_ACTIVE_OUTPUT_NAME,
          PHY_DATA,
          new Text(s3Data.getPhysicalPath()),
          NO_ACTIVE_OUTPUT_SUFFIX);
    }
    mosAdapter.write(
        NO_ACTIVE_OUTPUT_NAME,
        OP_LOG,
        new Text(toOpLogEntry(bucket, objHandleId)),
        NO_ACTIVE_OUTPUT_SUFFIX);
  }

  /**
   * Delete the operation log associated with objectHandleId
   *
   * @param objectHandleId Id
   * @param context Context
   */
  private void deleteOpLog(String objectHandleId, Context context)
      throws IOException, InterruptedException {
    Preconditions.checkState(!Strings.isNullOrEmpty(objectHandleId));
    Preconditions.checkNotNull(context); // NOSONAR
    writeDeleteMarkerToS3Mos(bucket, String.format("%s%s", objectHandleId, INFO_SUFFIX));
    context.getCounter(NUM_OP_LOG_DELETED_WITH_NO_PHYSICAL_DATA).increment(1L);
  }

  private void writeRestoreObjectMarker(VersionedObject versionedObject)
      throws IOException, InterruptedException {
    mosAdapter.write(
        METASTORE_OUTPUT_NAME,
        RESTORE_OBJECT,
        new VersionedObjectWritable(versionedObject),
        METASTORE_OUTPUT_SUFFIX);
  }

  private void writeDeleteMarkerToS3Mos(String bucket, String prefix)
      throws IOException, InterruptedException {
    mosAdapter.write(
        S3_OUTPUT_NAME, DELETE_OBJECT, FsckUtils.mosS3ValueWritable(bucket, prefix), S3_OUTPUT_SUFFIX);
  }

  /**
   * @param s3DataList List of S3 data
   * @param opLog Operation log entry
   * @return The current LogicalObjectWritable from fromS3Data which has the same prefix for its
   *     physical path as the prefix from fromOpLog
   */
  private Optional<LogicalObjectWritable> findCurrentPhyPath(
      List<LogicalObjectWritable> s3DataList, LogicalObjectWritable opLog) {
    Preconditions.checkNotNull(opLog); // NOSONAR
    Preconditions.checkState(s3DataList != null && !s3DataList.isEmpty());

    if (opLog.getPhysicalPath().equals(UNCOMMITED_PATH_MARKER)) {
      return Optional.empty();
    }

    String currentPhyPrefix = S3Helpers.getPrefix(opLog.getPhysicalPath());
    /*
     * The expectations are to find exactly one active S3 physical data. The active
     * one is pointed out by the physicalPath stored in opLog. However, I am extra
     * cautious and also considering the case where in s3DataList there can be found
     * multiple active S3 physical data and throw an exception
     */
    return s3DataList.stream()
        .filter(s3Data -> S3Helpers.getPrefix(s3Data.getPhysicalPath()).equals(currentPhyPrefix))
        .reduce(
            (a, b) -> {
              throw new IllegalStateException("Searched list " + s3DataList + "; Multiple elements: " + a + ", " + b);
            });
  }

  /**
   * Emits delete marker for inactive s3 data
   *
   * @param s3DataList List of physical s3 data
   * @param activeS3Data Active s3 data which belongs to s3DataList
   * @param context MR context
   * @throws IOException
   * @throws InterruptedException
   */
  private void deleteInactivePhyData(
      List<LogicalObjectWritable> s3DataList, LogicalObjectWritable activeS3Data, Context context)
      throws IOException, InterruptedException {
    Preconditions.checkState(s3DataList != null && !s3DataList.isEmpty());
    Preconditions.checkState(activeS3Data != null);

    String activePhyPrefix = S3Helpers.getPrefix(activeS3Data.getPhysicalPath());

    List<String> inactiveS3DataPrefixes =
        s3DataList.stream()
            .map(s3Data -> S3Helpers.getPrefix(s3Data.getPhysicalPath()))
            .filter(prefix -> !prefix.equals(activePhyPrefix))
            .collect(Collectors.toList());

    for (String inactiveS3DataPrefix : inactiveS3DataPrefixes) {
      writeDeleteMarkerToS3Mos(bucket, inactiveS3DataPrefix);
    }
    context.getCounter(NUM_INACTIVE_PHYSICAL_DATA_DELETED).increment(inactiveS3DataPrefixes.size());
  }

  private interface PendingStateConsumer {
    /**
     * @param s3DataList List of writables corresponding to S3 physical data
     * @param opLogEntry writable corresponding to operation log entry
     * @param objHandleId Object handle id
     * @param context MR context
     * @throws IOException
     * @throws InterruptedException
     */
    void consumePendingOpLog(
        List<LogicalObjectWritable> s3DataList,
        LogicalObjectWritable opLogEntry,
        String objHandleId,
        Context context)
        throws IOException, InterruptedException;
  }

  /** For pending operation logs we will write operation logs in order to review them afterwards */
  private class RecordPendingStateConsumer implements PendingStateConsumer {
    // Multiple outputs
    private final MultiOutputs mosAdapter;

    private RecordPendingStateConsumer(MultiOutputs mosAdapter) {
      this.mosAdapter = Preconditions.checkNotNull(mosAdapter);
    }

    @Override
    public void consumePendingOpLog(
        List<LogicalObjectWritable> s3DataList,
        LogicalObjectWritable opLogEntry,
        String objHandleId,
        Context context)
        throws IOException, InterruptedException {
      Preconditions.checkNotNull(context); // NOSONAR
      Optional<LogicalObjectWritable> s3DataOptional = findCurrentPhyPath(s3DataList, opLogEntry);
      if (s3DataOptional.isPresent()) {
        mosAdapter.write(
            PENDING_OUTPUT_NAME,
            new Text(s3DataOptional.get().getPhysicalPath()),
            new Text(toOpLogEntry(bucket, objHandleId)),
            PENDING_OUTPUT_SUFFIX);
        context.getCounter(NUM_PENDING_OP_LOG).increment(1L);
        deleteInactivePhyData(s3DataList, s3DataOptional.get(), context);
      } else {
        writeNoActiveS3Data(s3DataList, objHandleId);
        context.getCounter(NUM_NO_ACTIVE_PHYSICAL_DATA).increment(1L);
      }
    }
  }
}
