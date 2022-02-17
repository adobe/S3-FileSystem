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
import com.adobe.s3fs.filesystemcheck.mapreduce.multioutputs.MultiOutputs;
import com.adobe.s3fs.filesystemcheck.mapreduce.multioutputs.MultiOutputsAdapter;
import com.adobe.s3fs.filesystemcheck.mapreduce.multioutputs.MultiOutputsFactory;
import com.adobe.s3fs.utils.aws.s3.S3Helpers;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
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
import static com.adobe.s3fs.filesystemcheck.mapreduce.FsckUtils.mosS3ValueWritable;
import static com.adobe.s3fs.filesystemcheck.mapreduce.FsckUtils.operationLogPrefix;

public class FileSystemCheckVerifyReducer
    extends Reducer<Text, LogicalObjectWritable, Text, Text> {

  private static final Logger LOG =
      LoggerFactory.getLogger(FileSystemCheckVerifyReducer.class);

  private static final Text DELETE_OBJECT = new Text("deleteObject");
  private static final String UPDATE_OP_LOG = "updateOpLog:";

  private final MultiOutputsFactory multiOutputsFactory;
  // S3 bucket
  private String bucket;
  // Mos adapter
  private MultiOutputs mosAdapter;

  // No-arg constructor needed by Hadoop Framework
  public FileSystemCheckVerifyReducer() {
    this.multiOutputsFactory = MultipleOutputs::new;
  }

  @VisibleForTesting
  public FileSystemCheckVerifyReducer(MultiOutputsFactory multiOutputsFactory) {
    this.multiOutputsFactory = Objects.requireNonNull(multiOutputsFactory);
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
  }

  @Override
  protected void reduce(Text key, Iterable<LogicalObjectWritable> values, Context context)
      throws IOException, InterruptedException {
    String objectHandleId = key.toString();
    PartialRestoreData data = buildPartialRestoreParameters(objectHandleId, values);
    Optional<PartialRestoreAction> partialRestoreOpt = partialRestoreActionFactoryV2(data, context);
    if (partialRestoreOpt.isPresent()) {
      PartialRestoreAction partialRestore = partialRestoreOpt.get();
      partialRestore.performAction(data, context);
    }
  }

  private Optional<PartialRestoreAction> partialRestoreActionFactoryV2(
      PartialRestoreData data, Context context) {
    if (data.isOpLogOnlyPresent()
        /*
         * Having a state where only physical data is available
         * means that there was a delete operation previously issued
         * where meta and oplog were deleted successfully, but the callback
         * which deletes the physical data failed.
         * The only action to take here in order to reconcile with meta (which is the source of truth)
         * is to issue a delete marker.
         */
        || data.isPhysicalDataOnlyPresent()
        /*
         * This case can happen due to a delete operation.
         * During a delete operation the first element to be deleted
         * is meta and then oplog is deleted and finally the callback which
         * deletes physical data is triggered. So, this case can happen
         * only if delete oplog operation and delete phy data will fail.
         */
        || data.isOpLogAndPhysicalDataPresent()
        || data.areAllPresent()) {
      return Optional.of(new PartialRestoreForAnyState());
    }

    if (data.isMetaOnlyPresent()) {
      // Having only meta without oplog or physical data is an invalid state
      // The create operation exposed by the file system will create first the oplog
      // Whereas the delete operation will delete first meta and then oplog and phy data
      context.getCounter(PARTIAL_RESTORE_INVALID_STATE_META_ONLY).increment(1L);
      return Optional.empty();
    }

    /*
     * The only valid state where we can only have operation log and meta store object without
     * physical data is in CREATE/COMMITTED state for operation log and version 1 (both meta & oplog).
     * For more context, during close of a stream an object transitions from
     * CREATE/COMMITTED state to UPDATE/COMMITTED state where is pointing to
     * a valid physical data. If during this transitions any failure happens
     * then meta will remain uncommitted, although oplog can drift.
     *
     */
    if (data.isOpLogAndMetaPresent()) {
      LogicalObjectWritable meta = data.metaStoreElement().get(); // NOSONAR
      if (meta.getVersion() > 1) {
        context.getCounter(PARTIAL_RESTORE_INVALID_STATE_OPLOG_AND_META_WITH_NO_PHY_DATA).increment(1L);
        if (LOG.isDebugEnabled()) {
          LOG.debug("Oplog and meta present {} (object handle id {}), but no phy data", meta, data.objectHandleId());
        }
        return Optional.empty();
      } else {
        return Optional.of(new PartialRestoreForAnyState());
      }
    }

    if (data.isMetaAndPhysicalDataPresent()) {
      // There is no operation exposed by the file-system which
      // deletes first the oplog. Any operation involves either creating
      // the oplog first or deleting it last. Hence we mark this state as invalid
      // and increment a counter
      context.getCounter(PARTIAL_RESTORE_INVALID_STATE_META_AND_PHY_DATA).increment(1L);
      return Optional.empty();
    }

    throw new IllegalStateException("Unrecognized numElements!");
  }

  private PartialRestoreData buildPartialRestoreParameters(
      String objectHandleId, Iterable<LogicalObjectWritable> values) {
    List<LogicalObjectWritable> fromS3List = new ArrayList<>();
    LogicalObjectWritable fromOpLog = null;
    LogicalObjectWritable fromMetaStore = null;

    for (LogicalObjectWritable value : values) {
      SourceType type = value.getSourceType();
      switch (type) {
        case FROM_OPLOG:
          fromOpLog = LogicalObjectWritable.copyOf(value);
          break;
        case FROM_METASTORE:
          fromMetaStore = LogicalObjectWritable.copyOf(value);
          break;
        case FROM_S3:
          fromS3List.add(LogicalObjectWritable.copyOf(value));
          break;
        default:
          LOG.warn("Unknown type {}. Will skip it", type);
          break;
      }
    }

    return PartialRestoreData.builder()
        .objectHandleId(objectHandleId)
        .opLogElement(Optional.ofNullable(fromOpLog))
        .metaStoreElement(Optional.ofNullable(fromMetaStore))
        .phyDataElements(Collections.unmodifiableList(fromS3List))
        .build();
  }

  /**
   * Write S3 delete marker for operation log
   * @param objectHandleId
   * @throws IOException
   * @throws InterruptedException
   */
  private void writeDeleteMarkerForOpLog(String objectHandleId)
      throws IOException, InterruptedException {
    mosAdapter.write(
        S3_OUTPUT_NAME,
        DELETE_OBJECT,
        mosS3ValueWritable(bucket, operationLogPrefix(objectHandleId)),
        S3_OUTPUT_SUFFIX);
  }

  /**
   * Write Operation log update marker
   * @param newOpLog
   * @param objectHandleId
   * @throws IOException
   * @throws InterruptedException
   */
  private void writeUpdateMarkerForOpLog(LogicalObjectWritable newOpLog, String objectHandleId)
      throws IOException, InterruptedException {
    Text cmdTypeAndPrefix = new Text(UPDATE_OP_LOG + operationLogPrefix(objectHandleId));
    mosAdapter.write(OPLOG_OUTPUT_NAME, cmdTypeAndPrefix, newOpLog, OPLOG_OUTPUT_SUFFIX);
  }

  /**
   * Write S3 delete marker for physical data
   *
   * @param phyData
   * @throws IOException
   * @throws InterruptedException
   */
  private void writeDeleteMarkerForPhysicalData(List<LogicalObjectWritable> phyData)
      throws IOException, InterruptedException {
    List<String> prefixes =
        phyData.stream()
            .map(LogicalObjectWritable::getPhysicalPath)
            .map(S3Helpers::getPrefix)
            .collect(Collectors.toList());
    for (String prefix : prefixes) {
      mosAdapter.write(
          S3_OUTPUT_NAME, DELETE_OBJECT, mosS3ValueWritable(bucket, prefix), S3_OUTPUT_SUFFIX);
    }
  }

  /**
   * Verify and update operation log if it isn't in sync with the meta store object
   *
   * @param opLog Operation log
   * @param meta Meta Store object
   * @param objectHandleId Common objectHandleId
   * @param context MR context
   * @throws IOException
   * @throws InterruptedException
   */
  private void updateAndVerifyOpLog(
      LogicalObjectWritable opLog,
      LogicalObjectWritable meta,
      String objectHandleId,
      Context context)
      throws IOException, InterruptedException {
    if (opLog.getVersion() == meta.getVersion() && !opLog.isPendingState()) {
      // In this case the op log is fine having
      // same version as meta object and it's not in pending state
      context.getCounter(PARTIAL_RESTORE_OP_LOG_AND_META_OK).increment(1L);
    } else { // either opLog.getVersion() != meta.getVersion() or opLog.isPendingState() == true
      // This means that operation log
      // hasn't transitioned to COMMITTED state successfully (e.g.: opLog.isPendingState() == true)
      // Or the operation log has drifted one version away from meta object it needs
      // to be rolled back to same earlier version like meta (e.g.: opLog.getVersion() !=
      // meta.getVersion())
      LogicalObjectWritable updateOpLog =
          meta.withSourceType(SourceType.FROM_OPLOG).withPendingState(false);
      writeUpdateMarkerForOpLog(updateOpLog, objectHandleId);
      context.getCounter(PARTIAL_RESTORE_OP_LOG_UPDATE).increment(1L);
    }
  }

  /**
   * @param s3DataList List of S3 data
   * @param meta Meta store entry
   * @return The current LogicalObjectWritable from s3DataList which has the same prefix for its
   *     physical path as the prefix from meta's physical path
   */
  private Optional<LogicalObjectWritable> findCurrentPhyPath(
      List<LogicalObjectWritable> s3DataList, LogicalObjectWritable meta) {
    Preconditions.checkNotNull(meta); // NOSONAR
    Preconditions.checkState(s3DataList != null && !s3DataList.isEmpty());

    if (UNCOMMITED_PATH_MARKER.equals(meta.getPhysicalPath())) {
      return Optional.empty();
    }

    String currentPhyPrefix = S3Helpers.getPrefix(meta.getPhysicalPath());
    /*
     * The expectations are to find exactly one active S3 physical data. The active
     * one is pointed out by the physicalPath stored in meta. However, I am extra
     * cautious and also considering the case where in s3DataList there can be found
     * multiple active S3 physical data and throw an exception
     */
    return s3DataList.stream()
        .filter(s3Data -> S3Helpers.getPrefix(s3Data.getPhysicalPath()).equals(currentPhyPrefix))
        .reduce(
            (a, b) -> {
              throw new IllegalStateException(
                  "Searched list " + s3DataList + "; Multiple elements: " + a + ", " + b);
            });
  }

  // Interface for performing various actions
  // with regards to partial restore and
  // to which elements are available
  private interface PartialRestoreAction {
    void performAction(PartialRestoreData data, Context context)
        throws IOException, InterruptedException;
  }

  private class PartialRestoreForAnyState implements PartialRestoreAction {

    private void reconcileOplogAgainstMeta(PartialRestoreData data, Context context)
        throws IOException, InterruptedException {
      Optional<LogicalObjectWritable> metaOpt = data.metaStoreElement();
      Optional<LogicalObjectWritable> oplogOpt = data.opLogElement();

      if (metaOpt.isPresent()) {
        LogicalObjectWritable meta = metaOpt.get();
        if (oplogOpt.isPresent()) {
          LogicalObjectWritable oplog = oplogOpt.get();
          // Write oplog update marker (if necessary)
          updateAndVerifyOpLog(oplog, meta, data.objectHandleId(), context);
        } else {
          // Invalid state because there is no operation
          // which would have allowed to have meta present, but
          // oplog missing. For example, for create type operations
          // oplog is the first to be created, while for delete type operations
          // oplog is deleted only if meta is successfully removed before.
          context.getCounter(PARTIAL_RESTORE_INVALID_STATE_OPLOG_AND_META).increment(1L);
          if (LOG.isDebugEnabled()) {
            LOG.debug("Oplog missing, but meta present {}", meta);
          }
        }
      } else {
        // Meta is missing, thus delete oplog (only if present)
        if (oplogOpt.isPresent()) {
          writeDeleteMarkerForOpLog(data.objectHandleId());
          context.getCounter(PARTIAL_RESTORE_META_MISSING_WITH_OPLOG).increment(1L);
        }
      }
    }

    private void reconcilePresentPhyDataAgainstPresentMeta(PartialRestoreData data, Context context) throws IOException, InterruptedException {
      LogicalObjectWritable meta = data.metaStoreElement().get(); // NOSONAR
      List<LogicalObjectWritable> phyData = data.phyDataElements();
      Optional<LogicalObjectWritable> active = findCurrentPhyPath(phyData, meta);
      // Active is present
      // Delete any additional leftovers
      if (active.isPresent()) {
        String activePhyPath = active.get().getPhysicalPath();
        List<LogicalObjectWritable> phyDataWithoutActive =
            phyData.stream()
                .filter(phyDataElem -> !phyDataElem.getPhysicalPath().equals(activePhyPath))
                .collect(Collectors.toList());
        writeDeleteMarkerForPhysicalData(phyDataWithoutActive);
        context
            .getCounter(PARTIAL_RESTORE_VALID_STATE_META_COMMITTED_WITH_DELETE_INACTIVE_PHY)
            .increment(phyDataWithoutActive.size());
      } else {
        // Any meta object with a version grater than 1
        // needs to have physical data committed.
        // If there is a meta with a version grater than 1,
        // but with no active physical data when we run into
        // an anomaly situation
        if (meta.getVersion() > 1) {
          context
              .getCounter(PARTIAL_RESTORE_INVALID_STATE_META_COMMITTED_AND_NO_PHY_DATA)
              .increment(1L);
        } else {
          // If meta is at version == 1
          // it means that meta still has physical path
          // uncommitted. Thus, to reconcile things
          // we proceed with deleting all physical data
          Preconditions.checkState(meta.getVersion() == 1);
          Preconditions.checkState(UNCOMMITED_PATH_MARKER.equals(meta.getPhysicalPath()));
          writeDeleteMarkerForPhysicalData(phyData);
        }
      }
    }

    private void reconcilePhyDataAgainstMeta(PartialRestoreData data, Context context)
        throws IOException, InterruptedException {
      Optional<LogicalObjectWritable> metaOpt = data.metaStoreElement();
      List<LogicalObjectWritable> phyData = data.phyDataElements();

      if (metaOpt.isPresent()) {
        LogicalObjectWritable meta = metaOpt.get();

        if (phyData.isEmpty()) {
          if (meta.getVersion() > 1) {
            // Any meta object with a version grater than 1
            // needs to have physical data committed.
            // If there is a meta with a version grater than 1,
            // but with no active physical data when we run into
            // an anomaly situation
            context
                .getCounter(PARTIAL_RESTORE_INVALID_STATE_META_COMMITTED_AND_NO_PHY_DATA)
                .increment(1L);
          } else {
            Preconditions.checkState(meta.getVersion() == 1);
            Preconditions.checkState(UNCOMMITED_PATH_MARKER.equals(meta.getPhysicalPath()));
            context
                .getCounter(PARTIAL_RESTORE_VALID_STATE_META_UNCOMMITTED_AND_NO_PHY_DATA)
                .increment(1L);
          }
        } else {
          reconcilePresentPhyDataAgainstPresentMeta(data, context);
        }
      } else {
        // Meta is missing so proceed with deleting phy data
        if (!phyData.isEmpty()) {
          writeDeleteMarkerForPhysicalData(phyData);
          context
              .getCounter(PARTIAL_RESTORE_META_MISSING_WITH_PHY_DATA)
              .increment(phyData.size());
        }
      }
    }

    @Override
    public void performAction(PartialRestoreData data, Context context)
        throws IOException, InterruptedException {

      // Reconcile oplog against meta
      reconcileOplogAgainstMeta(data, context);

      // Reconcile phy data against meta
      reconcilePhyDataAgainstMeta(data, context);
    }
  }
}
