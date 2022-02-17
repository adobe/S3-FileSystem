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

package com.adobe.s3fs.filesystemcheck.cmdloader;

import com.adobe.s3fs.filesystemcheck.mapreduce.data.LogicalObjectWritable;
import com.adobe.s3fs.filesystemcheck.mapreduce.data.SourceType;
import com.adobe.s3fs.metastore.api.MetadataOperationLogExtended;
import com.adobe.s3fs.metastore.api.ObjectMetadata;
import com.adobe.s3fs.metastore.api.ObjectOperationType;
import com.adobe.s3fs.utils.exceptions.UncheckedException;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.UUID;

import static com.adobe.s3fs.filesystem.FileSystemImplementation.UNCOMMITED_PATH_MARKER;

public class OplogUpdateCommand implements FsckCommand {

  private static final Logger LOG = LoggerFactory.getLogger(OplogUpdateCommand.class);

  private final MetadataOperationLogExtended oplogExtended;
  private final LogicalObjectWritable newOpLog;
  private final String objectHandleId;

  private OplogUpdateCommand(
      MetadataOperationLogExtended oplogExtended,
      LogicalObjectWritable newOpLog,
      String objectHandleId) {
    this.oplogExtended = Preconditions.checkNotNull(oplogExtended);
    this.newOpLog = Preconditions.checkNotNull(newOpLog);
    Preconditions.checkState(!Strings.isNullOrEmpty(objectHandleId));
    this.objectHandleId = objectHandleId;
  }

  public static OplogUpdateCommand newInstance(
      MetadataOperationLogExtended oplogExtended,
      LogicalObjectWritable newOpLog,
      String objectHandleId) {
    return new OplogUpdateCommand(oplogExtended, newOpLog, objectHandleId);
  }

  @Override
  public void execute() {
    // Sanity checks
    Preconditions.checkState(SourceType.FROM_OPLOG == newOpLog.getSourceType());
    Preconditions.checkState(newOpLog.getVersion() >= 1);
    Preconditions.checkState(!newOpLog.isPendingState()); // Be sure to have a committed state


    ObjectOperationType type = ObjectOperationType.CREATE;
    if (newOpLog.getVersion() > 1) {
      // DELETE can't be because of the following reasoning:
      // If a delete operation is issued and the first call to operation log
      // which stores the PENDING marker fails, then all elements will be present
      // and op log is still in sync with meta. If storing PENDING succeeds, but
      // delete against Meta Store fails, then op log will be reverted to the previous state
      // which is either UPDATE or CREATE (depending on the current version number).
      // If delete against Meta Store succeeds and subsequent operations fail, then
      // there will be 2 left overs (op log and phy data), which are processed by a different flow
      // in partial restore logic (meaning that S3 delete marker will be issued)
      type = ObjectOperationType.UPDATE;
    }



    ObjectMetadata objectMetadata =
        ObjectMetadata.builder()
            .key(new Path(newOpLog.getLogicalPath()))
            .isDirectory(newOpLog.isDirectory())
            .size(newOpLog.getSize())
            .creationTime(newOpLog.getCreationTime())
            .physicalPath(
                newOpLog.isDirectory() ? Optional.empty() : Optional.of(newOpLog.getPhysicalPath()))
            .physicalDataCommitted(!newOpLog.getPhysicalPath().equals(UNCOMMITED_PATH_MARKER))
            .build();

    if (!oplogExtended.amendObject(
        objectMetadata, UUID.fromString(objectHandleId), newOpLog.getVersion(), type)) {
      throw new UncheckedException("Amend operation log object failed!");
    }
  }

  @Override
  public String toString() {
    return "oplogExtended.amendObject(" + newOpLog  + "," + objectHandleId + ")"; // NOSONAR
  }
}
