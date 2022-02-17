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
import com.adobe.s3fs.filesystemcheck.mapreduce.multioutputs.MultiOutputsFactory;
import com.adobe.s3fs.filesystemcheck.s3.S3ClientFactory;
import com.adobe.s3fs.filesystemcheck.utils.UriMetadataPair;
import com.adobe.s3fs.metastore.api.OperationLogEntryState;
import com.adobe.s3fs.operationlog.LogicalFileMetadataV2;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.services.s3.AmazonS3Client;
import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.util.Optional;

import static com.adobe.s3fs.filesystemcheck.mapreduce.FsckCounters.PARTIAL_RESTORE_S3_OPLOG_SCAN;

public class FileSystemCheckPartialS3Mapper extends AbstractFsckS3Mapper {

  public FileSystemCheckPartialS3Mapper() {
    super(
        (retryPolicy, maxConnections) ->
            AmazonS3Client.builder()
                .withClientConfiguration(
                    new ClientConfiguration()
                        .withRetryPolicy(retryPolicy)
                        .withMaxConnections(maxConnections))
                .build(),
        MultipleOutputs::new);
  }

  @VisibleForTesting
  public FileSystemCheckPartialS3Mapper(
      S3ClientFactory s3ClientFactory, MultiOutputsFactory mosFactory) {
    super(s3ClientFactory, mosFactory);
  }

  @Override
  protected void write(UriMetadataPair uriMetadataPair) throws IOException, InterruptedException {
    Optional<LogicalFileMetadataV2> optional = uriMetadataPair.metadata();
    if (!optional.isPresent()) {
      return;
    }

    LogicalFileMetadataV2 logicalFileMetadata = optional.get();
    if (!logicalFileMetadata.getLogicalPath().startsWith(rootPath)) {
      return;
    }

    LogicalObjectWritable writable =
        new LogicalObjectWritable.Builder()
            .withSize(logicalFileMetadata.getSize())
            .withCreationTime(logicalFileMetadata.getCreationTime())
            .isDirectory(false)
            .isPendingState(logicalFileMetadata.getState() == OperationLogEntryState.PENDING)
            .withPhysicalPath(logicalFileMetadata.getPhysicalPath())
            .withLogicalPath(logicalFileMetadata.getLogicalPath())
            .withVersion(logicalFileMetadata.getVersion())
            .withSourceType(SourceType.FROM_OPLOG)
            .build();

    context.write(new Text(logicalFileMetadata.getId()), writable);
    context.getCounter(PARTIAL_RESTORE_S3_OPLOG_SCAN).increment(1L);
  }
}
