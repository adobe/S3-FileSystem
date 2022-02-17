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
import com.adobe.s3fs.metastore.api.VersionedObjectHandle;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

import static com.adobe.s3fs.filesystemcheck.mapreduce.FsckCounters.PARTIAL_RESTORE_META_SCAN;

public class MetadataStorePartialRestoreMapper
    extends Mapper<Void, VersionedObjectHandle, Text, LogicalObjectWritable> {

  @Override
  protected void map(Void key, VersionedObjectHandle value, Context context)
      throws IOException, InterruptedException {
    if (value.metadata().isDirectory()) {
      // Skip directories because these type of meta object
      // doesn't store any operation log
      return;
    }

    Text outputKey = new Text(value.id().toString());

    LogicalObjectWritable outputValue = new LogicalObjectWritable.Builder()
        .withSourceType(SourceType.FROM_METASTORE)
        .withPhysicalPath(value.metadata().getPhysicalPath().get()) // NOSONAR
        .withLogicalPath(value.metadata().getKey().toString())
        .withSize(value.metadata().getSize())
        .withCreationTime(value.metadata().getCreationTime())
        .withVersion(value.version())
        .isDirectory(false)
        .isPendingState(false) // don't care
        .build();

    context.write(outputKey, outputValue);
    context.getCounter(PARTIAL_RESTORE_META_SCAN).increment(1L);
  }
}
