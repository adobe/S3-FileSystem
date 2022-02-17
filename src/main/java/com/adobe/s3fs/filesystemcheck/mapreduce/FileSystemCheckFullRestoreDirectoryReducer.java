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

import com.adobe.s3fs.filesystemcheck.mapreduce.data.VersionedObjectWritable;
import com.adobe.s3fs.filesystemcheck.mapreduce.multioutputs.MultiOutputs;
import com.adobe.s3fs.filesystemcheck.mapreduce.multioutputs.MultiOutputsAdapter;
import com.adobe.s3fs.filesystemcheck.mapreduce.multioutputs.MultiOutputsFactory;
import com.adobe.s3fs.metastore.api.ObjectMetadata;
import com.adobe.s3fs.metastore.internal.dynamodb.versioning.VersionedObject;
import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

import static com.adobe.s3fs.filesystemcheck.mapreduce.FileSystemMRJobConfig.*;
import static com.adobe.s3fs.filesystemcheck.mapreduce.FsckCounters.NUM_META_RESTORE_DIRECTORIES;

public class FileSystemCheckFullRestoreDirectoryReducer
    extends Reducer<Text, NullWritable, NullWritable, NullWritable> {

  private static final Logger LOG =
      LoggerFactory.getLogger(FileSystemCheckFullRestoreDirectoryReducer.class);
  private static final Text RESTORE_OBJECT = new Text("restoreObject");
  // Mos factory
  private final MultiOutputsFactory multiOutputsFactory;
  // Creation time
  private long creationTime;
  // Mos adapter
  private MultiOutputs mosAdapter;

  public FileSystemCheckFullRestoreDirectoryReducer() {
    this.multiOutputsFactory = MultipleOutputs::new;
  }

  @VisibleForTesting
  public FileSystemCheckFullRestoreDirectoryReducer(MultiOutputsFactory multiOutputsFactory) {
    this.multiOutputsFactory = Objects.requireNonNull(multiOutputsFactory);
  }

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);

    Objects.requireNonNull(context.getConfiguration().get(LOGICAL_ROOT_PATH));
    String bucket = new Path(context.getConfiguration().get(LOGICAL_ROOT_PATH)).toUri().getHost();
    LOG.info("[FullRestore Reduce Phase2]: bucket = {}", bucket);

    creationTime = Instant.now().getEpochSecond();
    LOG.info("[FullRestore Reduce Phase2]: creationTime = {}", creationTime);

    mosAdapter =
        MultiOutputsAdapter.createInstance(multiOutputsFactory.newMultipleOutputs(context));
  }

  @Override
  protected void reduce(Text key, Iterable<NullWritable> values, Context context)
      throws IOException, InterruptedException {

    Path logicalPath = new Path(key.toString());
    ObjectMetadata metadataToCreate =
        ObjectMetadata.builder()
            .key(logicalPath)
            .isDirectory(true)
            .creationTime(creationTime)
            .size(0)
            .physicalPath(Optional.empty())
            .build();

    VersionedObject versionedObject =
        VersionedObject.builder()
            .metadata(metadataToCreate)
            .id(UUID.randomUUID())
            .version(1)
            .build();

    mosAdapter.write(
        METASTORE_OUTPUT_NAME,
        RESTORE_OBJECT,
        new VersionedObjectWritable(versionedObject),
        METASTORE_OUTPUT_SUFFIX);
    context.getCounter(NUM_META_RESTORE_DIRECTORIES).increment(1L);
  }
}
