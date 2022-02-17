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

import com.adobe.s3fs.metastore.api.VersionedObjectHandle;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

import static com.adobe.s3fs.filesystemcheck.mapreduce.FileSystemMRJobConfig.PARENT_SHUFFLE_CACHE_SIZE;
import static com.adobe.s3fs.filesystemcheck.mapreduce.FsckCounters.NUM_SHUFFLED_DIRECTORIES;
import static com.adobe.s3fs.filesystemcheck.mapreduce.FsckCounters.NUM_UNEXPECTED_DIRECTORIES;

public class MetadataStorePartitionMapper
    extends Mapper<Void, VersionedObjectHandle, Text, NullWritable> {

  // Cache for avoiding shuffling same key multiple times
  private ParentShuffleCache parentShuffleCache;

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);

    int cacheSize = context.getConfiguration().getInt(PARENT_SHUFFLE_CACHE_SIZE, 100);
    parentShuffleCache = new ParentShuffleCache(cacheSize);
  }

  @Override
  protected void map(Void key, VersionedObjectHandle versionedObjHandle, Context context)
      throws IOException, InterruptedException {

    if (versionedObjHandle.metadata().isDirectory()) {
      /*
       * This case should not happened when running the
       * second phase of the full restore. During the first phase
       * only file are restored into the metastore.
       * However will capture this case, log it into a counter
       * and skip to the next element
       */
      context.getCounter(NUM_UNEXPECTED_DIRECTORIES).increment(1L);
      return;
    }
    Path logicalPath = versionedObjHandle.metadata().getKey();
    Path logicalParentPath = logicalPath.getParent();

    while (!logicalParentPath.isRoot()) {
      if (parentShuffleCache.shuffleParent(logicalParentPath)) {
        context.write(
            new Text(logicalParentPath.toString()), NullWritable.get());
        context.getCounter(NUM_SHUFFLED_DIRECTORIES).increment(1L);
      }
      logicalParentPath = logicalParentPath.getParent();
    }
  }
}
