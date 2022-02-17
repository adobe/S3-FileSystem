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

public final class FileSystemMRJobConfig {
  private FileSystemMRJobConfig() {
    // Prevents instantiation
    throw new IllegalStateException("Class not instantiable");
  }

  public static final String LOGICAL_ROOT_PATH = "fs.s3k.fsck.root.path";
  public static final String S3_BACKOFF_BASE_DELAY = "fs.s3k.fsck.s3.base.delay";
  public static final String S3_BACKOFF_MAX_DELAY = "fs.s3k.fsck.s3.max.delay";
  public static final String S3_RETRIES = "fs.s3k.fsck.s3.retries";
  public static final String S3_MAX_CONNECTIONS = "fs.s3k.fsck.s3.max.connections";
  public static final String S3_CMD_LOADER_BUCKET = "fs.s3k.cmd.loader.s3.bucket";
  public static final String S3_DOWNLOAD_BATCH_SIZE = "fs.s3k.fsck.s3.download.batch.size";
  public static final String PARENT_SHUFFLE_CACHE_SIZE = "fs.s3k.fsck.parent.shuffle.cache.size";

  public static final String PENDING_OUTPUT_NAME = "pending";
  public static final String PENDING_OUTPUT_SUFFIX = "pending/part";

  public static final String S3_OUTPUT_NAME = "s3";
  public static final String S3_OUTPUT_SUFFIX = "s3/part";

  public static final String OPLOG_OUTPUT_NAME = "oplog";
  public static final String OPLOG_OUTPUT_SUFFIX = "oplog/part";

  public static final String METASTORE_OUTPUT_NAME = "metastore";
  public static final String METASTORE_OUTPUT_SUFFIX = "metastore/part";

  public static final String NO_ACTIVE_OUTPUT_NAME = "noactive";
  public static final String NO_ACTIVE_OUTPUT_SUFFIX = "noactive/part";
}
