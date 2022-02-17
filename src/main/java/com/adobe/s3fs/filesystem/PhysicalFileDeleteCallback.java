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

package com.adobe.s3fs.filesystem;

import com.adobe.s3fs.metastore.api.ObjectHandle;
import com.adobe.s3fs.storage.api.PhysicalStorage;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Function;

public class PhysicalFileDeleteCallback implements Function<ObjectHandle, Boolean> {

  private final PhysicalStorage physicalStorage;
  private final FileSystemMetrics fsMetrics;

  private static final Logger LOG = LoggerFactory.getLogger(PhysicalFileDeleteCallback.class);

  public PhysicalFileDeleteCallback(PhysicalStorage physicalStorage, FileSystemMetrics fsMetrics) {
    this.physicalStorage = Preconditions.checkNotNull(physicalStorage);
    this.fsMetrics = Preconditions.checkNotNull(fsMetrics);
  }

  @VisibleForTesting
  public PhysicalStorage getPhysicalStorage() {
    return physicalStorage;
  }

  @Override
  public Boolean apply(ObjectHandle objectHandle) {
    if (objectHandle.metadata().isDirectory()) {
      return Boolean.TRUE;
    }
    if (!objectHandle.metadata().physicalDataCommitted()) {
      return Boolean.TRUE;
    }

    String physicalPath = objectHandle.metadata().getPhysicalPath()
        .orElseThrow(IllegalStateException::new);
    try {
      physicalStorage.deleteKey(new Path(physicalPath));
      return Boolean.TRUE;
    } catch (Exception e) {
      LOG.error("Error deleting key " + physicalPath, e);
      fsMetrics.recordFailedPhysicalDelete();
      return Boolean.FALSE;
    }
  }
}
