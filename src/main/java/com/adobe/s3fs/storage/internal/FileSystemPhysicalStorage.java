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

package com.adobe.s3fs.storage.internal;

import com.adobe.s3fs.storage.api.PhysicalStorage;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.FailsafeException;
import net.jodah.failsafe.RetryPolicy;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.TimeUnit;

/**
 * Implementation of {@link PhysicalStorage} backed by an instance of {@link FileSystem}.
 */
public class FileSystemPhysicalStorage implements PhysicalStorage {

  private final FileSystem fileSystem;
  private final RetryPolicy retryPolicy;

  private static final Logger LOG = LoggerFactory.getLogger(FileSystemPhysicalStorage.class);

  public FileSystemPhysicalStorage(FileSystem fileSystem, FileSystemPhysicalStorageConfiguration storageConfiguration) {
    this.fileSystem = Preconditions.checkNotNull(fileSystem);
    this.retryPolicy = new RetryPolicy()
        .retryOn(FileNotFoundException.class)
        .withDelay(storageConfiguration.getEventualConsistencyDelayBetweenRetriesMillis(), TimeUnit.MILLISECONDS)
        .withMaxRetries(storageConfiguration.getEventualConsistencyMaxRetries());
  }

  public OutputStream createKey(Path key) throws IOException {
    return fileSystem.create(key, true);
  }

  @Override
  public InputStream openKey(Path key) throws IOException {
    try {
      return Failsafe.with(retryPolicy).get(() -> fileSystem.open(key));
    } catch (FailsafeException fse) {
      LOG.error("Error opening key", fse.getCause());
      Throwables.propagateIfInstanceOf(fse.getCause(), IOException.class); // re-throw all IO errors
      throw Throwables.propagate(fse.getCause()); // wrap in runtime exceptions
    }
  }

  @Override
  public boolean exists(Path key) throws IOException {
    return fileSystem.exists(key);
  }

  @Override
  public void deleteKey(Path key) throws IOException {
    if (fileSystem.exists(key) && !fileSystem.delete(key, true)) {
      throw new IOException("Could not delete " + key);
    }
  }

  @Override
  public void close() throws IOException {
    fileSystem.close();
  }
}
