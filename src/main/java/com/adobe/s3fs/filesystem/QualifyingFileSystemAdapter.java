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

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import org.apache.hadoop.fs.*;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.util.EnumSet;

/**
 * Wrapper over a {@link FileSystemImplementation} that adds logic for the working directory API of {@link org.apache.hadoop.fs.FileSystem}.
 * All operations are delegated to the underlying {@link FileSystemImplementation} after paths are qualified.
 */
public class QualifyingFileSystemAdapter implements Closeable {

  private final URI defaultUri;
  private final FileSystemImplementation fileSystemImpl;
  private final QualifyingFileSystemMetrics qualifyingFsMetrics;
  private Path workingDirectory;

  public QualifyingFileSystemAdapter(
      URI defaultUri,
      FileSystemImplementation fileSystemImpl,
      QualifyingFileSystemMetrics qualifyingFsMetrics) {
    this.defaultUri = Preconditions.checkNotNull(defaultUri);
    this.fileSystemImpl = Preconditions.checkNotNull(fileSystemImpl);
    this.qualifyingFsMetrics = Preconditions.checkNotNull(qualifyingFsMetrics);
  }

  public FSDataInputStream open(Path path) throws IOException {
    return fileSystemImpl.open(qualify(path));
  }

  public FSDataOutputStream createNonRecursive(Path path, EnumSet<CreateFlag> flags) throws IOException {
    try {
      return fileSystemImpl.createNonRecursive(qualify(path), flags);
    } catch (Exception e) {
      if (flags.contains(CreateFlag.OVERWRITE)) {
        qualifyingFsMetrics.recordFailedOverwrite();
      } else {
        qualifyingFsMetrics.recordFailedCreate();
      }
      Throwables.throwIfInstanceOf(e, IOException.class); // re-throw all IO errors
      throw Throwables.propagate(e);
    }
  }

  public FSDataOutputStream create(Path path, boolean overwrite) throws IOException {
    try {
      return fileSystemImpl.create(qualify(path), overwrite);
    } catch (Exception e) {
      if (overwrite) {
        qualifyingFsMetrics.recordFailedOverwrite();
      } else {
        qualifyingFsMetrics.recordFailedCreate();
      }
      Throwables.throwIfInstanceOf(e, IOException.class); // re-throw all IO errors
      throw Throwables.propagate(e);
    }
  }

  public boolean rename(Path src, Path dst) throws IOException {
    try {
      boolean returnStatus = fileSystemImpl.rename(qualify(src), qualify(dst));
      if (returnStatus) {
        qualifyingFsMetrics.recordSuccessfulRename();
      }
      return returnStatus;
    } catch (Exception e) {
      // Same as mkdirs. fileSystemImpl.rename(src, dst) always returns true
      qualifyingFsMetrics.recordFailedRename();
      Throwables.throwIfInstanceOf(e, IOException.class); // re-throw all IO errors
      throw Throwables.propagate(e);
    }
  }

  public boolean delete(Path path, boolean recursive) throws IOException {
    try {
      DeleteOutcome returnStatus = fileSystemImpl.delete(qualify(path), recursive);
      if (returnStatus == DeleteOutcome.Successful) {
        qualifyingFsMetrics.recordSuccessfulDelete();
      } else if (returnStatus == DeleteOutcome.Failed){
        qualifyingFsMetrics.recordFailedDelete();
      }
      return returnStatus == DeleteOutcome.Successful;
    } catch (Exception e) {
      qualifyingFsMetrics.recordFailedDelete();
      Throwables.throwIfInstanceOf(e, IOException.class); // re-throw all IO errors
      throw Throwables.propagate(e);
    }
  }

  public FileStatus[] listStatus(Path path) throws IOException {
    return fileSystemImpl.listStatus(qualify(path));
  }

  public void setWorkingDirectory(Path path) {
    workingDirectory = path;
  }

  public Path getWorkingDirectory() {
    return workingDirectory;
  }

  public boolean mkdirs(Path path) throws IOException {
    try {
      boolean returnStatus = fileSystemImpl.mkdirs(qualify(path));
      if (returnStatus) {
        qualifyingFsMetrics.recordSuccessfulMkdir();
      }
      return returnStatus;
    } catch (Exception e) {
      // fileSystemImpl.mkdirs(path); will always return true
      // (if operation succeeds). On failure exceptions are thrown
      qualifyingFsMetrics.recordFailedMkdir();
      Throwables.throwIfInstanceOf(e, IOException.class); // re-throw all IO errors
      throw Throwables.propagate(e);
    }
  }

  public FileStatus getFileStatus(Path path) throws IOException {
    return fileSystemImpl.getFileStatus(qualify(path));
  }

  public boolean supportsSpecificContentSummaryComputation() {
    return fileSystemImpl.supportsSpecificContentSummaryComputation();
  }

  public ContentSummary getContentSummary(Path path) throws IOException {
    return fileSystemImpl.getContentSummary(qualify(path));
  }

  @Override
  public void close() throws IOException {
    fileSystemImpl.close();
  }

  private Path qualify(Path path) {
    return path.makeQualified(defaultUri, workingDirectory);
  }
}
