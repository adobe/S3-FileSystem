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

package com.adobe.s3fs.metastore.api;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public class DisabledMetadataOperationLog implements MetadataOperationLog {

  private static final LogEntryHandle DISABLED_LOG_ENTRY_HANDLE = new LogEntryHandle() {
    @Override
    public boolean rollback() {
      return true;
    }

    @Override
    public boolean commit() {
      return true;
    }
  };

  @Override
  public Optional<LogEntryHandle> logCreateOperation(VersionedObjectHandle object) {
    return Optional.of(DISABLED_LOG_ENTRY_HANDLE);
  }

  @Override
  public CompletableFuture<Optional<LogEntryHandle>> logCreateOperationAsync(VersionedObjectHandle object) {
    return CompletableFuture.completedFuture(Optional.of(DISABLED_LOG_ENTRY_HANDLE));
  }

  @Override
  public Optional<LogEntryHandle> logUpdateOperation(VersionedObjectHandle sourceObject, VersionedObjectHandle destinationObject) {
    return Optional.of(DISABLED_LOG_ENTRY_HANDLE);
  }

  @Override
  public CompletableFuture<Optional<LogEntryHandle>> logUpdateOperationAsync(VersionedObjectHandle sourceObject, VersionedObjectHandle destinationObject) {
    return CompletableFuture.completedFuture(Optional.of(DISABLED_LOG_ENTRY_HANDLE));
  }

  @Override
  public Optional<LogEntryHandle> logDeleteOperation(VersionedObjectHandle object) {
    return Optional.of(DISABLED_LOG_ENTRY_HANDLE);
  }

  @Override
  public CompletableFuture<Optional<LogEntryHandle>> logDeleteOperationAsync(VersionedObjectHandle object) {
    return CompletableFuture.completedFuture(Optional.of(DISABLED_LOG_ENTRY_HANDLE));
  }

  @Override
  public void close() throws IOException {

  }
}
