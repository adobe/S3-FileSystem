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

import java.io.Closeable;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * Persistently stores all metadata operations for recovery purposes.
 */
public interface MetadataOperationLog extends Closeable {

  /**
   * Handle for either rolling back the operation log to the last state for a given object, or committing the last state.
   */
  interface LogEntryHandle {
    boolean rollback();
    boolean commit();
  }

  /**
   * Logs a create operation executed by the metadata store.
   * @param object The object that was created.
   * @return A handle to either commit the create operation as the final operation for the given object,
   * or rollback to the last state (in this case this will leave the operation log as if the given object never existed).
   * <b>Note</b> the optional will be empty if the rollback handle cannot be created.
   */
  Optional<LogEntryHandle> logCreateOperation(VersionedObjectHandle object);

  /**
   * Async version of {@link MetadataOperationLog#logCreateOperation(VersionedObjectHandle)}
   * @param object
   * @return
   */
  CompletableFuture<Optional<LogEntryHandle>> logCreateOperationAsync(VersionedObjectHandle object);

  /**
   * Logs an update operation executed by the metadata store. This can either be a rename or an update of attributes(e.g. size).
   * @param sourceObject The object that was renamed.
   * @param destinationObject The object being updated.
   * @return A handle to either commit the update operation as the final operation for the given object,
   * or rollback to the last state (in this case the last state is sourceObject).
   * <b>Note</b> the optional will be empty if the rollback handle cannot be created.
   */
  Optional<LogEntryHandle> logUpdateOperation(VersionedObjectHandle sourceObject, VersionedObjectHandle destinationObject);

  /**
   * Async version of {@link MetadataOperationLog#logUpdateOperation(VersionedObjectHandle, VersionedObjectHandle)}
   * @param sourceObject
   * @param destinationObject
   * @return
   */
  CompletableFuture<Optional<LogEntryHandle>> logUpdateOperationAsync(VersionedObjectHandle sourceObject, VersionedObjectHandle destinationObject);

  /**
   * Logs a delete operation executed by the metadata store.
   * @param object The object that was deleted.
   * @return True if the store operation was successful, false otherwise
   */
  Optional<LogEntryHandle> logDeleteOperation(VersionedObjectHandle object);

  /**
   * Async version of {@link MetadataOperationLog#logDeleteOperation(VersionedObjectHandle)}
   * @param object
   * @return
   */
  CompletableFuture<Optional<LogEntryHandle>> logDeleteOperationAsync(VersionedObjectHandle object);
}
