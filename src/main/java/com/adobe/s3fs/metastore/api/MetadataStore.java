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

import org.apache.hadoop.fs.Path;

import java.io.Closeable;
import java.util.Optional;
import java.util.function.Function;

/**
 * Defines the contract for the storage of {@link ObjectMetadata}.
 * The {@link MetadataStore} has no durability guarantees.
 * It should rely on an implementation of {@link MetadataOperationLog} to store data for recovery purposes.
 */
public interface MetadataStore extends Closeable {

  /**
   * Stores the given {@link ObjectMetadata}. If it already exists, it will be overwritten.
   * This operation is strongly consistent, all subsequent calls to {{@link #getObject(Path)}} or
   * {{@link #listChildObjects(ObjectHandle)}} will see the effects of this call.
   * @param objectMetadata
   * @return true if operation succeeds, false otherwise.
   */
  ObjectHandle createObject(ObjectMetadata objectMetadata);

  ObjectHandle updateObject(ObjectHandle currentHandle, ObjectMetadata newMetadata);

  /**
   * Return the {@link ObjectMetadata} associated with the given key..
   * @param key
   * @return Returns {@link Optional#empty()} if no object is associated, otherwise the associated object.
   */
  Optional<? extends ObjectHandle> getObject(Path key);


  /**
   * Associates sourceObjectMetadata to the given destinationKey.
   * If sourceObjectMetadata is a directory, the operation is recursive.
   * @param sourceObject
   * @param destinationKey
   * @return true if the operation succeeds, false otherwise.
   */
  boolean renameObject(ObjectHandle sourceObject, Path destinationKey);

  /**
   * Deletes the given objectMetadata from storage.
   * if objectMetadata is a directory the operation is recursive.
   * @param object
   * @param callback Callback function invoked after operation is successfully completed by the meta store (deletion from meta store
   *                 and operation persisted in the operation log).
   *                 If objectMetadata is a directory then the callback is invoked for each child in the subtree.
   *                 If the callback return false then the call to {@link MetadataStore#deleteObject(ObjectHandle, Function)}
   *                 will return false as well.
   * @return true if operation succeeds, false otherwise.
   */
  boolean deleteObject(ObjectHandle object, Function<ObjectHandle, Boolean> callback);

  /**
   * @param object
   * @return first level children of the given objectMetadata,
   * or the objectMetadata itself if {@link ObjectMetadata#isDirectory()} returns true.
   */
  Iterable<? extends ObjectHandle> listChildObjects(ObjectHandle object);
}
