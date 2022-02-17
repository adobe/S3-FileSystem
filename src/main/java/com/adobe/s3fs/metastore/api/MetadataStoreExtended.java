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

import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.Path;

import java.util.function.Function;

/**
 * Extends the {@link MetadataStore} with more operations.
 */
public interface MetadataStoreExtended extends MetadataStore {

  /**
   * Recursively lists all children of the given key. There is no constraint to implement this method lazily.
   * @param key
   * @return
   */
  Iterable<? extends VersionedObjectHandle> recursiveList(Path key);


  /**
   * Return all the possible child objects of the given key. Results are partitioned according to the partitionCount parameter and partitionIndex parameter.
   * Calls made with different partitionIndex parameters <b>must</b> be executable in parallel.
   * If the concrete implementation does not support scan parallel partitioning, it <b>should</b> throw an exception..
   * This method must return even children that are not reachable though single step parent-child relation iterations.
   * For example:
   * If the metadata store contains:
   *  <ul>
   *    <li>/a</li>
   *    <li>/a/b</li>
   *    <li>/a/d/e</li>
   *  </ul>
   *  The scan on key "/a" should return ["/a", "/a/b", "/a/d/e"], even if intermediary child /a/d is missing.
   *  There is no constraint to implement this method lazily.
   * @param key
   * @param partitionIndex
   * @param partitionCount
   * @return
   */
  Iterable<? extends VersionedObjectHandle> scanPartition(Path key, int partitionIndex, int partitionCount);

  /**
   * Computes the content summary(ideally in an implementation specific optimized way) for the given object.
   * @param objectHandle
   * @return
   */
  ContentSummary computeContentSummary(ObjectHandle objectHandle);


  /**
   * Update metastore with a preconfigured object which already has
   * version and the object handle id. Used by file system check command
   * @param object a VersionedObjectHandle to upsert in DynamoDB
   * @return
   */
  VersionedObjectHandle restoreVersionedObject(VersionedObjectHandle object);

  /**
   * See {@link MetadataStore#deleteObject(ObjectHandle, Function)}.
   * Deletes the given object. If the object is a directory, just the directory entry will be deleted (children will remain).
   * @param object
   * @param callback
   * @return true if operation succeeds, false otherwise.
   */
  boolean deleteSingleObject(VersionedObjectHandle object, Function<ObjectHandle, Boolean> callback);
}
