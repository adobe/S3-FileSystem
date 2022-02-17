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

package com.adobe.s3fs.metastore.internal.dynamodb.operations;

import com.adobe.s3fs.metastore.api.ObjectHandle;
import com.adobe.s3fs.metastore.internal.dynamodb.versioning.VersionedObject;
import com.google.common.base.Preconditions;

import java.util.function.Function;

/** Visitor that deletes an object tree. */
public class DeleteVisitor implements ObjectVisitor {

  private final MetadataOperations metadataOps;
  private final Function<ObjectHandle, Boolean> callback;

  public DeleteVisitor(MetadataOperations metadataOps, Function<ObjectHandle, Boolean> callback) {
    this.metadataOps = Preconditions.checkNotNull(metadataOps);
    this.callback = Preconditions.checkNotNull(callback);
  }

  /**
   * No operation needed. Always returns true.
   *
   * @param directory
   * @return
   */
  @Override
  public boolean preVisitDirectoryObject(VersionedObject directory) {
    return true;
  }

  /**
   * Deletes the directory.
   *
   * @param directory
   * @return
   */
  @Override
  public boolean postVisitDirectoryObject(VersionedObject directory) {
    return metadataOps.delete(directory) && callback.apply(directory);
  }

  /**
   * Deletes a single file.
   *
   * @param file
   * @return
   */
  @Override
  public boolean visitFileObject(VersionedObject file) {
    return metadataOps.delete(file) && callback.apply(file);
  }
}
