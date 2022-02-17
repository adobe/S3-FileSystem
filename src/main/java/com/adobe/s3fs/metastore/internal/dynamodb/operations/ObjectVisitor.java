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

import com.adobe.s3fs.metastore.internal.dynamodb.versioning.VersionedObject;

/** A {@link java.nio.file.FileVisitor} style visitor for a node in the metadata tree. */
public interface ObjectVisitor {

  /**
   * Called before the subtree rooted at directory is visited.
   *
   * @param directory
   * @return true if pre-visit operation is successful, false otherwise.
   */
  boolean preVisitDirectoryObject(VersionedObject directory);

  /**
   * Called after the subtree rooted at directory is visited.
   *
   * @param directory
   * @return true if post-visit operation is successful, false otherwise.
   */
  boolean postVisitDirectoryObject(VersionedObject directory);

  /**
   * Called when visiting a single file.
   *
   * @param file
   * @return true is visit operation is successful, false otherwise.
   */
  boolean visitFileObject(VersionedObject file);

  /**
   * Create a visitor for a child. Default implementation returns the same visitor.
   *
   * @param parent
   * @param child
   * @return
   */
  default ObjectVisitor childVisitor(VersionedObject parent, VersionedObject child) {
    return this;
  }
}
