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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Function;

/** Implements a synchronous DFS tree traversal. */
public class SynchronousDFSTreeTraversal {

  private static final Logger LOG = LoggerFactory.getLogger(SynchronousDFSTreeTraversal.class);
  
  public boolean traverse(
      VersionedObject root,
      Function<VersionedObject, Iterable<VersionedObject>> childExpansionFunction,
      ObjectVisitor visitor) {
    return recursiveTraverse(root, childExpansionFunction, visitor);
  }

  private boolean recursiveTraverse(
      VersionedObject source,
      Function<VersionedObject, Iterable<VersionedObject>> childExpansionFunction,
      ObjectVisitor visitor) {
    try {
      if (!source.metadata().isDirectory()) {
        return visitor.visitFileObject(source);
      }

      if (!visitor.preVisitDirectoryObject(source)) {
        return false;
      }

      for (VersionedObject child : childExpansionFunction.apply(source)) {

        ObjectVisitor childVisitor = visitor.childVisitor(source, child);
        boolean childVisitation = recursiveTraverse(child, childExpansionFunction, childVisitor);

        if (!childVisitation) {
          return false;
        }
      }

      return visitor.postVisitDirectoryObject(source);
    } catch (Exception e) {
      LOG.error("Error traversing object " + source, e);
      return false;
    }
  }
}
