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
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.Path;

import java.util.function.Function;

public class SynchronousMetadataTreeOperations implements MetadataTreeOperations {


  private final SynchronousDFSTreeTraversal treeTraversal = new SynchronousDFSTreeTraversal();
  private final MetadataOperations metadataOps;

  public SynchronousMetadataTreeOperations(MetadataOperations metadataOps) {
    this.metadataOps = Preconditions.checkNotNull(metadataOps);
  }

  @Override
  public boolean deleteObjectTree(VersionedObject root, Function<ObjectHandle, Boolean> physicalDeleteCallback) {
    DeleteVisitor deleteVisitor = new DeleteVisitor(metadataOps, physicalDeleteCallback);
    return treeTraversal.traverse(root, metadataOps::list, deleteVisitor);
  }

  @Override
  public boolean renameObjectTree(VersionedObject source, Path destination) {
    RenameVisitor renameVisitor = new RenameVisitor(destination, metadataOps);
    return treeTraversal.traverse(source, metadataOps::list, renameVisitor);
  }
}
