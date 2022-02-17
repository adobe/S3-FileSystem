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

package com.adobe.s3fs.filesystemcheck.cmdloader;

import com.adobe.s3fs.filesystemcheck.mapreduce.data.VersionedObjectWritable;
import com.adobe.s3fs.metastore.api.MetadataStoreExtended;
import com.adobe.s3fs.metastore.internal.dynamodb.versioning.VersionedObject;
import com.google.common.base.Preconditions;
import org.apache.commons.lang3.tuple.ImmutablePair;

public class MetaRestoreCommand implements FsckCommand {

  private final ImmutablePair<MetadataStoreExtended, VersionedObject> pair;

  private MetaRestoreCommand(
      MetadataStoreExtended metaStoreExtended, VersionedObjectWritable writable) {
    Preconditions.checkState(metaStoreExtended != null);
    Preconditions.checkState(writable != null);
    Preconditions.checkState(writable.getVersionedObject() != null);
    this.pair = new ImmutablePair<>(metaStoreExtended, writable.getVersionedObject());
  }

  public static MetaRestoreCommand newInstance(
      MetadataStoreExtended metaStoreExtended, VersionedObjectWritable writable) {
    return new MetaRestoreCommand(metaStoreExtended, writable);
  }

  @Override
  public void execute() {
    pair.left.restoreVersionedObject(pair.right);
  }

  @Override
  public String toString() {
    return "metaStoreExtended.restoreVersionedObject " + pair.right;
  }
}
