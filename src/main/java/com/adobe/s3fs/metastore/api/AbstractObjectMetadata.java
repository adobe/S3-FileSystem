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

import com.google.common.base.Preconditions;

import org.apache.hadoop.fs.Path;
import org.immutables.value.Value;

import java.util.Optional;

/**
 * Defines the attributes of an object managed by the {@link MetadataStore}.
 * If the object is a directory it can have multiple child objects.
 */
@Value.Immutable
@Value.Style(strictBuilder = true, typeImmutable = "*")
public abstract class AbstractObjectMetadata {

  /**
   *
   * @return the key associated with this object.
   */
  public abstract Path getKey();

  /**
   * @return True if the object is a directory, false otherwise.
   */
  public abstract boolean isDirectory();

  /**
   * @return The file size in bytes.
   */
  public abstract long getSize();

  /**
   *@return The creation time of the object as UNIX epoch seconds.
   */
  public abstract long getCreationTime();

  /**
   *
   * @return True if all data has been successfully stored to physical storage, false otherwise.
   * <b>Note, this is always false for directories</b>
   */
  @Value.Default
  public boolean physicalDataCommitted() {
    return false;
  }

  /**
   * @return The S3 key under which object the exists. In case of directories this method will return {@link Optional#empty()}.
   */
  public abstract Optional<String> getPhysicalPath();

  @Value.Check
  protected void validate() {
    if (!getKey().isRoot()) {
      Preconditions.checkState(getCreationTime() > 0);
    }
    if (isDirectory()) {
      Preconditions.checkState(getSize() == 0 && !getPhysicalPath().isPresent());
      Preconditions.checkState(physicalDataCommitted() == false);
    } else {
      Preconditions.checkState(getPhysicalPath().isPresent());
    }
  }
}
