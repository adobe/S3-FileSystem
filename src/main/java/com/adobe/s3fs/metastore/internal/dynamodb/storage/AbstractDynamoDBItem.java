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

package com.adobe.s3fs.metastore.internal.dynamodb.storage;

import com.adobe.s3fs.metastore.api.AbstractObjectMetadata;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import org.immutables.value.Value;

import java.util.Optional;
import java.util.UUID;

/**
 * Defines an Item as it is stored in DynamoDB.
 */
@Value.Immutable
@Value.Style(strictBuilder = true, typeImmutable = "*")
public abstract class AbstractDynamoDBItem {

  /**
   *
   * @return The hash key of the item.
   */
  public abstract String getHashKey();

  /**
   *
   * @return The sort key of the item.
   */
  public abstract String getSortKey();

  /**
   *
   * @return true if the item represents a directory object, false otherwise.
   */
  public abstract boolean isDirectory();

  /**
   *
   * @return The creation time of the item as UNIX epoch seconds.
   */
  public abstract long getCreationTime();

  /**
   *
   * @return The size of the item.
   */
  public abstract long getSize();

  /**
   * The unique id of the object.
   * @return
   */
  public abstract UUID id();

  /**
   * The version of the object.
   * @return
   */
  public abstract int version();

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
   *
   * @return The physical path of the item. If {{@link AbstractDynamoDBItem#isDirectory()}} returns true, this method returns {@link Optional#empty()}
   */
  public abstract Optional<String> getPhysicalPath();

  @Value.Check
  protected void validate() {
    Preconditions.checkState(!Strings.isNullOrEmpty(getHashKey()));
    Preconditions.checkState(!Strings.isNullOrEmpty(getSortKey()));
    Preconditions.checkState(getCreationTime() > 0);
    if (isDirectory()) {
      Preconditions.checkState(getSize() == 0 && !getPhysicalPath().isPresent());
      Preconditions.checkState(physicalDataCommitted() == false);
    } else {
      Preconditions.checkState(getPhysicalPath().isPresent());
    }

  }
}
