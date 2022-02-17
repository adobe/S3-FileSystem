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

package com.adobe.s3fs.filesystemcheck.mapreduce;

import com.adobe.s3fs.filesystemcheck.mapreduce.data.LogicalObjectWritable;
import org.immutables.value.Value;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Define elements that are grouped under the same objectHandleId. It includes operationLog element,
 * metaStore element and physical data elements
 */
@Value.Immutable
@Value.Style(strictBuilder = true, typeImmutable = "*")
public abstract class AbstractPartialRestoreData { // NOSONAR

  /** @return Common objectHandleId used for grouping by all the below elements */
  public abstract String objectHandleId();

  /** @return Operation log entity */
  public abstract Optional<LogicalObjectWritable> opLogElement();

  /** @return MetaStore entity */
  public abstract Optional<LogicalObjectWritable> metaStoreElement();

  /** @return Physical data entities */
  @Value.Default
  public List<LogicalObjectWritable> phyDataElements() {
    return Collections.emptyList();
  }

  public boolean isOpLogOnlyPresent() {
    return numEntities() == 1 && opLogElement().isPresent();
  }

  public boolean isMetaOnlyPresent() {
    return numEntities() == 1 && metaStoreElement().isPresent();
  }

  public boolean isPhysicalDataOnlyPresent() {
    return numEntities() == 1 && !phyDataElements().isEmpty();
  }

  public boolean isOpLogAndMetaPresent() {
    return numEntities() == 2 && opLogElement().isPresent() && metaStoreElement().isPresent();
  }

  public boolean isOpLogAndPhysicalDataPresent() {
    return numEntities() == 2 && opLogElement().isPresent() && !phyDataElements().isEmpty();
  }

  public boolean isMetaAndPhysicalDataPresent() {
    return numEntities() == 2 && metaStoreElement().isPresent() && !phyDataElements().isEmpty();
  }

  public boolean areAllPresent() {
    return numEntities() == 3;
  }

  @Value.Lazy
  public int numEntities() {
    int numEntities = 0;
    if (opLogElement().isPresent()) {
      numEntities++;
    }

    if (metaStoreElement().isPresent()) {
      numEntities++;
    }

    if (!phyDataElements().isEmpty()) {
      numEntities++;
    }

    return numEntities;
  }
}
