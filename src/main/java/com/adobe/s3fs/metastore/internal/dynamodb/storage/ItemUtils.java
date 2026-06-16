/*
Copyright 2026 Adobe. All rights reserved.
This file is licensed to you under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License. You may obtain a copy
of the License at https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under
the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR REPRESENTATIONS
OF ANY KIND, either express or implied. See the License for the specific language
governing permissions and limitations under the License.
*/
package com.adobe.s3fs.metastore.internal.dynamodb.storage;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

/**
 * Utility class to convert Java values to DynamoDB AttributeValue.
 * This replicates the built-in ItemUtils from AWS SDK V1.
 * Handles null values the same way as V1 SDK did.
 */
class ItemUtils {

  private ItemUtils() {
    // Utility class
  }

  static AttributeValue toAttributeValue(Object value) {
    if (value == null) {
      return AttributeValue.builder().nul(true).build();
    }

    if (value instanceof String) {
      return AttributeValue.builder().s((String) value).build();
    }

    if (value instanceof Number) {
      return AttributeValue.builder().n(value.toString()).build();
    }

    if (value instanceof Boolean) {
      return AttributeValue.builder().bool((Boolean) value).build();
    }

    throw new IllegalArgumentException("Unsupported type: " + value.getClass().getName());
  }
}
