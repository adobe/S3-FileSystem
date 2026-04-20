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

package com.adobe.s3fs.utils;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

import org.junit.rules.ExternalResource;

import java.util.concurrent.atomic.AtomicLong;

public class DynamoTable extends ExternalResource {

  private final String table;
  private static final AtomicLong counter = new AtomicLong();
  private final DynamoDbClient dynamoDbClient;

  public DynamoTable(DynamoDbClient dynamoDbClient) {
    this.table = "dynamo-table" + counter.incrementAndGet();
    this.dynamoDbClient = dynamoDbClient;
  }

  @Override
  protected void before() {
    ITUtils.createMetaTable(dynamoDbClient, table);
  }

  @Override
  protected void after() {
    ITUtils.deleteMetaTable(dynamoDbClient, table);
  }

  public String getTable() {
    return table;
  }
}
