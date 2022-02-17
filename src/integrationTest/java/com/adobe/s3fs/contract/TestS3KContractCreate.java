/*

 Copyright 2021 Adobe. All rights reserved.
 This file is licensed to you under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License. You may obtain a copy
 of the License at http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software distributed under
 the License is distributed on an "AS IS" BASIS, WITHOUT
 WARRANTIES OR REPRESENTATIONS
 OF ANY KIND, either express or implied. See the License for the specific language
 governing permissions and limitations under the License.
 */

package com.adobe.s3fs.contract;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.contract.AbstractContractCreateTest;
import org.apache.hadoop.fs.contract.AbstractFSContract;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.localstack.LocalStackContainer;

public class TestS3KContractCreate extends AbstractContractCreateTest {

  @ClassRule
  public static Network network = Network.newNetwork();

  @ClassRule
  public static LocalStackContainer localStackContainer = new LocalStackContainer()
      .withNetwork(network)
      .withServices(LocalStackContainer.Service.DYNAMODB, LocalStackContainer.Service.S3)
      .withNetworkAliases("localstack");

  public TemporaryFolder temporaryFolder;

  @Override
  public void setup() throws Exception {
    temporaryFolder = new TemporaryFolder();
    temporaryFolder.create();
    super.setup();
  }

  @Override
  public void teardown() throws Exception {
    temporaryFolder.delete();
    super.teardown();
  }

  @Override
  protected AbstractFSContract createContract(Configuration configuration) {
    ContractUtils.configureFullyFunctionalFileSystem(configuration, localStackContainer, temporaryFolder.getRoot().toString());
    return new S3KFileSystemContract(configuration);
  }

  @Test
  @Override
  public void testFileStatusBlocksizeNonEmptyFile() throws Throwable {
    ContractTestUtils.skip("Block size for files not supported");
  }
}
