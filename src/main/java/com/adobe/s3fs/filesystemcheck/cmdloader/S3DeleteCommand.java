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

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.tuple.ImmutablePair;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;

public class S3DeleteCommand implements FsckCommand {

  private final ImmutablePair<S3Client, String[]> clientWithParams;

  private S3DeleteCommand(S3Client s3Client, String[] parameters) {
    Preconditions.checkState(s3Client != null);
    Preconditions.checkState(parameters != null && parameters.length == 2);
    this.clientWithParams = new ImmutablePair<>(s3Client, parameters);
  }

  public static S3DeleteCommand newInstance(S3Client s3Client, String[] parameters) {
    return new S3DeleteCommand(s3Client, parameters);
  }

  @Override
  public void execute() {
    clientWithParams.left.deleteObject(DeleteObjectRequest.builder()
        .bucket(clientWithParams.right[0])
        .key(clientWithParams.right[1])
        .build());
  }

  @Override
  public String toString() {
    return "s3.deleteObject(" + clientWithParams.right[0] + "," + clientWithParams.right[1] + ")"; // NOSONAR
  }
}
