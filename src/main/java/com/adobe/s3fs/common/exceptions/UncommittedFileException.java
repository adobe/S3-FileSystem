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

package com.adobe.s3fs.common.exceptions;

import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class UncommittedFileException extends IOException {

  public UncommittedFileException(Path path) {
    super(formatMessage(path));
  }

  private static String formatMessage(Path path) {
    return String.format("Data was not committed for %s."
                                   + " There may have been silent errors on the client side", path.toString());
  }
}
