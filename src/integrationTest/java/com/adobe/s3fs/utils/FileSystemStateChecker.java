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

import static org.junit.Assert.assertEquals;

import junit.framework.AssertionFailedError;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class FileSystemStateChecker {

  public static void checkFileSystemState(FileSystem fileSystem, ExpectedFSObject root) throws IOException {
    FileStatus fileStatus = fileSystem.getFileStatus(root.path);
    assertEquals(root.isDir, fileStatus.isDirectory());
    assertEquals(root.path, fileStatus.getPath());
    assertEquals(root.length, fileStatus.getLen());

    if (!root.isDir) {
      return;
    }

    FileStatus[] children = fileSystem.listStatus(root.path);
    assertEquals(root.children.size(), children.length);
    for (FileStatus childStatus : children) {
      ExpectedFSObject expectedChild = root.children.stream()
          .filter(it -> it.path.equals(childStatus.getPath()))
          .findFirst()
          .orElseThrow(AssertionFailedError::new);
      checkFileSystemState(fileSystem, expectedChild);
    }
  }

  public static ExpectedFSObject expectedFile(Path path, long length, int version) {
    return new ExpectedFSObject(path, false, length, version);
  }

  public static ExpectedFSObject expectedDirectory(Path path) {
    return new ExpectedFSObject(path, true, 0, 0);
  }
}
