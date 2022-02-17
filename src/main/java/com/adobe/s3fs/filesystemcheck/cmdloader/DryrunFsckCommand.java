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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a wrapper class which takes a FsckCommand and prints it
 * to standard output
 */
public class DryrunFsckCommand implements FsckCommand {

  private static final Logger LOG = LoggerFactory.getLogger(DryrunFsckCommand.class);

  private final FsckCommand delegate;

  private DryrunFsckCommand(FsckCommand delegate) {
    this.delegate = Preconditions.checkNotNull(delegate);
  }

  public static DryrunFsckCommand newInstance(FsckCommand delegate) {
    return new DryrunFsckCommand(delegate);
  }

  @Override
  public void execute() {
    LOG.info("[Dryrun] {}", delegate);
  }
}
