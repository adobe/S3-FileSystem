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

package com.adobe.s3fs.operationlog;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public class LogicalFileMetadata implements Externalizable {
  long creationTime;
  long size;
  String logicalPath;
  boolean physicalDataCommitted;

  public String getLogicalPath() {
    return logicalPath;
  }

  public void setLogicalPath(String logicalPath) {
    this.logicalPath = logicalPath;
  }

  public long getCreationTime() {
    return creationTime;
  }

  public void setCreationTime(long creationTime) {
    this.creationTime = creationTime;
  }

  public long getSize() {
    return size;
  }

  public void setSize(long size) {
    this.size = size;
  }

  public boolean isPhysicalDataCommitted() {
    return physicalDataCommitted;
  }

  public void setPhysicalDataCommitted(boolean physicalDataCommitted) {
    this.physicalDataCommitted = physicalDataCommitted;
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    out.writeUTF(logicalPath);
    out.writeLong(size);
    out.writeLong(creationTime);
    out.writeBoolean(physicalDataCommitted);
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException {
      logicalPath = in.readUTF();
      size = in.readLong();
      creationTime = in.readLong();
      physicalDataCommitted = in.readBoolean();
  }
}
