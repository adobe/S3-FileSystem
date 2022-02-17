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

package com.adobe.s3fs.filesystemcheck.mapreduce.data;

import com.google.common.base.MoreObjects;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class LogicalObjectWritable implements Writable {
  // Data
  private SourceType sourceType;
  private String physicalPath;
  private String logicalPath;
  private long size;
  private long creationTime;
  private int version;
  private boolean isDirectory;
  private boolean isPendingState;

  public static LogicalObjectWritable copyOf(LogicalObjectWritable other) {
    return new LogicalObjectWritable(other);
  }

  public LogicalObjectWritable() {
    // Need no arg ctor for Hadoop Framework
  }

  // Copy ctor
  private LogicalObjectWritable(LogicalObjectWritable other) {
    this.sourceType = other.sourceType;
    this.physicalPath = other.physicalPath;
    this.logicalPath = other.logicalPath;
    this.size = other.size;
    this.creationTime = other.creationTime;
    this.version = other.version;
    this.isDirectory = other.isDirectory;
    this.isPendingState = other.isPendingState;
  }

  private LogicalObjectWritable(Builder builder) {
    this.sourceType = builder.sourceType;
    this.physicalPath = builder.physical;
    this.logicalPath = builder.logical;
    this.size = builder.size;
    this.creationTime = builder.creationTime;
    this.version = builder.version;
    this.isDirectory = builder.isDirectory;
    this.isPendingState = builder.isPendingState;

  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    dataOutput.writeShort(sourceType.getCode());
    dataOutput.writeUTF(physicalPath);
    dataOutput.writeUTF(logicalPath);
    dataOutput.writeLong(size);
    dataOutput.writeLong(creationTime);
    dataOutput.writeInt(version);
    dataOutput.writeBoolean(isDirectory);
    dataOutput.writeBoolean(isPendingState);
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    sourceType = SourceType.fromCode(dataInput.readShort());
    physicalPath = dataInput.readUTF();
    logicalPath = dataInput.readUTF();
    size = dataInput.readLong();
    creationTime = dataInput.readLong();
    version = dataInput.readInt();
    isDirectory = dataInput.readBoolean();
    isPendingState = dataInput.readBoolean();
  }

  public boolean isDirectory() {
    return isDirectory;
  }

  public boolean isPendingState() {
    return isPendingState;
  }

  public String getPhysicalPath() {
    return physicalPath;
  }

  public String getLogicalPath() {
    return logicalPath;
  }

  public long getSize() {
    return size;
  }

  public long getCreationTime() {
    return creationTime;
  }

  public int getVersion() {
    return version;
  }

  public SourceType getSourceType() {
    return sourceType;
  }

  public LogicalObjectWritable withPendingState(boolean newPendingState) {
    return new Builder()
        .withSize(size)
        .withCreationTime(creationTime)
        .withPhysicalPath(physicalPath)
        .withLogicalPath(logicalPath)
        .withVersion(version)
        .isDirectory(isDirectory)
        .withSourceType(sourceType)
        .isPendingState(newPendingState)
        .build();
  }

  public LogicalObjectWritable withSourceType(SourceType newSourceType) {
    return new Builder()
        .withSize(size)
        .withCreationTime(creationTime)
        .withPhysicalPath(physicalPath)
        .withLogicalPath(logicalPath)
        .withVersion(version)
        .isDirectory(isDirectory)
        .withSourceType(newSourceType)
        .isPendingState(isPendingState)
        .build();
  }

  public static LogicalObjectWritable fromS3Writable(String phyPath) {
    return new LogicalObjectWritable.Builder()
        .withSize(0L) // don't care the value from here
        .withCreationTime(0L) // don't care about this value
        .isPendingState(false)
        .isDirectory(false)
        .withPhysicalPath(phyPath)
        .withLogicalPath("") // don't care
        .withVersion(0) // don't care
        .withSourceType(SourceType.FROM_S3)
        .build();
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper("LogicalObjectWritable")
        .omitNullValues()
        .add("physicalPath", physicalPath)
        .add("logicalPath", logicalPath)
        .add("version", version)
        .add("isPendingState", isPendingState)
        .toString();
  }


  public static class Builder {
    private String physical;
    private String logical;
    private long size;
    private long creationTime;
    private int version;
    private boolean isDirectory;
    private boolean isPendingState;
    private SourceType sourceType;

    public Builder withPhysicalPath(String physicalPath){
      this.physical = physicalPath;
      return this;
    }

    public Builder withLogicalPath(String logicalPath) {
      this.logical = logicalPath;
      return this;
    }

    public Builder withSize(long size) {
      this.size = size;
      return this;
    }

    public Builder withCreationTime(long creationTime){
      this.creationTime = creationTime;
      return this;
    }

    public Builder withVersion(int version) {
      this.version = version;
      return this;
    }

    public Builder isDirectory(boolean isDirectory){
      this.isDirectory = isDirectory;
      return this;
    }

    public Builder isPendingState(boolean isPendingState) {
      this.isPendingState = isPendingState;
      return this;
    }

    public Builder withSourceType(SourceType sourceType){
      this.sourceType = sourceType;
      return this;
    }

    public LogicalObjectWritable build() {
      return new LogicalObjectWritable(this);
    }
  }
}
