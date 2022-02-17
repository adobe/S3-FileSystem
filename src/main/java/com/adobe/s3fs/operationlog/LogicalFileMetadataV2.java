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

import com.adobe.s3fs.metastore.api.OperationLogEntryState;
import com.adobe.s3fs.metastore.api.ObjectOperationType;
import com.google.common.base.MoreObjects;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public class LogicalFileMetadataV2 implements Externalizable {

    private long creationTime;

    private long size;

    private String physicalPath;

    private String logicalPath;

    private boolean physicalDataCommitted;

    private int version;

    private String id;

    private OperationLogEntryState state;

    private ObjectOperationType type;

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeLong(getCreationTime());
        out.writeLong(getSize());
        out.writeUTF(getPhysicalPath());
        out.writeUTF(getLogicalPath());
        out.writeBoolean(isPhysicalDataCommitted());
        out.writeInt(getVersion());
        out.writeUTF(getId());
        out.writeUTF(state.name());
        out.writeUTF(type.name());
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException {
        setCreationTime(in.readLong());
        setSize(in.readLong());
        setPhysicalPath(in.readUTF());
        setLogicalPath(in.readUTF());
        setPhysicalDataCommitted(in.readBoolean());
        setVersion(in.readInt());
        setId(in.readUTF());
        setState(OperationLogEntryState.valueOf(in.readUTF()));
        setType(ObjectOperationType.valueOf(in.readUTF()));
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

    public String getPhysicalPath() {
        return physicalPath;
    }

    public void setPhysicalPath(String physicalPath) {
        this.physicalPath = physicalPath;
    }

    public String getLogicalPath() {
        return logicalPath;
    }

    public OperationLogEntryState getState() {
        return state;
    }

    public ObjectOperationType getType() {
        return type;
    }

    public void setLogicalPath(String logicalPath) {
        this.logicalPath = logicalPath;
    }

    public boolean isPhysicalDataCommitted() {
        return physicalDataCommitted;
    }

    public void setPhysicalDataCommitted(boolean physicalDataCommitted) {
        this.physicalDataCommitted = physicalDataCommitted;
    }

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public void setState(OperationLogEntryState state) {
        this.state = state;
    }

    public void setType(ObjectOperationType type) {
        this.type = type;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("creationTime", creationTime)
            .add("size", size)
            .add("physicalPath", physicalPath)
            .add("logicalPath", logicalPath)
            .add("physicalDataCommitted", physicalDataCommitted)
            .add("version", version)
            .add("id", id)
            .add("state", state)
            .add("type", type)
            .toString();
    }
}
