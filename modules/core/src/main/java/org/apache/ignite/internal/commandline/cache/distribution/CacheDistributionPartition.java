/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.internal.commandline.cache.distribution;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionState;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorDataTransferObject;

/**
 * DTO for CacheDistributionTask, contains information about partition
 */
public class CacheDistributionPartition extends VisorDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private int partition;

    /** */
    private boolean primary;

    /** */
    private GridDhtPartitionState state;

    /** */
    private long updateCounter;

    /** */
    private long size;

    /** */
    public int getPartition() {
        return partition;
    }

    /** */
    public void setPartition(int partition) {
        this.partition = partition;
    }

    /** */
    public boolean isPrimary() {
        return primary;
    }

    /** */
    public void setPrimary(boolean primary) {
        this.primary = primary;
    }

    /** */
    public GridDhtPartitionState getState() {
        return state;
    }

    /** */
    public void setState(GridDhtPartitionState state) {
        this.state = state;
    }

    /** */
    public long getUpdateCounter() {
        return updateCounter;
    }

    /** */
    public void setUpdateCounter(long updateCounter) {
        this.updateCounter = updateCounter;
    }

    /** */
    public long getSize() {
        return size;
    }

    /** */
    public void setSize(long size) {
        this.size = size;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        out.writeInt(partition);
        out.writeBoolean(primary);
        U.writeEnum(out, state);
        out.writeLong(updateCounter);
        out.writeLong(size);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException {
        partition = in.readInt();
        primary = in.readBoolean();
        state = GridDhtPartitionState.fromOrdinal(in.readByte());
        updateCounter = in.readLong();
        size = in.readLong();
    }
}
