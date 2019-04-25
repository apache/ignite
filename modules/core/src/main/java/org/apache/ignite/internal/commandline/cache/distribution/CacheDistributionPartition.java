/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
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
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorDataTransferObject;

/**
 * DTO for CacheDistributionTask, contains information about partition
 */
public class CacheDistributionPartition extends VisorDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Partition identifier. */
    private int partId;

    /** Flag primary or backup partition. */
    private boolean primary;

    /** Partition status. */
    private GridDhtPartitionState state;

    /** Partition update counters. */
    private long updateCntr;

    /** Number of entries in partition. */
    private long size;

    /** Default constructor. */
    public CacheDistributionPartition() {
    }

    /**
     * @param partId Partition identifier.
     * @param primary Flag primary or backup partition.
     * @param state Partition status.
     * @param updateCntr Partition update counters.
     * @param size Number of entries in partition.
     */
    public CacheDistributionPartition(int partId, boolean primary,
        GridDhtPartitionState state, long updateCntr, long size) {
        this.partId = partId;
        this.primary = primary;
        this.state = state;
        this.updateCntr = updateCntr;
        this.size = size;
    }

    /** */
    public int getPartition() {
        return partId;
    }

    /** */
    public void setPartition(int partId) {
        this.partId = partId;
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
        return updateCntr;
    }

    /** */
    public void setUpdateCounter(long updateCntr) {
        this.updateCntr = updateCntr;
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
        out.writeInt(partId);
        out.writeBoolean(primary);
        U.writeEnum(out, state);
        out.writeLong(updateCntr);
        out.writeLong(size);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException {
        partId = in.readInt();
        primary = in.readBoolean();
        state = GridDhtPartitionState.fromOrdinal(in.readByte());
        updateCntr = in.readLong();
        size = in.readLong();
    }
}
