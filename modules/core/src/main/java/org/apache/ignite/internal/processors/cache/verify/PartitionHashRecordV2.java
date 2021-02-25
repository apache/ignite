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
package org.apache.ignite.internal.processors.cache.verify;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorDataTransferObject;
import org.jetbrains.annotations.Nullable;

/**
 * Record containing partition checksum, primary flag and consistent ID of owner.
 */
public class PartitionHashRecordV2 extends VisorDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Moving partition size. */
    public static final long MOVING_PARTITION_SIZE = Long.MIN_VALUE;

    /** Partition key. */
    @GridToStringExclude
    private PartitionKeyV2 partKey;

    /** Is primary flag. */
    private boolean isPrimary;

    /** Consistent id. */
    @GridToStringInclude
    private Object consistentId;

    /** Partition hash. */
    @GridToStringExclude
    private int partHash;

    /** Update counter. */
    private long updateCntr;

    /** Size. */
    @GridToStringExclude
    private long size;

    /** Partition state. */
    private PartitionState partitionState;

    /**
     * @param partKey Partition key.
     * @param isPrimary Is primary.
     * @param consistentId Consistent id.
     * @param partHash Partition hash.
     * @param updateCntr Update counter.
     * @param size Size.
     * @param partitionState Partition state.
     */
    public PartitionHashRecordV2(
        PartitionKeyV2 partKey,
        boolean isPrimary,
        Object consistentId,
        int partHash,
        long updateCntr,
        long size,
        PartitionState partitionState
    ) {
        this.partKey = partKey;
        this.isPrimary = isPrimary;
        this.consistentId = consistentId;
        this.partHash = partHash;
        this.updateCntr = updateCntr;
        this.size = size;
        this.partitionState = partitionState;
    }

    /**
     * Default constructor for Externalizable.
     */
    public PartitionHashRecordV2() {
    }

    /**
     * @return Partition key.
     */
    public PartitionKeyV2 partitionKey() {
        return partKey;
    }

    /**
     * @return Is primary.
     */
    public boolean isPrimary() {
        return isPrimary;
    }

    /**
     * @return Consistent id.
     */
    public Object consistentId() {
        return consistentId;
    }

    /**
     * @return Partition hash.
     */
    public int partitionHash() {
        return partHash;
    }

    /**
     * @return Update counter.
     */
    public long updateCounter() {
        return updateCntr;
    }

    /**
     * @return Size.
     */
    public long size() {
        return size;
    }

    /**
     * @return Partitions state.
     */
    public PartitionState partitionState() {
        return partitionState;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        out.writeObject(partKey);
        out.writeBoolean(isPrimary);
        out.writeObject(consistentId);
        out.writeInt(partHash);
        out.writeLong(updateCntr);
        out.writeLong(size);
        U.writeEnum(out, partitionState);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer,
        ObjectInput in) throws IOException, ClassNotFoundException {
        partKey = (PartitionKeyV2)in.readObject();
        isPrimary = in.readBoolean();
        consistentId = in.readObject();
        partHash = in.readInt();
        updateCntr = in.readLong();
        size = in.readLong();

        if (protoVer >= V2)
            partitionState = PartitionState.fromOrdinal(in.readByte());
        else
            partitionState = size == MOVING_PARTITION_SIZE ? PartitionState.MOVING : PartitionState.OWNING;
    }

    /** {@inheritDoc} */
    @Override public byte getProtocolVersion() {
        return V2;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return size == MOVING_PARTITION_SIZE ?
            S.toString(PartitionHashRecordV2.class, this, "state", "MOVING") :
            S.toString(PartitionHashRecordV2.class, this, "size", size, "partHash", partHash);
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        PartitionHashRecordV2 record = (PartitionHashRecordV2)o;

        return consistentId.equals(record.consistentId);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return consistentId.hashCode();
    }

    /** **/
    public enum PartitionState {
        /** */
        OWNING,
        /** */
        MOVING,
        /** */
        LOST;

        /** Enumerated values. */
        private static final PartitionState[] VALS = values();

        /**
         * Efficiently gets enumerated value from its ordinal.
         *
         * @param ord Ordinal value.
         * @return Enumerated value or {@code null} if ordinal out of range.
         */
        public static @Nullable PartitionState fromOrdinal(int ord) {
            return ord >= 0 && ord < VALS.length ? VALS[ord] : null;
        }
    }
}
