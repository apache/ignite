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
import java.util.Objects;
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

    /** Partition entries content hash. */
    @GridToStringExclude
    private int partHash;

    /** Partition entries versions hash. */
    @GridToStringExclude
    private int partVerHash;

    /** Update counter's state. */
    @GridToStringInclude
    private Object updateCntr;

    /** Size. */
    @GridToStringExclude
    private long size;

    /** Partition state. */
    private PartitionState partitionState;

    /**
     * @param partKey Partition key.
     * @param isPrimary Is primary.
     * @param consistentId Consistent id.
     * @param partHash Partition entries content hash.
     * @param partVerHash Partition entries versions hash.
     * @param updateCntr Update counter.
     * @param size Size.
     * @param partitionState Partition state.
     */
    public PartitionHashRecordV2(
        PartitionKeyV2 partKey,
        boolean isPrimary,
        Object consistentId,
        int partHash,
        int partVerHash,
        Object updateCntr,
        long size,
        PartitionState partitionState
    ) {
        this.partKey = partKey;
        this.isPrimary = isPrimary;
        this.consistentId = consistentId;
        this.partHash = partHash;
        this.partVerHash = partVerHash;
        this.updateCntr = updateCntr;
        this.size = size;
        this.partitionState = partitionState;

        assert updateCntr != null;
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
     * @return Partition versions hash.
     */
    public int partitionVersionsHash() {
        return partVerHash;
    }

    /**
     * @return Update counter.
     */
    public Object updateCounter() {
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
        out.writeInt(partVerHash);
        out.writeObject(updateCntr);
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
        partVerHash = in.readInt();
        updateCntr = in.readObject();
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
            S.toString(PartitionHashRecordV2.class, this, "size", size, "partHash", partHash, "partVerHash", partVerHash);
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        PartitionHashRecordV2 v2 = (PartitionHashRecordV2)o;

        return partHash == v2.partHash && partVerHash == v2.partVerHash && updateCntr.equals(v2.updateCntr) &&
            size == v2.size && partKey.equals(v2.partKey) && consistentId.equals(v2.consistentId) &&
            partitionState == v2.partitionState;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return Objects.hash(partKey, consistentId, partHash, partVerHash, updateCntr, size, partitionState);
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
