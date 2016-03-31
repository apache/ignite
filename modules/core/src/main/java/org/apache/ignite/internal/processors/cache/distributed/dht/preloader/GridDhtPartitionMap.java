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

package org.apache.ignite.internal.processors.cache.distributed.dht.preloader;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionState;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Partition map.
 */
@Deprecated // Backward compatibility, use GridDhtPartitionMap2 instead.
public class GridDhtPartitionMap extends GridDhtPartitionMap2 {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * @param nodeId Node ID.
     * @param updateSeq Update sequence number.
     * @param m Map to copy.
     */
    public GridDhtPartitionMap(UUID nodeId, long updateSeq,
        Map<Integer, GridDhtPartitionState> m) {
        assert nodeId != null;
        assert updateSeq > 0;

        this.nodeId = nodeId;
        this.updateSeq = updateSeq;

        map = U.newHashMap(m.size());

        for (Map.Entry<Integer, GridDhtPartitionState> e : m.entrySet()) {
            GridDhtPartitionState state = e.getValue();

            put(e.getKey(), state);
        }
    }

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    public GridDhtPartitionMap() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        U.writeUuid(out, nodeId);

        out.writeLong(updateSeq);

        int size = map.size();

        out.writeInt(size);

        int i = 0;

        for (Map.Entry<Integer, GridDhtPartitionState> entry : map.entrySet()) {
            int ordinal = entry.getValue().ordinal();

            assert ordinal == (ordinal & 0x3);
            assert entry.getKey() == (entry.getKey() & 0x3FFF);

            int coded = (ordinal << 14) | entry.getKey();

            out.writeShort((short)coded);

            i++;
        }

        assert i == size;
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        nodeId = U.readUuid(in);

        updateSeq = in.readLong();

        int size = in.readInt();

        map = U.newHashMap(size);

        for (int i = 0; i < size; i++) {
            int entry = in.readShort() & 0xFFFF;

            int part = entry & 0x3FFF;
            int ordinal = entry >> 14;

            put(part, GridDhtPartitionState.fromOrdinal(ordinal));
        }
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        GridDhtPartitionMap2 other = (GridDhtPartitionMap2)o;

        return other.nodeId.equals(nodeId) && other.updateSeq == updateSeq;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return 31 * nodeId.hashCode() + (int)(updateSeq ^ (updateSeq >>> 32));
    }

    /**
     * @return Full string representation.
     */
    public String toFullString() {
        return S.toString(GridDhtPartitionMap2.class, this, "size", size(), "map", map.toString());
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDhtPartitionMap2.class, this, "size", size());
    }
}