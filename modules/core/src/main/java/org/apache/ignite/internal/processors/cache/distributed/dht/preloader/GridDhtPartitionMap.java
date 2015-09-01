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
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionState;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionState.MOVING;

/**
 * Partition map.
 */
public class GridDhtPartitionMap implements Comparable<GridDhtPartitionMap>, Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Node ID. */
    private UUID nodeId;

    /** Update sequence number. */
    private long updateSeq;

    /** */
    private Map<Integer, GridDhtPartitionState> map;

    /** */
    private volatile int moving;

    /**
     * @param nodeId Node ID.
     * @param updateSeq Update sequence number.
     */
    public GridDhtPartitionMap(UUID nodeId, long updateSeq) {
        assert nodeId != null;
        assert updateSeq > 0;

        this.nodeId = nodeId;
        this.updateSeq = updateSeq;

        map = new HashMap<>();
    }

    /**
     * @param nodeId Node ID.
     * @param updateSeq Update sequence number.
     * @param m Map to copy.
     * @param onlyActive If {@code true}, then only active states will be included.
     */
    public GridDhtPartitionMap(UUID nodeId, long updateSeq, Map<Integer, GridDhtPartitionState> m, boolean onlyActive) {
        assert nodeId != null;
        assert updateSeq > 0;

        this.nodeId = nodeId;
        this.updateSeq = updateSeq;

        map = U.newHashMap(m.size());

        for (Map.Entry<Integer, GridDhtPartitionState> e : m.entrySet()) {
            GridDhtPartitionState state = e.getValue();

            if (!onlyActive || state.active())
                put(e.getKey(), state);
        }
    }

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    public GridDhtPartitionMap() {
        // No-op.
    }

    /**
     * @param part Partition.
     * @param state Partition state.
     */
    public void put(Integer part, GridDhtPartitionState state) {
        GridDhtPartitionState old = map.put(part, state);

        if (old == MOVING)
            moving--;

        if (state == MOVING)
            moving++;
    }

    /**
     * @return {@code true} If partition map contains moving partitions.
     */
    public boolean hasMovingPartitions() {
        assert moving >= 0 : moving;

        return moving != 0;
    }

    /**
     * @param part Partition.
     * @return Partition state.
     */
    public GridDhtPartitionState get(Integer part) {
        return map.get(part);
    }

    /**
     * @param part Partition.
     * @return {@code True} if contains given partition.
     */
    public boolean containsKey(Integer part) {
        return map.containsKey(part);
    }

    /**
     * @return Entries.
     */
    public Set<Map.Entry<Integer, GridDhtPartitionState>> entrySet() {
        return map.entrySet();
    }

    /**
     * @return Map size.
     */
    public int size() {
        return map.size();
    }

    /**
     * @return Partitions.
     */
    public Set<Integer> keySet() {
        return map.keySet();
    }

    /**
     * @return Underlying map.
     */
    public Map<Integer, GridDhtPartitionState> map() {
        return map;
    }

    /**
     * @return Node ID.
     */
    public UUID nodeId() {
        return nodeId;
    }

    /**
     * @return Update sequence.
     */
    public long updateSequence() {
        return updateSeq;
    }

    /**
     * @param updateSeq New update sequence value.
     * @return Old update sequence value.
     */
    public long updateSequence(long updateSeq) {
        long old = this.updateSeq;

        assert updateSeq >= old : "Invalid update sequence [cur=" + old + ", new=" + updateSeq + ']';

        this.updateSeq = updateSeq;

        return old;
    }

    /** {@inheritDoc} */
    @Override public int compareTo(GridDhtPartitionMap o) {
        assert nodeId.equals(o.nodeId);

        return Long.compare(updateSeq, o.updateSeq);
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

        GridDhtPartitionMap other = (GridDhtPartitionMap)o;

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
        return S.toString(GridDhtPartitionMap.class, this, "size", size(), "map", map.toString());
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDhtPartitionMap.class, this, "size", size());
    }
}