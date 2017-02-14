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
import java.util.Set;
import java.util.UUID;

import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionState;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteProductVersion;

import static org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionState.MOVING;

/**
 * Partition map.
 */
public class GridDhtPartitionMap2 implements Comparable<GridDhtPartitionMap2>, Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Used since. */
    public static final IgniteProductVersion SINCE = IgniteProductVersion.fromString("1.5.0");

    /** Node ID. */
    protected UUID nodeId;

    /** Update sequence number. */
    protected long updateSeq;

    /** Topology version. */
    protected AffinityTopologyVersion top;

    /** */
    protected Map<Integer, GridDhtPartitionState> map;

    /** */
    private volatile int moving;

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    public GridDhtPartitionMap2() {
        // No-op.
    }

    /**
     * @param nodeId Node ID.
     * @param updateSeq Update sequence number.
     * @param top Topology version.
     * @param m Map to copy.
     * @param onlyActive If {@code true}, then only active states will be included.
     */
    public GridDhtPartitionMap2(UUID nodeId,
        long updateSeq,
        AffinityTopologyVersion top,
        Map<Integer, GridDhtPartitionState> m,
        boolean onlyActive) {
        assert nodeId != null;
        assert updateSeq > 0;

        this.nodeId = nodeId;
        this.updateSeq = updateSeq;
        this.top = top;

        map = U.newHashMap(m.size());

        for (Map.Entry<Integer, GridDhtPartitionState> e : m.entrySet()) {
            GridDhtPartitionState state = e.getValue();

            if (!onlyActive || state.active())
                put(e.getKey(), state);
        }
    }

    /**
     * @param nodeId Node ID.
     * @param updateSeq Update sequence number.
     * @param top Topology version.
     * @param map Map.
     * @param moving Number of moving partitions.
     */
    private GridDhtPartitionMap2(UUID nodeId,
        long updateSeq,
        AffinityTopologyVersion top,
        Map<Integer, GridDhtPartitionState> map,
        int moving) {
        this.nodeId = nodeId;
        this.updateSeq = updateSeq;
        this.top = top;
        this.map = map;
        this.moving = moving;
    }

    /**
     * @return Copy with empty partition state map.
     */
    public GridDhtPartitionMap2 emptyCopy() {
        return new GridDhtPartitionMap2(nodeId,
            updateSeq,
            top,
            U.<Integer, GridDhtPartitionState>newHashMap(0),
            0);
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
     * @param topVer Current topology version.
     * @return Old update sequence value.
     */
    public long updateSequence(long updateSeq, AffinityTopologyVersion topVer) {
        long old = this.updateSeq;

        assert updateSeq >= old : "Invalid update sequence [cur=" + old + ", new=" + updateSeq + ']';

        this.updateSeq = updateSeq;

        top = topVer;

        return old;
    }

    /**
     * @return Topology version.
     */
    public AffinityTopologyVersion topologyVersion() {
        return top;
    }

    /** {@inheritDoc} */
    @Override public int compareTo(GridDhtPartitionMap2 o) {
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
            assert entry.getKey() < CacheConfiguration.MAX_PARTITIONS_COUNT : entry.getKey();

            int coded = (ordinal << 14) | entry.getKey();

            out.writeShort((short)coded);

            i++;
        }

        assert i == size;

        if (top != null) {
            out.writeLong(topologyVersion().topologyVersion());
            out.writeInt(topologyVersion().minorTopologyVersion());
        }
        else {
            out.writeLong(0);
            out.writeInt(0);
        }
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

        long ver = in.readLong();
        int minorVer = in.readInt();

        if (ver != 0)
            top = new AffinityTopologyVersion(ver, minorVer);
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
        return S.toString(GridDhtPartitionMap2.class, this, "size", size(), "map", map.toString(), "top", top);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDhtPartitionMap2.class, this, "size", size());
    }
}
