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
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Full partition map.
 */
public class GridDhtPartitionFullMap extends HashMap<UUID, GridDhtPartitionMap2>
    implements Comparable<GridDhtPartitionFullMap>, Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Node ID. */
    private UUID nodeId;

    /** Node order. */
    private long nodeOrder;

    /** Update sequence number. */
    private long updateSeq;

    /**
     * @param nodeId Node ID.
     * @param nodeOrder Node order.
     * @param updateSeq Update sequence number.
     */
    public GridDhtPartitionFullMap(UUID nodeId, long nodeOrder, long updateSeq) {
        assert nodeId != null;
        assert nodeOrder > 0;
        assert updateSeq > 0;

        this.nodeId = nodeId;
        this.nodeOrder = nodeOrder;
        this.updateSeq = updateSeq;
    }

    /**
     * @param nodeId Node ID.
     * @param nodeOrder Node order.
     * @param updateSeq Update sequence number.
     * @param m Map to copy.
     */
    @Deprecated // Backward compatibility.
    public GridDhtPartitionFullMap(UUID nodeId, long nodeOrder, long updateSeq, Map<UUID, GridDhtPartitionMap2> m) {
        assert nodeId != null;
        assert updateSeq > 0;
        assert nodeOrder > 0;

        this.nodeId = nodeId;
        this.nodeOrder = nodeOrder;
        this.updateSeq = updateSeq;

        for (Map.Entry<UUID, GridDhtPartitionMap2> e : m.entrySet()) {
            GridDhtPartitionMap2 part = e.getValue();

            put(e.getKey(), new GridDhtPartitionMap(part.nodeId(), part.updateSequence(), part.map()));
        }
    }

    /**
     * @param nodeId Node ID.
     * @param nodeOrder Node order.
     * @param updateSeq Update sequence number.
     * @param m Map to copy.
     * @param onlyActive If {@code true}, then only active partitions will be included.
     */
    public GridDhtPartitionFullMap(UUID nodeId, long nodeOrder, long updateSeq, Map<UUID, GridDhtPartitionMap2> m,
        boolean onlyActive) {
        assert nodeId != null;
        assert updateSeq > 0;
        assert nodeOrder > 0;

        this.nodeId = nodeId;
        this.nodeOrder = nodeOrder;
        this.updateSeq = updateSeq;

        for (Map.Entry<UUID, GridDhtPartitionMap2> e : m.entrySet()) {
            GridDhtPartitionMap2 part = e.getValue();

            GridDhtPartitionMap2 cpy = new GridDhtPartitionMap2(part.nodeId(),
                part.updateSequence(),
                part.topologyVersion(),
                part.map(),
                onlyActive);

            put(e.getKey(), cpy);
        }
    }

    /**
     * @param m Map to copy.
     * @param updateSeq Update sequence.
     */
    public GridDhtPartitionFullMap(GridDhtPartitionFullMap m, long updateSeq) {
        super(m);

        nodeId = m.nodeId;
        nodeOrder = m.nodeOrder;
        this.updateSeq = updateSeq;
    }

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    public GridDhtPartitionFullMap() {
        // No-op.
    }

    /**
     * @return {@code True} if properly initialized.
     */
    public boolean valid() {
        return nodeId != null && nodeOrder > 0;
    }

    /**
     * @return Node ID.
     */
    public UUID nodeId() {
        return nodeId;
    }

    /**
     * @return Node order.
     */
    public long nodeOrder() {
        return nodeOrder;
    }

    /**
     * @return Update sequence.
     */
    public long updateSequence() {
        return updateSeq;
    }

    /**
     * @param fullMap Map.
     * @return {@code True} if this map and given map contain the same data.
     */
    public boolean partitionStateEquals(GridDhtPartitionFullMap fullMap) {
        if (size() != fullMap.size())
            return false;

        for (Map.Entry<UUID, GridDhtPartitionMap2> e : entrySet()) {
            GridDhtPartitionMap2 m = fullMap.get(e.getKey());

            if (m == null || !m.map().equals(e.getValue().map()))
                return false;
        }

        return true;
    }

    /**
     * @param updateSeq New update sequence value.
     */
    public void newUpdateSequence(long updateSeq) {
        this.updateSeq = updateSeq;
    }

    /**
     * @param updateSeq New update sequence value.
     * @return Old update sequence value.
     */
    public long updateSequence(long updateSeq) {
        long old = this.updateSeq;

        assert updateSeq >= old : "Invalid update sequence [cur=" + old + ", new=" + updateSeq +
            ", partMap=" + toFullString() + ']';

        this.updateSeq = updateSeq;

        return old;
    }

    /** {@inheritDoc} */
    @Override public int compareTo(GridDhtPartitionFullMap o) {
        assert nodeId == null || (nodeOrder != o.nodeOrder && !nodeId.equals(o.nodeId)) ||
            (nodeOrder == o.nodeOrder && nodeId.equals(o.nodeId)): "Inconsistent node order and ID [id1=" + nodeId +
                ", order1=" + nodeOrder + ", id2=" + o.nodeId + ", order2=" + o.nodeOrder + ']';

        if (nodeId == null && o.nodeId != null)
            return -1;
        else if (nodeId != null && o.nodeId == null)
            return 1;
        else if (nodeId == null && o.nodeId == null)
            return 0;

        int res = Long.compare(nodeOrder, o.nodeOrder);

        if (res == 0)
            res = Long.compare(updateSeq, o.updateSeq);

        return res;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        U.writeUuid(out, nodeId);

        out.writeLong(nodeOrder);
        out.writeLong(updateSeq);

        U.writeMap(out, this);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        nodeId = U.readUuid(in);

        nodeOrder = in.readLong();
        updateSeq = in.readLong();

        putAll(U.<UUID, GridDhtPartitionMap2>readMap(in));
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        GridDhtPartitionFullMap other = (GridDhtPartitionFullMap)o;

        return other.nodeId.equals(nodeId) && other.updateSeq == updateSeq;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return 31 * nodeId.hashCode() + (int)(updateSeq ^ (updateSeq >>> 32));
    }

    /**
     * @return Map string representation.
     */
    public String map2string() {
        Iterator<Map.Entry<UUID, GridDhtPartitionMap2>> it = entrySet().iterator();

        if (!it.hasNext())
            return "{}";

        StringBuilder buf = new StringBuilder();

        buf.append('{');

        while(true) {
            Map.Entry<UUID, GridDhtPartitionMap2> e = it.next();

            UUID nodeId = e.getKey();

            GridDhtPartitionMap2 partMap = e.getValue();

            buf.append(nodeId).append('=').append(partMap.toFullString());

            if (!it.hasNext())
                return buf.append('}').toString();

            buf.append(", ");
        }
    }

    /**
     * @return Full string representation.
     */
    public String toFullString() {
        return S.toString(GridDhtPartitionFullMap.class, this, "size", size(), "map", map2string());
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDhtPartitionFullMap.class, this, "size", size());
    }
}