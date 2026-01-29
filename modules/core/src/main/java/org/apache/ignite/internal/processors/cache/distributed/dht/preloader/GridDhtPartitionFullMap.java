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
import java.util.AbstractMap;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.jetbrains.annotations.NotNull;

/**
 * Full partition map from all nodes.
 */
public class GridDhtPartitionFullMap
    extends AbstractMap<UUID, GridDhtPartitionMap> implements Comparable<GridDhtPartitionFullMap>, Externalizable, Message {
    /** Type code. */
    public static final short TYPE_CODE = 519;

    /** */
    private static final long serialVersionUID = 0L;

    /** Node ID. */
    @Order(0)
    private UUID nodeId;

    /** Node order. */
    @Order(1)
    private long nodeOrder;

    /** Update sequence number. */
    @Order(value = 2, method = "updateSequence")
    private long updateSeq;

    /** Partition map. */
    @Order(3)
    private Map<UUID, GridDhtPartitionMap> map;

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
        map = new HashMap<>();
    }

    /**
     * @param nodeId Node ID.
     * @param nodeOrder Node order.
     * @param updateSeq Update sequence number.
     * @param m Map to copy.
     * @param onlyActive If {@code true}, then only active partitions will be included.
     */
    public GridDhtPartitionFullMap(UUID nodeId, long nodeOrder, long updateSeq, Map<UUID, GridDhtPartitionMap> m,
        boolean onlyActive) {
        assert nodeId != null;
        assert updateSeq > 0;
        assert nodeOrder > 0;

        this.nodeId = nodeId;
        this.nodeOrder = nodeOrder;
        this.updateSeq = updateSeq;
        map = new HashMap<>();

        for (Map.Entry<UUID, GridDhtPartitionMap> e : m.entrySet()) {
            GridDhtPartitionMap part = e.getValue();

            GridDhtPartitionMap cpy = new GridDhtPartitionMap(part.nodeId(),
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
        map = new HashMap<>(m);

        nodeId = m.nodeId;
        nodeOrder = m.nodeOrder;
        this.updateSeq = updateSeq;
    }

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    public GridDhtPartitionFullMap() {
        map = new HashMap<>();
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
     * @param nodeId Node ID.
     */
    public void nodeId(UUID nodeId) {
        this.nodeId = nodeId;
    }

    /**
     * @return Node order.
     */
    public long nodeOrder() {
        return nodeOrder;
    }

    /**
     * @param nodeOrder Node order.
     */
    public void nodeOrder(long nodeOrder) {
        this.nodeOrder = nodeOrder;
    }

    /**
     * @return Update sequence.
     */
    public long updateSequence() {
        return updateSeq;
    }

    /**
     * @return Partition map.
     */
    public Map<UUID, GridDhtPartitionMap> map() {
        return map;
    }

    /**
     * @param map Partition map.
     */
    public void map(Map<UUID, GridDhtPartitionMap> map) {
        this.map = map;
    }

    /**
     * @param fullMap Map.
     * @return {@code True} if this map and given map contain the same data.
     */
    public boolean partitionStateEquals(GridDhtPartitionFullMap fullMap) {
        if (size() != fullMap.size())
            return false;

        for (Map.Entry<UUID, GridDhtPartitionMap> e : entrySet()) {
            GridDhtPartitionMap m = fullMap.get(e.getKey());

            if (m == null || !m.map().equals(e.getValue().map()))
                return false;
        }

        return true;
    }

    /**
     * @param updateSeq New update sequence value.
     */
    public void updateSequence(long updateSeq) {
        this.updateSeq = updateSeq;
    }

    /**
     * @param updateSeq New update sequence value.
     * @return Old update sequence value.
     */
    public long checkAndUpdateSequence(long updateSeq) {
        long old = this.updateSeq;

        assert updateSeq >= old : "Invalid update sequence [cur=" + old + ", new=" + updateSeq +
            ", partMap=" + toFullString() + ']';

        this.updateSeq = updateSeq;

        return old;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        U.writeUuid(out, nodeId);

        out.writeLong(nodeOrder);
        out.writeLong(updateSeq);

        U.writeMap(out, map);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        nodeId = U.readUuid(in);

        nodeOrder = in.readLong();
        updateSeq = in.readLong();

        map = new HashMap<>();

        Map<UUID, GridDhtPartitionMap> map0 = U.readMap(in);

        if (map0 != null)
            map.putAll(map0);
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
        Iterator<Map.Entry<UUID, GridDhtPartitionMap>> it = entrySet().iterator();

        if (!it.hasNext())
            return "{}";

        StringBuilder buf = new StringBuilder();

        buf.append('{');

        while (true) {
            Map.Entry<UUID, GridDhtPartitionMap> e = it.next();

            UUID nodeId = e.getKey();

            GridDhtPartitionMap partMap = e.getValue();

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

    /** {@inheritDoc} */
    @Override public int compareTo(@NotNull GridDhtPartitionFullMap o) {
        assert nodeId.equals(o.nodeId);

        return Long.compare(updateSeq, o.updateSeq);
    }

    /** {@inheritDoc} */
    @Override public GridDhtPartitionMap put(UUID key, GridDhtPartitionMap val) {
        return map.put(key, val);
    }

    /** {@inheritDoc} */
    @Override public void putAll(Map<? extends UUID, ? extends GridDhtPartitionMap> m) {
        map.putAll(m);
    }

    /** {@inheritDoc} */
    @Override public GridDhtPartitionMap get(Object key) {
        return map.get(key);
    }

    /** {@inheritDoc} */
    @Override public GridDhtPartitionMap remove(Object key) {
        return map.remove(key);
    }

    /** {@inheritDoc} */
    @Override public int size() {
        return map.size();
    }

    /** {@inheritDoc} */
    @Override public boolean isEmpty() {
        return map.isEmpty();
    }

    /** {@inheritDoc} */
    @Override public boolean containsValue(Object val) {
        return map.containsValue(val);
    }

    /** {@inheritDoc} */
    @Override public boolean containsKey(Object key) {
        return map.containsKey(key);
    }

    /** {@inheritDoc} */
    @Override public void clear() {
        map.clear();
    }

    /** {@inheritDoc} */
    @Override public @NotNull Set<UUID> keySet() {
        return map.keySet();
    }

    /** {@inheritDoc} */
    @Override public @NotNull Collection<GridDhtPartitionMap> values() {
        return map.values();
    }

    /** {@inheritDoc} */
    @Override public @NotNull Set<Entry<UUID, GridDhtPartitionMap>> entrySet() {
        return map.entrySet();
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return TYPE_CODE;
    }
}
