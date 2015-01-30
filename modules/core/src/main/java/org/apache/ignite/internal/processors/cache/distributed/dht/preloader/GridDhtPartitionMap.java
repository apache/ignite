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

import org.apache.ignite.internal.processors.cache.distributed.dht.*;
import org.apache.ignite.internal.util.typedef.internal.*;

import java.io.*;
import java.util.*;

/**
 * Partition map.
 */
public class GridDhtPartitionMap extends HashMap<Integer, GridDhtPartitionState>
    implements Comparable<GridDhtPartitionMap>, Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Node ID. */
    private UUID nodeId;

    /** Update sequence number. */
    private long updateSeq;

    /**
     * @param nodeId Node ID.
     * @param updateSeq Update sequence number.
     */
    public GridDhtPartitionMap(UUID nodeId, long updateSeq) {
        assert nodeId != null;
        assert updateSeq > 0;

        this.nodeId = nodeId;
        this.updateSeq = updateSeq;
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

        return updateSeq < o.updateSeq ? -1 : updateSeq == o.updateSeq ? 0 : 1;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        U.writeUuid(out, nodeId);

        out.writeLong(updateSeq);

        U.writeMap(out, this);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        nodeId = U.readUuid(in);

        updateSeq = in.readLong();

        putAll(U.<Integer, GridDhtPartitionState>readMap(in));
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
        return S.toString(GridDhtPartitionMap.class, this, "size", size(), "map", super.toString());
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDhtPartitionMap.class, this, "size", size());
    }
}
