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

package org.apache.ignite.internal.processors.datastructures;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayDeque;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.UUID;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 *  Grid cache reentrant lock state.
 */
public final class GridCacheLockState2 extends VolatileAtomicDataStructureValue implements Cloneable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private long gridStartTime;

    /** Queue containing nodes that are waiting to acquire this lock, used to ensure fairness. */
    @GridToStringInclude
    public ArrayDeque<UUID> nodes;

    /** For fast contains. */
    public HashSet<UUID> nodesSet;

    @Override public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        GridCacheLockState2 state = (GridCacheLockState2)o;

        if (nodes != null ? !nodes.equals(state.nodes) : state.nodes != null)
            return false;

        return true;
    }

    /**
     * Constructor.
     *
     * @param gridStartTime Cluster start time.
     */
    public GridCacheLockState2(long gridStartTime) {
        nodes = new ArrayDeque<>();
        nodesSet = new HashSet<>();

        this.gridStartTime = gridStartTime;
    }

    /** Clone constructor. */
    private GridCacheLockState2(GridCacheLockState2 state) {
        nodes = new ArrayDeque<>(state.nodes);
        nodesSet = new HashSet<>(state.nodesSet);
    }

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    public GridCacheLockState2() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public DataStructureType type() {
        return DataStructureType.REENTRANT_LOCK2;
    }

    /** {@inheritDoc} */
    @Override public long gridStartTime() {
        return gridStartTime;
    }

    /** Will take lock if it is free. */
    public LockedModified lockIfFree(UUID nodeId) {
        LockedModified result = new LockedModified(true, false);

        if (nodes == null) {
            nodes = new ArrayDeque<>();
        }

        if (nodesSet == null)
            nodesSet = new HashSet<>();

        if (nodes.isEmpty()) {
            nodes.add(nodeId);
            nodesSet.add(nodeId);

            result.modified = true;

            return result;
        }

        if (nodes.getFirst().equals(nodeId))
            return result;

        result.locked = false;

        return result;
    }

    /** Will take lock if it is free, or remove node from the waiting queue. */
    public LockedModified lockOrRemove(UUID nodeId) {
        LockedModified result = new LockedModified(true, false);

        if (nodes == null) {
            nodes = new ArrayDeque<>();
        }

        if (nodesSet == null)
            nodesSet = new HashSet<>();

        if (nodes.isEmpty()) {
            nodes.add(nodeId);
            nodesSet.add(nodeId);

            result.modified = true;

            return result;
        }

        if (nodes.getFirst().equals(nodeId))
            return result;

        result.locked = false;

        if (nodesSet.remove(nodeId)) {
            nodes.remove(nodeId);
            result.modified = true;
        }

        return result;
    }

    /** Will take lock if it is free, or will add node to the waiting queue. */
    public LockedModified lockOrAdd(UUID nodeId) {
        LockedModified result = new LockedModified(true, false);

        if (nodes == null) {
            nodes = new ArrayDeque<>();
        }

        if (nodesSet == null)
            nodesSet = new HashSet<>();

        if (nodes.isEmpty()) {
            nodes.add(nodeId);
            nodesSet.add(nodeId);

            result.modified = true;

            return result;
        }

        if (nodes.getFirst().equals(nodeId))
            return result;

        if (!nodesSet.contains(nodeId)) {
            nodes.add(nodeId);
            nodesSet.add(nodeId);

            result.modified = true;
        }

        result.locked = false;

        return result;
    }

    /** Remove node from first position in waiting list. */
    public UUID unlock(UUID nodeId) {
        if (nodes == null || nodes.isEmpty() || !nodes.getFirst().equals(nodeId))
            return null;

        nodesSet.remove(nodes.removeFirst());

        if (nodes.isEmpty())
            return null;

        return nodes.getFirst();
    }

    /** {@inheritDoc} */
    @Override public Object clone() throws CloneNotSupportedException {
        return new GridCacheLockState2(this);
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeLong(gridStartTime);

        out.writeBoolean(nodes != null);

        if (nodes != null) {
            out.writeInt(nodes.size());

            for (UUID uuid: nodes)
                U.writeUuid(out, uuid);
        }
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException {
        gridStartTime = in.readLong();

        if (in.readBoolean()) {
            int size = in.readInt();

            nodes = new ArrayDeque<>();

            for (int i = 0; i < size; i++)
                nodes.add(U.readUuid(in));
            nodesSet = new HashSet<>(nodes);
        }
        else {
            nodes = null;
            nodesSet = null;
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheLockState2.class, this);
    }

    /** Simple tuple for result. */
    public static class LockedModified {
        public boolean locked;
        public boolean modified;

        public LockedModified(boolean locked, boolean modified) {
            this.locked = locked;
            this.modified = modified;
        }
    }
}
