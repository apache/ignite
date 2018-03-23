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
import java.util.Iterator;
import java.util.Objects;
import java.util.UUID;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.jetbrains.annotations.Nullable;

/**
 * The base class for shared lock state.
 *
 * @param <T> Lock owner.
 */
abstract class GridCacheLockState2Base<T> extends VolatileAtomicDataStructureValue {
    /** Queue containing nodes that are waiting to acquire this lock. */
    @GridToStringInclude
    protected ArrayDeque<T> owners;
    /** For fast {@link HashSet#contains}. */
    protected HashSet<T> ownerSet;
    /** Set of nodes for safety remove this state */
    protected HashSet<UUID> nodes;
    /** */
    protected long gridStartTime;

    /**
     * Constructor.
     *
     * @param gridStartTime Cluster start time.
     */
    GridCacheLockState2Base(long gridStartTime) {
        owners = new ArrayDeque<>();
        ownerSet = new HashSet<>();
        nodes = new HashSet<>();

        this.gridStartTime = gridStartTime;
    }

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    GridCacheLockState2Base() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int res = (int) (gridStartTime ^ (gridStartTime >>> 32));

        res = 31 * res + (owners != null ? owners.hashCode() : 0);

        return res;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        GridCacheLockState2Base state = (GridCacheLockState2Base) o;


        if (ownerSet.size() != state.ownerSet.size())
            return false;

        Iterator<T> iter1 = owners.iterator();
        Iterator<T> iter2 = state.owners.iterator();

        while (iter1.hasNext() && iter2.hasNext()) {
            if (!Objects.equals(iter1.next(), iter2.next()))
                return false;
        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public DataStructureType type() {
        return DataStructureType.REENTRANT_LOCK2;
    }

    /** {@inheritDoc} */
    @Override public long gridStartTime() {
        return gridStartTime;
    }

    /** Add node. */
    boolean addNode(UUID node) {
        return nodes.add(node);
    }

    /** Has node. */
    boolean hasNode(UUID node) {
        return nodes.contains(node);
    }

    /** Create partial copy with one more node. */
    abstract GridCacheLockState2Base<T> withNode(UUID node);

    /** Remove node. */
    void removeNode(UUID node) {
        nodes.remove(node);
    }

    /** Can remove the state. */
    boolean canRemove() {
        return nodes.isEmpty();
    }

    /** Will take lock if it is free. */
    LockStateUpdateResult lockIfFree(T owner) {
        LockStateUpdateResult res = new LockStateUpdateResult(true, false);

        if (owners == null)
            owners = new ArrayDeque<>();

        if (ownerSet == null)
            ownerSet = new HashSet<>();

        if (owners.isEmpty()) {
            owners.add(owner);
            ownerSet.add(owner);

            res.modified = true;

            return res;
        }

        if (owners.getFirst().equals(owner))
            return res;

        res.locked = false;

        return res;
    }

    /** Will take lock if it is free, or remove node from the waiting queue. */
    LockStateUpdateResult lockOrRemove(T owner) {
        LockStateUpdateResult res = new LockStateUpdateResult(true, false);

        if (owners == null)
            owners = new ArrayDeque<>();

        if (ownerSet == null)
            ownerSet = new HashSet<>();

        if (owners.isEmpty()) {
            owners.add(owner);
            ownerSet.add(owner);

            res.modified = true;

            return res;
        }

        if (owners.getFirst().equals(owner))
            return res;

        res.locked = false;

        if (ownerSet.remove(owner)) {
            owners.remove(owner);
            res.modified = true;
        }

        return res;
    }

    /** Will take lock if it is free, or will add node to the waiting queue. */
    LockStateUpdateResult lockOrAdd(T owner) {
        LockStateUpdateResult res = new LockStateUpdateResult(true, false);

        if (owners == null)
            owners = new ArrayDeque<>();

        if (ownerSet == null)
            ownerSet = new HashSet<>();

        if (owners.isEmpty()) {
            owners.add(owner);
            ownerSet.add(owner);

            res.modified = true;

            return res;
        }

        if (owners.getFirst().equals(owner))
            return res;

        // Optimization for fast contains.
        if (!ownerSet.contains(owner)) {
            owners.add(owner);
            ownerSet.add(owner);

            res.modified = true;
        }

        res.locked = false;

        return res;
    }

    /** Remove node from first position in waiting list. */
    @Nullable T unlock(T owner) {
        if (owners == null || owners.isEmpty() || !owners.getFirst().equals(owner))
            return null;

        ownerSet.remove(owners.removeFirst());

        if (owners.isEmpty())
            return null;

        return owners.getFirst();
    }

    /**
     * Remove all lock-owners from one node.
     *
     * @param id Failed node.
     * @return A lock-owner which can take lock cause other node has failed.
     */
    @Nullable abstract T onNodeRemoved(UUID id);

    /** Write T object to stream. */
    protected abstract void writeItem(ObjectOutput out, T item) throws IOException;

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeLong(gridStartTime);

        assert owners != null;

        out.writeInt(owners.size());

        for (T item : owners)
            writeItem(out, item);

        assert nodes != null;

        out.writeInt(nodes.size());

        for (UUID node : nodes) {
            out.writeLong(node.getMostSignificantBits());
            out.writeLong(node.getLeastSignificantBits());
        }
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException {
        gridStartTime = in.readLong();

        int size = in.readInt();

        owners = new ArrayDeque<>(size);

        for (int i = 0; i < size; i++)
            owners.add(readItem(in));

        ownerSet = new HashSet<>(owners);

        int nodesSize = in.readInt();

        nodes = new HashSet<>(nodesSize);

        for (int i = 0; i < nodesSize; i++)
            nodes.add(new UUID(in.readLong(), in.readLong()));
    }

    /** Read T object from stream. */
    protected abstract T readItem(ObjectInput in) throws IOException;

    /** */
    protected abstract boolean checkConsistency();
}
