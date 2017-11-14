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
import java.util.UUID;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;

/** The base class for shared lock state. */
public abstract class GridCacheLockState2Base<T> extends VolatileAtomicDataStructureValue {
    /** */
    private long gridStartTime;

    /** Queue containing nodes that are waiting to acquire this lock. */
    @GridToStringInclude
    public ArrayDeque<T> nodes;

    /** For fast contains. */
    public HashSet<T> nodesSet;

    /**
     * Constructor.
     *
     * @param gridStartTime Cluster start time.
     */
    public GridCacheLockState2Base(long gridStartTime) {
        nodes = new ArrayDeque<>();
        nodesSet = new HashSet<>();

        this.gridStartTime = gridStartTime;
    }

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    public GridCacheLockState2Base() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int result = (int)(gridStartTime ^ (gridStartTime >>> 32));

        result = 31 * result + (nodes != null ? nodes.hashCode() : 0);

        return result;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        GridCacheLockState2Base state = (GridCacheLockState2Base)o;

        if (nodes != null ? !nodes.equals(state.nodes) : state.nodes != null)
            return false;

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

    /** Will take lock if it is free. */
    public LockedModified lockIfFree(T owner) {
        LockedModified result = new LockedModified(true, false);

        if (nodes == null) {
            nodes = new ArrayDeque<>();
        }

        if (nodesSet == null)
            nodesSet = new HashSet<>();

        if (nodes.isEmpty()) {
            nodes.add(owner);
            nodesSet.add(owner);

            result.modified = true;

            return result;
        }

        if (nodes.getFirst().equals(owner))
            return result;

        result.locked = false;

        return result;
    }

    /** Will take lock if it is free, or remove node from the waiting queue. */
    public LockedModified lockOrRemove(T owner) {
        LockedModified result = new LockedModified(true, false);

        if (nodes == null) {
            nodes = new ArrayDeque<>();
        }

        if (nodesSet == null)
            nodesSet = new HashSet<>();

        if (nodes.isEmpty()) {
            nodes.add(owner);
            nodesSet.add(owner);

            result.modified = true;

            return result;
        }

        if (nodes.getFirst().equals(owner))
            return result;

        result.locked = false;

        if (nodesSet.remove(owner)) {
            nodes.remove(owner);
            result.modified = true;
        }

        return result;
    }

    /** Will take lock if it is free, or will add node to the waiting queue. */
    public LockedModified lockOrAdd(T owner) {
        LockedModified result = new LockedModified(true, false);

        if (nodes == null) {
            nodes = new ArrayDeque<>();
        }

        if (nodesSet == null)
            nodesSet = new HashSet<>();

        if (nodes.isEmpty()) {
            nodes.add(owner);
            nodesSet.add(owner);

            result.modified = true;

            return result;
        }

        if (nodes.getFirst().equals(owner))
            return result;

        // Optimization for fast contains.
        if (!nodesSet.contains(owner)) {
            nodes.add(owner);
            nodesSet.add(owner);

            result.modified = true;
        }

        result.locked = false;

        return result;
    }

    /** Remove node from first position in waiting list. */
    public T unlock(T owner) {
        if (nodes == null || nodes.isEmpty() || !nodes.getFirst().equals(owner))
            return null;

        nodesSet.remove(nodes.removeFirst());

        if (nodes.isEmpty())
            return null;

        return nodes.getFirst();
    }

    /**
     * Remove all lock-owners from one node.
     *
     * @param id failed node.
     * @return a lock-owner which can take lock cause other node has failed.
     */
    public abstract T onNodeRemoved(UUID id);

    /** Write T object to stream. */
    protected abstract void writeItem(ObjectOutput out, T item) throws IOException;

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeLong(gridStartTime);

        out.writeBoolean(nodes != null);

        if (nodes != null) {
            out.writeInt(nodes.size());

            for (T item : nodes) {
                writeItem(out, item);
            }
        }
    }

    /** Read T object from stream. */
    protected abstract T readItem(ObjectInput in) throws IOException;

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException {
        gridStartTime = in.readLong();

        if (in.readBoolean()) {
            int size = in.readInt();

            nodes = new ArrayDeque<>(size);

            for (int i = 0; i < size; i++)
                nodes.add(readItem(in));

            nodesSet = new HashSet<>(nodes);
        }
        else {
            nodes = null;
            nodesSet = null;
        }
    }
}
