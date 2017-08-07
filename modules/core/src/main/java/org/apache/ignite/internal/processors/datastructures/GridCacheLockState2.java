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

    /** FailoverSafe flag. */
    private boolean failoverSafe;

    /** Flag indicating lock fairness. */
    private boolean fair;

    /** Queue containing nodes that are waiting to acquire this lock, used to ensure fairness. */
    @GridToStringInclude
    public LinkedList<UUID> nodes;

    /**
     * Flag indicating that global state changed.
     * Used in fair mode to ensure that only successful acquires and releases trigger update.
     */
    //private boolean changed;

    @Override public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        GridCacheLockState2 state = (GridCacheLockState2)o;

        if (failoverSafe != state.failoverSafe)
            return false;
        if (fair != state.fair)
            return false;
        if (nodes != null ? !nodes.equals(state.nodes) : state.nodes != null)
            return false;

        return true;
    }

    /**
     * Constructor.
     *
     * @param failoverSafe true if created in failoverSafe mode.
     * @param fair true if created in fair mode.
     * @param gridStartTime Cluster start time.
     */
    public GridCacheLockState2(boolean failoverSafe, boolean fair, long gridStartTime) {
        nodes = new LinkedList<>();

        this.fair = fair;

        this.failoverSafe = failoverSafe;

        this.gridStartTime = gridStartTime;
    }

    private GridCacheLockState2(GridCacheLockState2 state) {
        failoverSafe = state.failoverSafe;

        fair = state.fair;
        nodes = new LinkedList<>(state.nodes);
    }

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    public GridCacheLockState2() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public DataStructureType type() {
        return DataStructureType.REENTRANT_LOCK;
    }

    /** {@inheritDoc} */
    @Override public long gridStartTime() {
        return gridStartTime;
    }

    /** */
    public boolean lockIfFree(UUID nodeId) {
        if (nodes == null)
            nodes = new LinkedList<>();

        if (nodes.isEmpty()) {
            nodes.add(nodeId);

            return true;
        }

        if (nodes.getFirst().equals(nodeId))
            return true;

        return false;
    }

    public void unlock(UUID nodeId) {
        if (nodes == null || nodes.isEmpty() || !nodes.getFirst().equals(nodeId))
            return;

        nodes.removeFirst();
    }

    public boolean remove(UUID nodeId) {
        return nodes.remove(nodeId);
    }

    public UUID getFirstNode() {
        if (nodes == null || nodes.isEmpty())
            return null;
        return nodes.getFirst();
    }

    /**
     * @return Failover safe flag.
     */
    public boolean isFailoverSafe() {
        return failoverSafe;
    }

    public boolean addToWaitingList(UUID nodeId) {
        if (nodes == null) {
            nodes = new LinkedList<>();
        } else if (nodes.contains(nodeId))
            return false;

        nodes.add(nodeId);
        return true;
    }

    /**
     * @return Fair flag.
     */
    public boolean isFair() {
        return fair;
    }

    /** {@inheritDoc} */
    @Override public Object clone() throws CloneNotSupportedException {
        return super.clone();
    }

    /** */
    public GridCacheLockState2 fastClone() {
        return new GridCacheLockState2(this);
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeLong(gridStartTime);

        out.writeBoolean(failoverSafe);

        out.writeBoolean(fair);

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

        failoverSafe = in.readBoolean();

        fair = in.readBoolean();

        if (in.readBoolean()) {
            int size = in.readInt();

            nodes = new LinkedList();

            for (int i = 0; i < size; i++)
                nodes.add(U.readUuid(in));
        }
        else
            nodes = null;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheLockState2.class, this);
    }
}
