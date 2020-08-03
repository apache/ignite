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
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 *  Grid cache reentrant lock state.
 */
public final class GridCacheLockState extends VolatileAtomicDataStructureValue implements Cloneable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Count. */
    private int cnt;

    /** Owner thread local ID. */
    private long threadId;

    /** Owner node ID. */
    private UUID id;

    /** */
    private long gridStartTime;

    /** FailoverSafe flag. */
    private boolean failoverSafe;

    /** Map containing state for each condition object associated with this lock. */
    @GridToStringInclude
    private Map<String, LinkedList<UUID>> conditionMap;

    /** Map containing unprocessed signals for condition objects that are associated with this lock. */
    @GridToStringInclude
    private Map<UUID, LinkedList<String>> signals;

    /** Flag indicating lock fairness. */
    private boolean fair;

    /** Queue containing nodes that are waiting to acquire this lock, used to ensure fairness. */
    @GridToStringInclude
    private LinkedList<UUID> nodes;

    /**
     * Flag indicating that global state changed.
     * Used in fair mode to ensure that only successful acquires and releases trigger update.
     */
    private boolean changed;

    /**
     * Constructor.
     *
     * @param cnt Initial count.
     * @param id UUID of owning node.
     * @param threadID ID of the current thread.
     * @param failoverSafe true if created in failoverSafe mode.
     * @param fair true if created in fair mode.
     * @param gridStartTime Cluster start time.
     */
    public GridCacheLockState(int cnt, UUID id, long threadID, boolean failoverSafe, boolean fair, long gridStartTime) {
        assert cnt >= 0;

        this.id = id;

        this.threadId = threadID;

        conditionMap = new HashMap();

        signals = null;

        nodes = new LinkedList<UUID>();

        this.fair = fair;

        this.failoverSafe = failoverSafe;

        this.gridStartTime = gridStartTime;
    }

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    public GridCacheLockState() {
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

    /**
     * @param cnt New count.
     */
    public void set(int cnt) {
        this.cnt = cnt;
    }

    /**
     * @return Current count.
     */
    public int get() {
        return cnt;
    }

    /**
     * @return Current owner thread ID.
     */
    public long getThreadId() {
        return threadId;
    }

    /**
     * @param threadId New thread owner ID.
     */
    public void setThreadId(long threadId) {
        this.threadId = threadId;
    }

    /**
     * @return Current owner node ID.
     */
    public UUID getId() {
        return id;
    }

    /**
     * @param id New owner node ID.
     */
    public void setId(UUID id) {
        this.id = id;
    }

    /**
     * @return Failover safe flag.
     */
    public boolean isFailoverSafe() {
        return failoverSafe;
    }

    /**
     * @return Condition count.
     */
    public int condtionCount() {
        return conditionMap.size();
    }

    /**
     * @return Condition map.
     */
    public Map<String, LinkedList<UUID>> getConditionMap() {
        return conditionMap;
    }

    /**
     * @param conditionMap Condition map.
     */
    public void setConditionMap(Map<String, LinkedList<UUID>> conditionMap) {
        this.conditionMap = conditionMap;
    }

    /**
     * @return Signals.
     */
    public Map<UUID, LinkedList<String>> getSignals() {
        return signals;
    }

    /**
     * @param signals Signals.
     */
    public void setSignals(Map<UUID, LinkedList<String>> signals) {
        this.signals = signals;
    }

    /**
     * @return Nodes.
     */
    public LinkedList<UUID> getNodes() {
        return nodes;
    }

    /**
     * @param nodes Nodes.
     */
    public void setNodes(LinkedList<UUID> nodes) {
        this.nodes = nodes;
    }

    /**
     * @return Fair flag.
     */
    public boolean isFair() {
        return fair;
    }

    /**
     * @return Changed flag.
     */
    public boolean isChanged() {
        return changed;
    }

    /**
     * @param changed Changed flag.
     */
    public void setChanged(boolean changed) {
        this.changed = changed;
    }

    /** {@inheritDoc} */
    @Override public Object clone() throws CloneNotSupportedException {
        return super.clone();
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(cnt);
        out.writeLong(threadId);
        U.writeUuid(out, id);
        out.writeLong(gridStartTime);

        out.writeBoolean(failoverSafe);

        out.writeBoolean(fair);

        out.writeBoolean(changed);

        out.writeBoolean(conditionMap != null);

        if (conditionMap != null) {
            out.writeInt(conditionMap.size());

            for (Map.Entry<String, LinkedList<UUID>> e : conditionMap.entrySet()) {
                U.writeString(out, e.getKey());

                out.writeInt(e.getValue().size());

                for (UUID uuid:e.getValue())
                    U.writeUuid(out, uuid);
            }
        }

        out.writeBoolean(signals != null);

        if (signals != null) {
            out.writeInt(signals.size());

            for (Map.Entry<UUID, LinkedList<String>> e : signals.entrySet()) {
                U.writeUuid(out, e.getKey());

                out.writeInt(e.getValue().size());

                for (String condition:e.getValue())
                    U.writeString(out, condition);
            }
        }

        out.writeBoolean(nodes != null);

        if (nodes != null) {
            out.writeInt(nodes.size());

            for (UUID uuid: nodes)
                U.writeUuid(out, uuid);
        }
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException {
        cnt = in.readInt();
        threadId = in.readLong();
        id = U.readUuid(in);
        gridStartTime = in.readLong();

        failoverSafe = in.readBoolean();

        fair = in.readBoolean();

        changed = in.readBoolean();

        if (in.readBoolean()) {
            int size = in.readInt();

            conditionMap = U.newLinkedHashMap(size);

            for (int i = 0; i < size; i++) {
                String key = U.readString(in);

                int size1 = in.readInt();

                LinkedList<UUID> list = new LinkedList();

                for (int j = 0; j < size1; j++)
                    list.add(U.readUuid(in));

                conditionMap.put(key, list);
            }
        }

        if (in.readBoolean()) {
            assert (conditionMap != null);

            int size = in.readInt();

            signals = U.newLinkedHashMap(size);

            for (int i = 0; i < size; i++) {
                UUID node = U.readUuid(in);

                int size1 = in.readInt();

                LinkedList<String> list = new LinkedList();

                for (int j = 0; j < size1; j++)
                    list.add(U.readString(in));

                signals.put(node, list);
            }
        }
        else
            signals = null;

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
        return S.toString(GridCacheLockState.class, this);
    }
}
