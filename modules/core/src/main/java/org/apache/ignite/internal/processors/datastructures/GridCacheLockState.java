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
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.internal.processors.cache.GridCacheInternal;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 *  Grid cache reentrant lock state.
 */
public final class GridCacheLockState implements GridCacheInternal, Externalizable, Cloneable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Count. */
    private int cnt;

    /** Owner thread local id. */
    private long threadId;

    /** Owner node ID. */
    private UUID id;

    /** FailoverSafe flag. */
    private boolean failoverSafe;

    /** Map containing state for each condition object associated with this lock. */
    @GridToStringInclude
    private Map<String, LinkedList<UUID>> conditionMap;

    /** Map containing unprocessed signals for condition objects that are associated with this lock. */
    @GridToStringInclude
    private Map<UUID, LinkedList<String>> signals;

    /**
     * Constructor.
     *
     * @param cnt Initial count.
     * @param id UUID of owning node.
     */
    public GridCacheLockState(int cnt, UUID id, long threadID, boolean failoverSafe) {
        assert cnt >= 0;

        this.id = id;

        this.threadId = threadID;

        conditionMap = new HashMap();

        signals = null;

        this.failoverSafe = failoverSafe;
    }

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    public GridCacheLockState() {
        // No-op.
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
     * @return Current owner thread id.
     */
    public long getThreadId() {
        return threadId;
    }

    /**
     * @param threadId New thread owner id.
     */
    public void setThreadId(long threadId) {
        this.threadId = threadId;
    }

    /**
     * @return Current owner node id.
     */
    public UUID getId() {
        return id;
    }

    /**
     * @return New owner node id.
     */
    public void setId(UUID id) {
        this.id = id;
    }

    public boolean isFailoverSafe() {
        return failoverSafe;
    }

    public int condtionCount(){
        return conditionMap.size();
    }

    public Map<String, LinkedList<UUID>> getConditionMap() {
        return conditionMap;
    }

    public void setConditionMap(Map<String, LinkedList<UUID>> conditionMap) {
        this.conditionMap = conditionMap;
    }

    public Map<UUID, LinkedList<String>> getSignals() {
        return signals;
    }

    public void setSignals(Map<UUID, LinkedList<String>> signals) {
        this.signals = signals;
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

        out.writeBoolean(failoverSafe);

        out.writeBoolean(conditionMap != null);

        if (conditionMap != null) {
            out.writeInt(conditionMap.size());

            for (Map.Entry<String, LinkedList<UUID>> e : conditionMap.entrySet()) {
                U.writeString(out, e.getKey());

                out.writeInt(e.getValue().size());

                for(UUID uuid:e.getValue()){
                    U.writeUuid(out,uuid);
                }
            }
        }

        out.writeBoolean(signals != null);

        if (signals != null) {
            out.writeInt(signals.size());

            for (Map.Entry<UUID, LinkedList<String>> e : signals.entrySet()) {
                U.writeUuid(out, e.getKey());

                out.writeInt(e.getValue().size());

                for(String condition:e.getValue()){
                    U.writeString(out, condition);
                }
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException {
        cnt = in.readInt();
        threadId = in.readLong();
        id = U.readUuid(in);

        failoverSafe = in.readBoolean();

        if (in.readBoolean()) {
            int size = in.readInt();

            conditionMap = U.newLinkedHashMap(size);

            for (int i = 0; i < size; i++) {
                String key = U.readString(in);

                int size1 = in.readInt();

                LinkedList<UUID> list = new LinkedList();

                for (int j = 0; j < size1; j++) {
                    list.add(U.readUuid(in));
                }

                conditionMap.put(key, list);
            }
        }

        if(in.readBoolean()) {
            assert (conditionMap != null);

            int size = in.readInt();

            signals = U.newLinkedHashMap(size);

            for (int i = 0; i < size; i++) {
                UUID node = U.readUuid(in);

                int size1 = in.readInt();

                LinkedList<String> list = new LinkedList();

                for (int j = 0; j < size1; j++) {
                    list.add(U.readString(in));
                }

                signals.put(node, list);
            }
        }
        else{
            signals = null;
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheLockState.class, this);
    }
}
