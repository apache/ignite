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
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.internal.processors.cache.GridCacheInternal;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

/**
 * Grid cache semaphore state.
 */
public class GridCacheSemaphoreState implements GridCacheInternal, Externalizable, Cloneable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Permission count. */
    private int count;

    /** Map containing number of acquired permits for each node waiting on this semaphore. */
    @GridToStringInclude
    private Map<UUID,Integer> waiters;

    /** FailoverSafe flag. */
    private boolean failoverSafe;

    /**
     * Constructor.
     *
     * @param count Number of permissions.
     */
    public GridCacheSemaphoreState(int count, @Nullable Map<UUID,Integer> waiters, boolean failoverSafe) {
        this.count = count;
        this.waiters = waiters;
        this.failoverSafe = failoverSafe;
    }

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    public GridCacheSemaphoreState() {
        // No-op.
    }

    /**
     * @param count New count.
     */
    public void setCount(int count) {
        this.count = count;
    }

    /**
     * @return Current count.
     */
    public int getCount() {
        return count;
    }

    /**
     * @return Waiters.
     */
    public Map<UUID,Integer> getWaiters() {
        return waiters;
    }

    /**
     * @param waiters Map containing the number of permissions acquired by each node.
     */
    public void setWaiters(Map<UUID, Integer> waiters) {
        this.waiters = waiters;
    }

    /**
     * @return failoverSafe flag.
     */
    public boolean isFailoverSafe() {
        return failoverSafe;
    }

    /** {@inheritDoc} */
    @Override public Object clone() throws CloneNotSupportedException {
        return super.clone();
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(count);
        out.writeBoolean(failoverSafe);
        out.writeBoolean(waiters != null);
        if (waiters != null) {
            out.writeInt(waiters.size());
            for (UUID uuid : waiters.keySet()) {
                out.writeUTF(uuid.toString());
                out.writeInt(waiters.get(uuid));
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException {
        count = in.readInt();
        failoverSafe = in.readBoolean();
        if(in.readBoolean()) {
            int size = in.readInt();
            waiters = new HashMap<>();
            for (int i = 0; i < size; i++) {
                UUID uuid = UUID.fromString(in.readUTF());
                int permits = in.readInt();
                waiters.put(uuid, permits);
            }
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheSemaphoreState.class, this);
    }
}
