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
import org.apache.ignite.internal.processors.cache.GridCacheInternal;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Grid cache semaphore state.
 */
public class GridCacheSemaphoreState implements GridCacheInternal, Externalizable, Cloneable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Permission count. */
    private int count;

    /** Waiter ID. */
    private int waiters;

    /** Fairness flag. */
    private boolean fair;

    /**
     * Constructor.
     *
     * @param count Number of permissions.
     */
    public GridCacheSemaphoreState(int count, int waiters) {
        this.count = count;
        this.waiters = waiters;
        this.fair = false;
    }

    /**
     * Constructor.
     *
     * @param count Number of permissions.
     */
    public GridCacheSemaphoreState(int count, int waiters, boolean fair) {
        this.count = count;
        this.waiters = waiters;
        this.fair = fair;
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
    public int getWaiters() {
        return waiters;
    }

    /**
     * @param id Waiters.
     */
    public void setWaiters(int id) {
        this.waiters = id;
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

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(count);
        out.writeInt(waiters);
        out.writeBoolean(fair);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException {
        count = in.readInt();
        waiters = in.readInt();
        fair = in.readBoolean();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheSemaphoreState.class, this);
    }
}
