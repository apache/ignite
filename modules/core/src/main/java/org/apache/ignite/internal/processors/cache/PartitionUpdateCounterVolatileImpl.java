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

package org.apache.ignite.internal.processors.cache;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.internal.util.GridEmptyIterator;
import org.apache.ignite.internal.util.GridLongList;
import org.jetbrains.annotations.Nullable;

/**
 * Partition update counter for volatile caches.
 * <p>
 * Doesn't track gaps in update sequence because it's not needed for volatile caches
 * (because their state is lost on node failure).
 * <p>
 * In this mode LWM and HWM are non distinguishable.
 */
public class PartitionUpdateCounterVolatileImpl implements PartitionUpdateCounter {
    /** Counter of applied updates in partition. */
    private final AtomicLong cntr = new AtomicLong();

    /**
     * Initial counter is set to update with max sequence number after WAL recovery.
     */
    private volatile long initCntr;

    /** */
    private final CacheGroupContext grp;

    /**
     * @param grp Group.
     */
    public PartitionUpdateCounterVolatileImpl(CacheGroupContext grp) {
        this.grp = grp;
    }

    /** {@inheritDoc} */
    @Override public void init(long initUpdCntr, @Nullable byte[] cntrUpdData) {
        cntr.set(initUpdCntr);

        initCntr = initUpdCntr;
    }

    /** {@inheritDoc} */
    @Override public long initial() {
        return initCntr;
    }

    /** {@inheritDoc} */
    @Override public long get() {
        return cntr.get();
    }

    /** {@inheritDoc} */
    @Override public long next() {
        return cntr.incrementAndGet();
    }

    /** {@inheritDoc} */
    @SuppressWarnings("StatementWithEmptyBody")
    @Override public void update(long val) {
        long cur;

        while (val > (cur = cntr.get()) && !cntr.compareAndSet(cur, val));
    }

    /** {@inheritDoc} */
    @Override public boolean update(long start, long delta) {
        update(start + delta);

        return false; // Prevents RollbackRecord in mixed tx-atomic mode.
    }

    /** {@inheritDoc} */
    @Override public synchronized void updateInitial(long start, long delta) {
        update(start + delta);

        initCntr = get();
    }

    /** {@inheritDoc} */
    @Override public GridLongList finalizeUpdateCounters() {
        return new GridLongList();
    }

    /** {@inheritDoc} */
    @Override public long reserve(long delta) {
        return next(delta);
    }

    /** {@inheritDoc} */
    @Override public long next(long delta) {
        return cntr.getAndAdd(delta);
    }

    /** {@inheritDoc} */
    @Override public boolean sequential() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public @Nullable byte[] getBytes() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public synchronized void reset() {
        initCntr = 0;

        cntr.set(0);
    }

    /** {@inheritDoc} */
    @Override public void resetInitialCounter() {
        initCntr = 0;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        PartitionUpdateCounterVolatileImpl cntr = (PartitionUpdateCounterVolatileImpl)o;

        return this.cntr.get() == cntr.cntr.get();
    }

    /** {@inheritDoc} */
    @Override public long reserved() {
        return get();
    }

    /** {@inheritDoc} */
    @Override public boolean empty() {
        return get() == 0;
    }

    /** {@inheritDoc} */
    @Override public Iterator<long[]> iterator() {
        return new GridEmptyIterator<>();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "Counter [init=" + initCntr + ", val=" + get() + ']';
    }

    /** {@inheritDoc} */
    @Override public CacheGroupContext context() {
        return grp;
    }

    /** {@inheritDoc} */
    @Override public PartitionUpdateCounter copy() {
        PartitionUpdateCounterVolatileImpl copy = new PartitionUpdateCounterVolatileImpl(grp);

        copy.cntr.set(cntr.get());
        copy.initCntr = this.initCntr;

        return copy;
    }
}
