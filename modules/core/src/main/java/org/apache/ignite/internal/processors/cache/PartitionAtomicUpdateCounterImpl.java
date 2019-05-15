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
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.util.GridEmptyIterator;
import org.apache.ignite.internal.util.GridLongList;
import org.jetbrains.annotations.Nullable;

/**
 * Partition update counter for non-tx scenarios without support for tracking missed updates.
 * TODO FIXME https://issues.apache.org/jira/browse/IGNITE-11797
 */
public class PartitionAtomicUpdateCounterImpl implements PartitionUpdateCounter {
    /** Counter of applied updates in partition. */
    private final AtomicLong cntr = new AtomicLong();

    /**
     * Initial counter is set to update with max sequence number after WAL recovery.
     */
    private long initCntr;

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

        while(val > (cur = cntr.get()) && !cntr.compareAndSet(cur, val));
    }

    /**
     * Updates counter by delta from start position.
     *
     * @param start Start.
     * @param delta Delta.
     */
    @Override public boolean update(long start, long delta) {
        return false; // Prevent RollbackRecord in mixed tx-atomic mode.
    }

    /**
     * Updates initial counter on recovery. Not thread-safe.
     *
     * @param start Start.
     * @param delta Delta.
     */
    @Override public synchronized void updateInitial(long start, long delta) {
        update(start + delta);

        initCntr = get();
    }

    /** {@inheritDoc} */
    @Override public GridLongList finalizeUpdateCounters() {
        return new GridLongList(0);
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
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        PartitionAtomicUpdateCounterImpl cntr = (PartitionAtomicUpdateCounterImpl)o;

        return this.cntr.get() == cntr.cntr.get();
    }

    /** {@inheritDoc} */
    @Override public long reserved() {
        return get();
    }

    /** {@inheritDoc} */
    @Override public synchronized boolean empty() {
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
}
