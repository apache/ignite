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

import java.util.PriorityQueue;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.IgniteLogger;
import org.jetbrains.annotations.NotNull;

/**
 * Partition update counter with MVCC delta updates capabilities.
 */
public class PartitionUpdateCounter {
    /** */
    private IgniteLogger log;

    /** Queue of counter update tasks*/
    private final Queue<Item> queue = new PriorityQueue<>();

    /** Counter. */
    private final AtomicLong cntr = new AtomicLong();

    /** Initial counter. */
    private long initCntr;

    /**
     * @param log Logger.
     */
    PartitionUpdateCounter(IgniteLogger log) {
        this.log = log;
    }

    /**
     * Sets init counter.
     *
     * @param updateCntr Init counter valus.
     */
    public void init(long updateCntr) {
        initCntr = updateCntr;

        cntr.set(updateCntr);
    }

    /**
     * @return Initial counter value.
     */
    public long initial() {
        return initCntr;
    }

    /**
     * @return Current update counter value.
     */
    public long get() {
        return cntr.get();
    }

    /**
     * Adds delta to current counter value.
     *
     * @param delta Delta.
     * @return Value before add.
     */
    public long getAndAdd(long delta) {
        return cntr.getAndAdd(delta);
    }

    /**
     * @return Next update counter.
     */
    public long next() {
        return cntr.incrementAndGet();
    }

    /**
     * Sets value to update counter,
     *
     * @param val Values.
     */
    public void update(long val) {
        while (true) {
            long val0 = cntr.get();

            if (val0 >= val)
                break;

            if (cntr.compareAndSet(val0, val))
                break;
        }
    }

    /**
     * Updates counter by delta from start position.
     *
     * @param start Start.
     * @param delta Delta.
     */
    public synchronized void update(long start, long delta) {
        long cur = cntr.get(), next;

        if (cur > start) {
            log.warning("Stale update counter task [cur=" + cur + ", start=" + start + ", delta=" + delta + ']');

            return;
        }

        if (cur < start) {
            // backup node with gaps
            offer(new Item(start, delta));

            return;
        }

        while (true) {
            boolean res = cntr.compareAndSet(cur, next = start + delta);

            assert res;

            Item peek = peek();

            if (peek == null || peek.start != next)
                return;

            Item item = poll();

            assert peek == item;

            start = item.start;
            delta = item.delta;
            cur = next;
        }
    }

    /**
     * @param cntr Sets initial counter.
     */
    public void updateInitial(long cntr) {
        if (get() < cntr)
            update(cntr);

        initCntr = cntr;
    }

    /**
     * @return Retrieves the minimum update counter task from queue.
     */
    private Item poll() {
        return queue.poll();
    }

    /**
     * @return Checks the minimum update counter task from queue.
     */
    private Item peek() {
        return queue.peek();
    }

    /**
     * @param item Adds update task to priority queue.
     */
    private void offer(Item item) {
        queue.offer(item);
    }

    /**
     * Update counter task. Update from start value by delta value.
     */
    private static class Item implements Comparable<Item> {
        /** */
        private final long start;

        /** */
        private final long delta;

        /**
         * @param start Start value.
         * @param delta Delta value.
         */
        private Item(long start, long delta) {
            this.start = start;
            this.delta = delta;
        }

        /** {@inheritDoc} */
        @Override public int compareTo(@NotNull Item o) {
            int cmp = Long.compare(this.start, o.start);

            assert cmp != 0;

            return cmp;
        }
    }
}
