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

import java.util.NavigableSet;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.GridLongList;
import org.jetbrains.annotations.NotNull;

/**
 * Partition update counter.
 */
public class PartitionUpdateCounter {
    /** */
    private IgniteLogger log;

    /** Queue of counter update tasks*/
    private final TreeSet<Item> queue = new TreeSet<>();

    /** Counter. */
    private final AtomicLong cntr = new AtomicLong();

    /** Reservation counter. */
    private final AtomicLong reserveCntr = new AtomicLong();

    /** Initial counter. */
    private long initCntr;

    /**
     * @param log Logger.
     */
    public PartitionUpdateCounter(IgniteLogger log) {
        this.log = log;
    }

    /** {@inheritDoc} */
    public void init(long lwm, long hwm, long cnt) {
        initCntr = lwm;

        cntr.set(lwm);
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

    public long reserved() {
        return reserveCntr.get();
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

            if (peek == null || peek.start != next || peek.open)
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
        return queue.pollFirst();
    }

    /**
     * @return Checks the minimum update counter task from queue.
     */
    private Item peek() {
        return queue.isEmpty() ? null : queue.first();

    }

    /**
     * @param item Adds update task to priority queue.
     */
    private void offer(Item item) {
        queue.add(item);
    }

    /**
     * Flushes pending update counters closing all possible gaps.
     *
     * @return Even-length array of pairs [start, end] for each gap.
     */
    public synchronized GridLongList finalizeUpdateCounters() {
        Item item = poll();

        GridLongList gaps = null;

        while (item != null) {
            if (gaps == null)
                gaps = new GridLongList((queue.size() + 1) * 2);

            long start = cntr.get() + 1;
            long end = item.start;

            gaps.add(start);
            gaps.add(end);

            // Close pending ranges.
            update(item.start + item.delta);

            item = poll();
        }

        reserveCntr.set(cntr.get());

        return gaps;
    }

    public long maxUpdateCounter() {
        return 0;
    }

    public long updateCounterGap() {
        return 0;
    }

    public synchronized long reserve(long delta) {
        long start = reserveCntr.getAndAdd(delta);

        offer(new Item(start, delta, true));

        return start;
    }

    public synchronized void release(long start, long delta) {
        NavigableSet<Item> items = queue.tailSet(new Item(start, delta), true);

        Item first = items.first();

        assert first != null && first.delta == delta && first.open: "Interval is not reserved [start=" + start + ", delta=" + delta + "]";

        first.open = false;

        long cur = cntr.get(), next;

        if (start != cur)
            return;

        items.pollFirst(); // Skip first.

        while (true) {
            boolean res = cntr.compareAndSet(cur, next = start + delta);

            assert res;

            if (items.isEmpty())
                break;

            Item peek = items.first();

            if (peek.start != next || peek.open)
                return;

            Item item = items.pollFirst();

            assert peek == item;

            start = item.start;
            delta = item.delta;
            cur = next;
        }
    }

    /**
     * Update counter task. Update from start value by delta value.
     */
    private static class Item implements Comparable<Item> {
        /** */
        private final long start;

        /** */
        private final long delta;

        /** */
        boolean open;

        /**
         * @param start Start value.
         * @param delta Delta value.
         */
        private Item(long start, long delta) {
            this.start = start;
            this.delta = delta;
        }

        /**
         * @param start Start.
         * @param delta Delta.
         * @param open Open.
         */
        private Item(long start, long delta, boolean open) {
            this.start = start;
            this.delta = delta;
            this.open = open;
        }

        /** {@inheritDoc} */
        @Override public int compareTo(@NotNull Item o) {
            return Long.compare(this.start, o.start);
        }

        @Override public String toString() {
            return "Item [" +
                "start=" + start +
                ", open=" + open +
                ", delta=" + delta +
                ']';
        }
    }

    /** {@inheritDoc} */
    public String toString() {
        return "Counter [cntr=" + cntr.get() + ", holes=" + queue + ", reserveCntr=" + reserveCntr.get() + ']';
    }
}
