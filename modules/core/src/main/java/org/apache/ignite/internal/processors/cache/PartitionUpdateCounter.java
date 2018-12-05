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

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.NavigableSet;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.processors.cache.persistence.ByteArrayDataRow;
import org.apache.ignite.internal.util.GridLongList;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Partition update counter.
 */
public class PartitionUpdateCounter {
    /** */
    private IgniteLogger log;

    /** Queue of counter update tasks*/
    private TreeSet<Item> queue = new TreeSet<>();

    /** Counter of finished updates in partition. */
    private final AtomicLong cntr = new AtomicLong();

    /** Initial counter points to last update which is written to persistent storage. */
    private long initCntr;
    private byte[] bytes;

    /**
     * @param log Logger.
     */
    public PartitionUpdateCounter(IgniteLogger log) {
        this.log = log;
    }

    /**
     * @param initUpdCntr Initial update counter.
     * @param holes Holes or null if counter is sequential.
     */
    public void init(long initUpdCntr, @Nullable TreeSet<Item> holes) {
        cntr.set(initUpdCntr);

        queue = holes;
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

    public synchronized long hwm() {
        return queue.isEmpty() ? cntr.get() : queue.last().absolute();
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
     * Updates initial counter on recovery.
     *
     * @param cntr Initial counter.
     */
    public void updateInitial(long cntr) {
        releaseOne(cntr);

        initCntr = get();
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

        return gaps;
    }

    public synchronized long reserve(long delta) {
        long start;

        if (queue.isEmpty())
            offer(new Item((start = 0), delta));
        else {
            Item last = queue.last();

            offer(new Item((start = last.start + last.delta), delta));
        }

        return start;
    }

    /**
     * Release subsequent closed reservations and adjust lwm.
     *
     * @param start Start.
     * @param delta Delta.
     */
    public synchronized void release(long start, long delta) {
        NavigableSet<Item> items = queue.tailSet(new Item(start, delta), true);

        Item first = items.first();

        assert first != null && first.delta == delta : "Wrong interval " + first;

        if (first.open())
            first.close();

        long cur = cntr.get(), next;

        if (start != cur) // If not first just mark as closed and return.
            return;

        items.pollFirst(); // Skip first.

        while (true) {
            boolean res = cntr.compareAndSet(cur, next = start + delta);

            assert res;

            if (items.isEmpty())
                break;

            Item peek = items.first();

            if (peek.start != next || peek.open())
                return;

            Item item = items.pollFirst();

            assert peek == item;

            start = item.start;
            delta = item.delta;
            cur = next;
        }
    }

    /**
     * Used on recovery.
     * TODO FIXME make thread safe.
     * @param c
     */
    public synchronized void releaseOne(long c) {
        NavigableSet<Item> items = queue.headSet(new Item(c, 0), true);

        assert !items.isEmpty();

        Item last = items.last();

        last.increment();

        if (!last.open() && items.first() == last)
            release(last.start, last.delta);
    }

    public @Nullable byte[] getBytes() {
        if (queue.isEmpty())
            return null;

        // TODO slow output stream
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();

            DataOutputStream dos = new DataOutputStream(bos);

            dos.writeInt(queue.size());

            for (Item item : queue) {
                dos.writeLong(item.start);
                //dos.writeP
            }

            return bytes;
        }
        catch (IOException e) {
            throw new IgniteException(e);
        }
    }

    /**
     * Update counter task. Update from start value by delta value.
     */
    private static class Item implements Comparable<Item> {
        /** */
        private final long start;

        /** */
        private long delta;

        /** */
        private int closed;

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
            return Long.compare(this.start, o.start);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "Item [" +
                "start=" + start +
                ", open=" + open() +
                ", delta=" + delta +
                ']';
        }

        public void increment() {
            closed++;
        }

        public boolean open() {
            return closed < delta;
        }

        public Item close() {
            closed = (int)delta; // TODO FIXME why delta not integer?

            return this;
        }

        public long absolute() {
            return start + delta;
        }
    }

    /** {@inheritDoc} */
    public String toString() {
        return "Counter [lwm=" + get() + ", holes=" + queue + ", hwm=" + hwm() + ']';
    }
}
