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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.NavigableSet;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.GridLongList;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Partition update counter.
 *
 * TODO FIXME consider rolling bit set implementation.
 * TODO describe ITEM structure
 * TODO add debugging info
 * TODO add update order tracking capabilities ?
 * TODO non-blocking version ? BitSets instead of TreeSet ?
 */
public class PartitionUpdateCounterImpl implements PartitionUpdateCounter {
    /** Max allowed gaps. */
    private static final int MAX_GAPS = 10_000;

    /** */
    private static final byte VERSION = 1;

    /** */
    private IgniteLogger log;

    /** Queue of counter update tasks. */
    private TreeSet<Item> queue = new TreeSet<>();

    /** Counter of applied updates in partition. */
    private final AtomicLong cntr = new AtomicLong();

    /** Counter of pending updates in partition. Updated on primary node and during exchange (set as max upd cntr). */
    private final AtomicLong reserveCntr = new AtomicLong();

    /** Initial counter points to last sequential update after recovery. */
    private long initCntr;

    private int partId;

    /**
     * @param log Logger.
     */
    public PartitionUpdateCounterImpl(IgniteLogger log) {
        this.log = log;
    }

    public PartitionUpdateCounterImpl(IgniteLogger log, int partId) {
        this.log = log;
        this.partId = partId;
    }

    /**
     * @param initUpdCntr Initial update counter.
     * @param rawData Byte array of holes raw data.
     */
    public void init(long initUpdCntr, @Nullable byte[] rawData) {
        cntr.set(initUpdCntr);

        initCntr = initUpdCntr;

        queue = fromBytes(rawData);
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
     * Highest seen (applied) update counter.
     * @return Counter.
     */
    public synchronized long hwm() {
        return queue.isEmpty() ? cntr.get() : queue.last().absolute();
    }

    /**
     * @return Next update counter.
     */
    public long next() {
        return cntr.incrementAndGet();
    }

    /**
     * Sets value to update counter clearing all gaps.
     *
     * @param val Values.
     */
    public synchronized void update(long val) throws IgniteCheckedException {
        // New counter should be not less than last seen update.
        // Otherwise supplier doesn't contain some updates and rebalancing couldn't restore consistency.
        // Best behavior is to stop node by failure handler in such a case.
        if (!gaps().isEmpty() && val < hwm())
            throw new IgniteCheckedException("Illegal counter update");

        long cur = cntr.get();

        // Reserved update counter is updated only on exchange or in non-tx mode.
        reserveCntr.set(Math.max(cur, val));

        if (val <= cur)
            return;

        cntr.set(val);

        if (!queue.isEmpty())
            queue.clear();
    }

    /**
     * Updates counter by delta from start position. Used only in transactions.
     *  @param start Start.
     * @param delta Delta.
     */
    public synchronized boolean update(long start, long delta) {
        long cur = cntr.get(), next;

        if (cur > start) {
            log.warning("Stale update counter task [partId=" + partId + ", cur=" + cur + ", start=" + start + ", delta=" + delta + ']');

            return false;
        }

        if (cur < start) {
            // Try find existing gap.
            NavigableSet<Item> set = queue.headSet(new Item(start, 0), false);

            if (!set.isEmpty()) {
                Item last = set.last();

                if (last.start + last.delta == start)
                    last.delta += delta;
                else
                    return offer(new Item(start, delta)); // backup node with gaps
            }
            else if (!(set = queue.tailSet(new Item(start, 0), false)).isEmpty()) {
                Item first = set.first();

                if (start + delta == first.start) {
                    first.start = start;
                    first.delta += delta;
                }
                else
                    return offer(new Item(start, delta)); // backup node with gaps
            }
            else
                return offer(new Item(start, delta)); // backup node with gaps

            return true;
        }

        while (true) {
            boolean res = cntr.compareAndSet(cur, next = start + delta);

            assert res;

            Item peek = peek();

            if (peek == null || peek.start != next)
                return true;

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
        long cntr0 = get();

        if (cntr <= cntr0) // These counter updates was already applied before checkpoint.
            return;

        // TODO FIXME fix inefficient assertion next 3 lines.
        long tmp = cntr - 1;

        NavigableSet<Item> items = queue.headSet(new Item(tmp, 0), true);

        assert items.isEmpty() || !items.last().within(tmp) : "Invalid counter";

        update(tmp, 1); // We expecting only unapplied updates.

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
    private boolean offer(Item item) {
        if (queue.size() == MAX_GAPS)
            throw new IgniteException("Too much gaps [part=" + partId + ", cntr=" + this + ']');

        return queue.add(item);
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
            cntr.set(item.start + item.delta);

            item = poll();
        }

        return gaps;
    }

    /**
     * Should never be called
     *
     * @param delta Delta.
     */
    public long reserve(long delta) {
        long cntr = this.cntr.get();

        long newCntr = reserveCntr.getAndAdd(delta);

        assert newCntr >= cntr : "Update counter behind reserve counter: cntr=" + cntr + ", reserveCntr=" + newCntr + ", partId=" + partId;

        return newCntr;
    }

//    /**
//     * Release subsequent closed reservations and adjust lwm.
//     *
//     * @param start Start.
//     * @param delta Delta.
//     */
//    public synchronized void release(long start, long delta) {
//        NavigableSet<Item> items = queue.tailSet(new Item(start, delta), true);
//
//        Item first = items.first();
//
//        assert first != null && first.delta == delta : "Wrong interval " + first;
//
//        long cur = cntr.get(), next;
//
//        if (start != cur) // If not first just mark as closed and return.
//            return;
//
//        items.pollFirst(); // Skip first.
//
//        while (true) {
//            boolean res = cntr.compareAndSet(cur, next = start + delta);
//
//            assert res;
//
//            if (items.isEmpty())
//                break;
//
//            Item peek = items.first();
//
//            if (peek.start != next || peek.open())
//                return;
//
//            Item item = items.pollFirst();
//
//            assert peek == item;
//
//            start = item.start;
//            delta = item.delta;
//            cur = next;
//        }
//    }

    /** {@inheritDoc} */
    @Override public synchronized boolean sequential() {
        return gaps().isEmpty();
    }

    public @Nullable byte[] getBytes() {
        if (queue.isEmpty())
            return null;

        // TODO slow output stream
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();

            DataOutputStream dos = new DataOutputStream(bos);

            dos.writeByte(VERSION); // Version.

            int size = queue.size();

            dos.writeInt(size); // Holes count.

            // TODO store as deltas in varint format. Eg:
            // 10000000000, 2; 10000000002, 4; 10000000004, 10;
            // stored as:
            // 10000000000; 0, 2; 2, 4; 4, 10.
            // All ints are packed except first.

            for (Item item : queue) {
                dos.writeLong(item.start);
                dos.writeLong(item.delta);
            }

            bos.close();

            return bos.toByteArray();
        }
        catch (IOException e) {
            throw new IgniteException(e);
        }
    }

    /**
     * TODO read from stream ?
     *
     * @param raw Raw.
     */
    private @Nullable TreeSet<Item> fromBytes(@Nullable byte[] raw) {
        if (raw == null)
            return new TreeSet<>();

        TreeSet<Item> ret = new TreeSet<>();

        try {
            ByteArrayInputStream bis = new ByteArrayInputStream(raw);

            DataInputStream dis = new DataInputStream(bis);

            dis.readByte(); // Version.

            int cnt = dis.readInt(); // Holes count.

            while(cnt-- > 0)
                ret.add(new Item(dis.readLong(), dis.readLong()));

            return ret;
        }
        catch (IOException e) {
            throw new IgniteException(e);
        }
    }

    // TODO make private.
    public TreeSet<Item> gaps() {
        return queue;
    }

    @Override public synchronized void resetCounters() {
        initCntr = 0;

        cntr.set(0);

        reserveCntr.set(0);

        queue = new TreeSet<>();
    }

    /**
     * Update counter task. Update from start value by delta value.
     */
    public static class Item implements Comparable<Item> {
        /** */
        private long start;

        /** */
        private long delta;

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
                ", delta=" + delta +
                ']';
        }

        public long start() {
            return start;
        }

        public long delta() {
            return delta;
        }

        public long absolute() {
            return start + delta;
        }

        public boolean within(long cntr) {
            return cntr - start < delta;
        }

        @Override public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            Item item = (Item)o;

            if (start != item.start)
                return false;
            return  (delta != item.delta);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        PartitionUpdateCounterImpl cntr = (PartitionUpdateCounterImpl)o;

        if (!queue.equals(cntr.queue))
            return false;

        return this.cntr.get() == cntr.cntr.get();
    }

    public long reserved() {
        return reserveCntr.get();
    }

    /** {@inheritDoc} */
    public String toString() {
        return "Counter [init=" + initCntr + ", lwm=" + get() + ", holes=" + queue + ", hwm=" + hwm() + ", resrv=" + reserveCntr.get() + ']';
    }
}
