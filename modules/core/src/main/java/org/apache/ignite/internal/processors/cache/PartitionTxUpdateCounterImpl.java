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
import java.util.Iterator;
import java.util.NavigableSet;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.processors.datastreamer.DataStreamerImpl;
import org.apache.ignite.internal.util.GridLongList;
import org.apache.ignite.internal.util.typedef.F;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Update counter implementation used for transactional cache groups.
 */
public class PartitionTxUpdateCounterImpl implements PartitionUpdateCounter {
    /**
     * Max allowed missed updates. Overflow will trigger critical failure handler to prevent OOM.
     */
    public static final int MAX_MISSED_UPDATES = 10_000;

    /** Counter updates serialization version. */
    private static final byte VERSION = 1;

    /** Queue of finished out of order counter updates. */
    private TreeSet<Item> queue = new TreeSet<>();

    /** LWM. */
    private final AtomicLong cntr = new AtomicLong();

    /** HWM. */
    private final AtomicLong reserveCntr = new AtomicLong();

    /**
     * Initial counter points to last sequential update after WAL recovery.
     * @deprecated TODO FIXME https://issues.apache.org/jira/browse/IGNITE-11794
     */
    @Deprecated private long initCntr;

    /** {@inheritDoc} */
    @Override public void init(long initUpdCntr, @Nullable byte[] cntrUpdData) {
        cntr.set(initUpdCntr);

        initCntr = initUpdCntr;

        queue = fromBytes(cntrUpdData);
    }

    /** {@inheritDoc} */
    @Override public long initial() {
        return initCntr;
    }

    /** {@inheritDoc} */
    @Override public long get() {
        return cntr.get();
    }

    /** */
    protected synchronized long highestAppliedCounter() {
        return queue.isEmpty() ? cntr.get() : queue.last().absolute();
    }

    /**
     * @return Next update counter. For tx mode called by {@link DataStreamerImpl} IsolatedUpdater.
     */
    @Override public long next() {
        long next = cntr.incrementAndGet();

        reserveCntr.set(next);

        return next;
    }

    /** {@inheritDoc} */
    @Override public synchronized void update(long val) throws IgniteCheckedException {
        // Absolute counter should be not less than last applied update.
        // Otherwise supplier doesn't contain some updates and rebalancing couldn't restore consistency.
        // Best behavior is to stop node by failure handler in such a case.
        if (!gaps().isEmpty() && val < highestAppliedCounter())
            throw new IgniteCheckedException("New value is incompatible with current update counter state. " +
                "Most probably a node with most actual data is out of topology or data streamer is used in isolated " +
                "mode (allowOverride=true) concurrently with normal cache operations [val=" + val +
                ", locCntr=" + this + ']');

        long cur = cntr.get();

        // Reserved update counter is updated only on exchange or in non-tx mode.
        reserveCntr.set(Math.max(cur, val));

        if (val <= cur)
            return;

        cntr.set(val);

        if (!queue.isEmpty())
            queue.clear();
    }

    /** {@inheritDoc} */
    @Override public synchronized boolean update(long start, long delta) {
        long cur = cntr.get(), next;

        if (cur > start)
            return false;

        if (cur < start) {
            // Try merge with adjacent gaps in sequence.
            Item tmp = new Item(start, delta);
            Item ref = tmp;

            NavigableSet<Item> set = queue.headSet(tmp, false);

            // Merge with previous, possibly modifying previous.
            if (!set.isEmpty()) {
                Item last = set.last();

                if (last.start + last.delta == start) {
                    tmp = last;

                    last.delta += delta;
                }
                else if (last.within(start) && last.within(start + delta - 1))
                    return false;
            }

            // Merge with next, possibly modifying previous and removing next.
            if (!(set = queue.tailSet(tmp, false)).isEmpty()) {
                Item first = set.first();

                if (tmp.start + tmp.delta == first.start) {
                    if (ref != tmp) {
                        tmp.delta += first.delta;

                        set.pollFirst(); // Merge and remove obsolete head.
                    }
                    else {
                        tmp = first;

                        first.start = start;
                        first.delta += delta;
                    }
                }
            }

            if (tmp != ref)
                return true;

            return offer(new Item(start, delta)); // backup node with gaps
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

    /** {@inheritDoc} */
    @Override public void updateInitial(long start, long delta) {
        long cntr0 = get();

        assert start >= cntr0 : "Illegal update counters order: cur=" + cntr0 + ", new=" + start;

        update(start, delta);

        initCntr = get();
    }

    /** */
    private Item poll() {
        return queue.pollFirst();
    }

    /** */
    private Item peek() {
        return queue.isEmpty() ? null : queue.first();
    }

    /**
     * @param item Item.
     */
    private boolean offer(Item item) {
        if (queue.size() == MAX_MISSED_UPDATES) // Should trigger failure handler.
            throw new IgniteException("Too many gaps [cntr=" + this + ']');

        return queue.add(item);
    }

    /** {@inheritDoc} */
    @Override public synchronized GridLongList finalizeUpdateCounters() {
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

    /** {@inheritDoc} */
    @Override public long reserve(long delta) {
        long cntr = get();

        long reserved = reserveCntr.getAndAdd(delta);

        assert reserved >= cntr : "LWM after HWM: lwm=" + cntr + ", hwm=" + reserved;

        return reserved;
    }

    /** {@inheritDoc} */
    @Override public long next(long delta) {
        return cntr.getAndAdd(delta);
    }

    /** {@inheritDoc} */
    @Override public synchronized boolean sequential() {
        return gaps().isEmpty();
    }

    /** {@inheritDoc} */
    @Override public synchronized @Nullable byte[] getBytes() {
        if (queue.isEmpty())
            return null;

        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();

            DataOutputStream dos = new DataOutputStream(bos);

            dos.writeByte(VERSION);

            int size = queue.size();

            dos.writeInt(size);

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
     * @param raw Raw bytes.
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

    /** */
    private TreeSet<Item> gaps() {
        return queue;
    }

    /** {@inheritDoc} */
    @Override public synchronized void reset() {
        cntr.set(0);

        reserveCntr.set(0);

        queue = new TreeSet<>();
    }

    /**
     * Update counter task. Update from start value by delta value.
     */
    private static class Item implements Comparable<Item> {
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

        /** */
        public long start() {
            return start;
        }

        /** */
        public long delta() {
            return delta;
        }

        /** */
        public long absolute() {
            return start + delta;
        }

        /** */
        public boolean within(long cntr) {
            return cntr - start < delta;
        }

        /** {@inheritDoc} */
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

        PartitionTxUpdateCounterImpl cntr = (PartitionTxUpdateCounterImpl)o;

        if (!queue.equals(cntr.queue))
            return false;

        return this.cntr.get() == cntr.cntr.get();
    }

    /** {@inheritDoc} */
    @Override public long reserved() {
        return reserveCntr.get();
    }

    /** {@inheritDoc} */
    @Override public synchronized boolean empty() {
        return get() == 0 && sequential();
    }

    /** {@inheritDoc} */
    @Override public Iterator<long[]> iterator() {
        return F.iterator(queue.iterator(), item -> {
            return new long[] {item.start, item.delta};
        }, true);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "Counter [lwm=" + get() + ", holes=" + queue +
            ", maxApplied=" + highestAppliedCounter() + ", hwm=" + reserveCntr.get() + ']';
    }
}
