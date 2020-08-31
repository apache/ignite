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
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.pagemem.wal.record.RollbackRecord;
import org.apache.ignite.internal.processors.datastreamer.DataStreamerImpl;
import org.apache.ignite.internal.util.GridLongList;
import org.apache.ignite.internal.util.typedef.F;
import org.jetbrains.annotations.Nullable;

/**
 * Update counter implementation used for cache groups in persistent mode for both tx and atomic caches.
 * <p>
 * Implements the partition update counter flow to avoid situations when:
 * <ol>
 *     <li>update counter could be incremented and persisted while corresponding update is not recorded to WAL.</li>
 *     <li>update counter could be updated out of order.</li>
 * </ol>
 * All these situations are sources of partitions desync in case of node failure under load.
 * <p>
 * The main idea is to track updates received out-of-order to ensure valid state of the update counter for rebalancing.
 * <p>
 * Below a short description of new flow:
 * <ol>
 *     <li>Update counter is <i>reserved</i> for each update in partition on tx prepare phase (which always happens
 *     on primary partition owner). Reservation causes HWM increment.</li>
 *     <li>Reserved counter values are propagated on backup nodes and stored in backup transactions.</li>
 *     <li>On commit reserved counters are assigned to cache entries.</li>
 *     <li>LWM is incremented ONLY after corresponding WAL data record for each entry was written.</li>
 *     <li>In case of rollback (manual or during tx recovery on node failure) reserved updates are also applied and
 *     logged to WAL using {@link RollbackRecord} for further recovery purposes.</li>
 * </ol>
 */
public class PartitionUpdateCounterTrackingImpl implements PartitionUpdateCounter {
    /**
     * Max allowed missed updates. Overflow will trigger critical failure handler to prevent OOM.
     */
    public static final int MAX_MISSED_UPDATES = 10_000;

    /** Counter updates serialization version. */
    private static final byte VERSION = 1;

    /** Queue of applied out of order counter updates. */
    private NavigableMap<Long, Item> queue = new TreeMap<>();

    /** LWM. */
    private final AtomicLong cntr = new AtomicLong();

    /** HWM. */
    protected final AtomicLong reserveCntr = new AtomicLong();

    /** */
    private boolean first = true;

    /** */
    protected final CacheGroupContext grp;

    /**
     * Initial counter points to last sequential update after WAL recovery.
     * @deprecated TODO FIXME https://issues.apache.org/jira/browse/IGNITE-11794
     */
    @Deprecated private volatile long initCntr;

    /**
     * @param grp Group.
     */
    public PartitionUpdateCounterTrackingImpl(CacheGroupContext grp) {
        this.grp = grp;
    }

    /** {@inheritDoc} */
    @Override public void init(long initUpdCntr, @Nullable byte[] cntrUpdData) {
        cntr.set(initUpdCntr);

        reserveCntr.set(initCntr = initUpdCntr);

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
        return queue.isEmpty() ? cntr.get() : queue.lastEntry().getValue().absolute();
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
        // Reserved update counter is updated only on exchange.
        long cur = get();

        // Always set reserved counter equal to max known counter.
        long max = Math.max(val, cur);

        if (reserveCntr.get() < max)
            reserveCntr.set(max);

        // Outdated counter (txs are possible before current topology future is finished if primary is not changed).
        if (val < cur)
            return;

        // Absolute counter should be not less than last applied update.
        // Otherwise supplier doesn't contain some updates and rebalancing couldn't restore consistency.
        // Best behavior is to stop node by failure handler in such a case.
        if (val < highestAppliedCounter())
            throw new IgniteCheckedException("Failed to update the counter [newVal=" + val + ", curState=" + this + ']');

        cntr.set(val);

        /** If some holes are present at this point, thar means some update were missed on recovery and will be restored
         * during rebalance. All gaps are safe to "forget".
         * Should only do it for first PME (later missed updates on node left are reset in {@link #finalizeUpdateCounters}. */
        if (first) {
            queue.clear();

            first = false;
        }
    }

    /** {@inheritDoc} */
    @Override public synchronized boolean update(long start, long delta) {
        long cur = cntr.get();

        if (cur > start)
            return false;
        else if (cur < start) {
            // Try merge with adjacent gaps in sequence.
            long next = start + delta;

            // Merge with next.
            Item nextItem = queue.remove(next);

            if (nextItem != null)
                delta += nextItem.delta;

            // Merge with previous, possibly modifying previous.
            Map.Entry<Long, Item> prev = queue.lowerEntry(start);

            if (prev != null) {
                Item prevItem = prev.getValue();

                if (prevItem.absolute() == start) {
                    prevItem.delta += delta;

                    return true;
                }
                else if (prevItem.within(next - 1))
                    return false;
            }

            if (queue.size() >= MAX_MISSED_UPDATES) // Should trigger failure handler.
                throw new IgniteException("Too many gaps [cntr=" + this + ']');

            return queue.putIfAbsent(start, new Item(start, delta)) == null;
        }
        else { // cur == start
            long next = start + delta;

            // There is only one next sequential item possible, all other items will be merged.
            Item nextItem = queue.remove(next);

            if (nextItem != null)
                next += nextItem.delta;

            boolean res = cntr.compareAndSet(cur, next);

            assert res;

            return true;
        }
    }

    /** {@inheritDoc} */
    @Override public void updateInitial(long start, long delta) {
        update(start, delta);

        initCntr = get();

        if (reserveCntr.get() < initCntr)
            reserveCntr.set(initCntr);
    }

    /** {@inheritDoc} */
    @Override public synchronized GridLongList finalizeUpdateCounters() {
        Map.Entry<Long, Item> item = queue.pollFirstEntry();

        GridLongList gaps = null;

        while (item != null) {
            if (gaps == null)
                gaps = new GridLongList((queue.size() + 1) * 2);

            long start = cntr.get() + 1;
            long end = item.getValue().start;

            gaps.add(start);
            gaps.add(end);

            // Close pending ranges.
            cntr.set(item.getValue().absolute());

            item = queue.pollFirstEntry();
        }

        reserveCntr.set(get());

        return gaps;
    }

    /** {@inheritDoc} */
    @Override public synchronized long reserve(long delta) {
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
        return queue.isEmpty();
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

            for (Item item : queue.values()) {
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
    private @Nullable NavigableMap<Long, Item> fromBytes(@Nullable byte[] raw) {
        NavigableMap<Long, Item> ret = new TreeMap<>();

        if (raw == null)
            return ret;

        try {
            ByteArrayInputStream bis = new ByteArrayInputStream(raw);

            DataInputStream dis = new DataInputStream(bis);

            dis.readByte(); // Version.

            int cnt = dis.readInt(); // Holes count.

            while (cnt-- > 0) {
                Item item = new Item(dis.readLong(), dis.readLong());

                ret.put(item.start, item);
            }

            return ret;
        }
        catch (IOException e) {
            throw new IgniteException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public synchronized void reset() {
        initCntr = 0;

        cntr.set(0);

        reserveCntr.set(0);

        queue.clear();
    }

    /** {@inheritDoc} */
    @Override public void resetInitialCounter() {
        initCntr = 0;
    }

    /**
     * Update counter task. Update from start value by delta value.
     */
    private static class Item {
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

            return delta == item.delta;
        }
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        PartitionUpdateCounterTrackingImpl cntr = (PartitionUpdateCounterTrackingImpl)o;

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
        return F.iterator(queue.values().iterator(), item -> new long[] {item.start, item.delta}, true);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "Counter [lwm=" + get() + ", holes=" + queue +
            ", maxApplied=" + highestAppliedCounter() + ", hwm=" + reserveCntr.get() + ']';
    }

    /** {@inheritDoc} */
    @Override public CacheGroupContext context() {
        return grp;
    }

    /** {@inheritDoc} */
    @Override public PartitionUpdateCounter copy() {
        PartitionUpdateCounterTrackingImpl copy = new PartitionUpdateCounterTrackingImpl(grp);

        copy.cntr.set(cntr.get());
        copy.first = first;
        copy.queue = new TreeMap<>(queue);
        copy.initCntr = initCntr;
        copy.reserveCntr.set(reserveCntr.get());

        return copy;
    }
}
