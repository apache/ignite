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
import org.apache.ignite.internal.processors.cache.mvcc.txlog.TxKey;
import org.jetbrains.annotations.NotNull;

/**
 * // TODO byte array heap structure for txs queue?
 */
public class PartitionUpdateCounter {
    final Queue<Item> queue = new PriorityQueue<>();

    final AtomicLong cntr = new AtomicLong();

    private long initCntr;

    public void init(long updateCntr) {
        initCntr = updateCntr;

        cntr.set(updateCntr);
    }

    public long initial() {
        return initCntr;
    }

    public long get() {
        return cntr.get();
    }

    public long getAndAdd(long delta) {
        return cntr.getAndAdd(delta);
    }

    public long next() {
        return cntr.incrementAndGet();
    }

    public void update(long val) {
        while (true) {
            long val0 = cntr.get();

            if (val0 >= val)
                break;

            if (cntr.compareAndSet(val0, val))
                break;
        }
    }

    public synchronized void update(long start, long delta, TxKey tx) {
        while (true) {
            long cur = cntr.get(), next;

            if (cur > start)
                return; // TODO warning??

            if (cur < start) {
                // backup node with gaps
                offer(new Item(start, delta, tx));

                return;
            }

            boolean res = cntr.compareAndSet(cur, next = start + delta);

            assert res;

            Item peek = peek();

            if (peek == null || peek.start != next)
                return;

            Item item = poll();

            assert peek == item;

            start = item.start;
            delta = item.delta;
            tx = item.tx;
        }
    }

    public void updateInitial(long cntr) {
        if (get() < cntr)
            update(cntr);

        initCntr = cntr;
    }

    private Item poll() {
        return queue.poll();
    }

    private Item peek() {
        return queue.peek();
    }

    private void offer(Item item) {
        queue.offer(item);
    }

    private static class Item implements Comparable<Item> {
        private final long start;
        private final long delta;
        private final TxKey tx;

        private Item(long start, long delta, TxKey tx) {
            this.start = start;
            this.delta = delta;
            this.tx = tx;
        }

        /** {@inheritDoc} */
        @Override public int compareTo(@NotNull Item o) {
            int cmp = Long.compare(this.start, o.start);

            assert cmp != 0;

            return cmp;
        }
    }
}
