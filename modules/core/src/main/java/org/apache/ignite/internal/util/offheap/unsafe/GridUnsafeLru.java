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

package org.apache.ignite.internal.util.offheap.unsafe;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.ignite.internal.util.offheap.GridOffHeapOutOfMemoryException;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Striped LRU queue.
 */
@SuppressWarnings("ForLoopReplaceableByForEach")
class GridUnsafeLru {
    /** Number of stripes. */
    private final short cnt;

    /** Stripes. */
    @GridToStringExclude
    private final LruStripe[] lrus;

    /** Unsafe memory. */
    @GridToStringExclude
    private final GridUnsafeMemory mem;

    /** Current round-robin add stripe index. */
    private final AtomicInteger addIdx;

    /** Current round-robin remove stripe index. */
    private final AtomicInteger rmvIdx;

    /** Released flag. */
    private AtomicBoolean released = new AtomicBoolean(false);

    /**
     * @param cnt Number of stripes.
     * @param mem Unsafe memory.
     */
    GridUnsafeLru(short cnt, GridUnsafeMemory mem) {
        assert cnt > 0;
        assert mem != null;

        lrus = new LruStripe[cnt];

        this.cnt = cnt;
        this.mem = mem;

        for (short i = 0; i < cnt; i++)
            lrus[i] = new LruStripe(i, mem);

        addIdx = new AtomicInteger();
        rmvIdx = new AtomicInteger(cnt / 2);
    }

    /**
     * Gets number of stripes.
     *
     * @return Number of stripes.
     */
    short concurrency() {
        return cnt;
    }

    /**
     * @return Total number of entries in all stripes.
     */
    long size() {
        long sum = 0;

        for (int i = 0; i < lrus.length; i++)
            sum += lrus[i].size();

        return sum;
    }

    /**
     * @return Total memory consumed by all stripes.
     */
    long memorySize() {
        long sum = 0;

        for (int i = 0; i < lrus.length; i++)
            sum += lrus[i].memorySize();

        return sum;
    }

    /**
     * Reads order of LRU stripe for queue node.
     *
     * @param qAddr Queue node address.
     * @return Order of LRU stripe.
     */
    short order(long qAddr) {
        return LruStripe.order(qAddr, mem);
    }

    /**
     * Reads partition of entry at queue node address.
     *
     * @param order Order of LRU stripe.
     * @param qAddr Queue node address.
     * @return Entry partition.
     */
    int partition(short order, long qAddr) {
        return lrus[order].partition(qAddr);
    }

    /**
     * Reads hash code of entry at queue node address.
     *
     * @param order Order of LRU stripe.
     * @param qAddr Queue node address.
     * @return Entry hash code.
     */
    int hash(short order, long qAddr) {
        return lrus[order].hash(qAddr);
    }

    /**
     * Reads entry address for given queue node.
     *
     * @param order Order of LRU stripe.
     * @param qAddr Queue node address.
     * @return Entry address.
     */
    long entry(short order, long qAddr) {
        return lrus[order].entry(qAddr);
    }

    /**
     * Adds entry address to LRU queue.
     *
     * @param part Entry Entry partition.
     * @param addr Entry address.
     * @param hash Entry hash code.
     * @return Queue node address.
     * @throws GridOffHeapOutOfMemoryException If failed.
     */
    long offer(int part, long addr, int hash) throws GridOffHeapOutOfMemoryException {
        return lrus[addIdx.getAndIncrement() % cnt].offer(part, addr, hash);
    }

    /**
     * Marks oldest node from the queue as {@code polling}.
     *
     * @return Queue node address.
     */
    long prePoll() {
        int idx = rmvIdx.getAndIncrement();

        // Must try to poll from each LRU.
        for (int i = 0; i < lrus.length; i++) {
            long qAddr = lrus[(idx + i) % cnt].prePoll();

            if (qAddr != 0)
                return qAddr;
        }

        return 0;
    }

    /**
     * Removes polling node from the queue.
     * @param qAddr Queue node address.
     */
    void poll(long qAddr) {
        lrus[LruStripe.order(qAddr, mem)].poll(qAddr);
    }

    /**
     * Updates entry address at specified queue node address.
     *
     * @param qAddr Queue address.
     * @param addr Entry address.
     */
    void touch(long qAddr, long addr) {
        lrus[LruStripe.order(qAddr, mem)].touch(qAddr, addr);
    }

    /**
     * Removes queue node from queue.
     *
     * @param qAddr Address of queue node.
     */
    void remove(long qAddr) {
        lrus[LruStripe.order(qAddr, mem)].remove(qAddr);
    }

    /**
     * Releases memory allocated for this queue.
     */
    void destruct() {
        if (released.compareAndSet(false, true)) {
            for (int i = 0; i < cnt; i++)
                lrus[i].destruct();
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridUnsafeLru.class, this);
    }

    /**
     * Single LRU stripe.
     */
    private static class LruStripe {
        /** Size of a queue node. */
        private static final int NODE = 2/*queue-index*/ + 4 /*part*/ + 4 /*hash*/ + 1/*poll-flag*/ + 8 /*previous*/ +
            8/*next*/ + 8/*entry-address*/;

        /** Unsafe memory. */
        private final GridUnsafeMemory mem;

        /** Stripe order. */
        private final short order;

        /** Queue head. */
        private long head;

        /** Queue tail */
        private long tail;

        /** Number of elements in the queue. */
        private volatile long size;

        /** Mutex. */
        private final Lock lock = new ReentrantLock();

        /**
         * @param order Stripe order.
         * @param mem Unsafe memory.
         */
        private LruStripe(short order, GridUnsafeMemory mem) {
            assert order >= 0;
            assert mem != null;

            this.order = order;
            this.mem = mem;
        }

        /**
         * Gets stripe order for queue node.
         *
         * @param qAddr Queue node address.
         * @param mem Unsafe memory.
         * @return Stripe order.
         */
        static short order(long qAddr, GridUnsafeMemory mem) {
            return mem.readShort(qAddr);
        }

        /**
         * @return Stripe order.
         */
        int order() {
            return order;
        }

        /**
         * @return Number if entries in queue.
         */
        long size() {
            return size;
        }

        /**
         * @return Memory size.
         */
        long memorySize() {
            return size * NODE;
        }

        /**
         * Releases memory allocated for this queue stripe.
         */
        void destruct() {
            lock.lock();

            try {
                for (long n = head, prev = 0; n != 0; prev = n, n = next(n))
                    mem.releaseSystem(prev, NODE);
            }
            finally {
                lock.unlock();
            }
        }

        /**
         * Adds entry address to LRU queue.
         *
         * @param part Entry partition.
         * @param addr Entry address.
         * @param hash Entry hash code.
         * @return Queue node address.
         * @throws GridOffHeapOutOfMemoryException If failed.
         */
        long offer(int part, long addr, int hash) throws GridOffHeapOutOfMemoryException {
            lock.lock();

            try {
                long qAddr = mem.allocateSystem(NODE, false);

                if (head == 0)
                    head = qAddr;

                long prev = tail;

                tail = qAddr;

                if (prev != 0)
                    next(prev, qAddr);

                order(qAddr);
                partition(qAddr, part);
                polling(qAddr, false);
                hash(qAddr, hash);
                entry(qAddr, addr);
                previous(qAddr, prev);
                next(qAddr, 0L);

                size++;

                return qAddr;
            }
            finally {
                lock.unlock();
            }
        }

        /**
         * Polls oldest entry from the queue.
         *
         * @return Queue node address.
         */
        long prePoll() {
            lock.lock();

            try {
                long n = head;

                while (n != 0) {
                    if (!polling(n)) {
                        // Mark as polling, but do not remove.
                        // Node will be removed by explicitly calling remove.
                        polling(n, true);

                        break;
                    }

                    n = next(n);
                }

                return n;
            }
            finally {
                lock.unlock();
            }
        }

        /**
         * Updates entry address at specified queue node address.
         *
         * @param qAddr Queue address.
         * @param addr Entry address.
         */
        void touch(long qAddr, long addr) {
            lock.lock();

            try {
                entry(qAddr, addr);

                if (qAddr != tail) {
                    long prev = previous(qAddr);
                    long next = next(qAddr);

                    if (prev != 0)
                        next(prev, next);
                    else {
                        assert qAddr == head;

                        head = next;
                    }

                    if (next != 0)
                        previous(next, prev);

                    next(tail, qAddr);
                    next(qAddr, 0);
                    previous(qAddr, tail);

                    tail = qAddr;
                }
            }
            finally {
                lock.unlock();
            }
        }

        /**
         * Removes queue node from queue.
         *
         * @param qAddr Address of queue node.
         */
        void poll(long qAddr) {
            lock.lock();

            try {
                assert polling(qAddr);

                unlink(qAddr);
            }
            finally {
                lock.unlock();
            }
        }

        /**
         * Removes queue node from queue.
         *
         * @param qAddr Address of queue node.
         */
        void remove(long qAddr) {
            lock.lock();

            try {
                // Don't remove polling entries (poll operation will remove them).
                if (!polling(qAddr))
                    unlink(qAddr);
                else
                    // Update entry address in node to 0.
                    entry(qAddr, 0);
            }
            finally {
                lock.unlock();
            }
        }

        /**
         * Unlinks and releases queue node.
         *
         * @param qAddr Queue node address.
         */
        private void unlink(long qAddr) {
            assert head != 0 && tail != 0;

            long prev = 0;
            long next = next(qAddr);

            if (head == qAddr)
                head = next;
            else {
                prev = previous(qAddr);

                assert prev != 0 : "Invalid previous link for stripe: " + order;

                next(prev, next);
            }

            if (next != 0)
                previous(next, prev);
            else {
                assert qAddr == tail;

                tail = prev;
            }

            mem.releaseSystem(qAddr, NODE);

            size--;

            assert head != 0 || (tail == 0 && size == 0);
        }

        /**
         * Writes queue order.
         *
         * @param qAddr Queue node address.
         */
        private void order(long qAddr) {
            mem.writeShort(qAddr, order);
        }

        /**
         * Reads partition entry belongs to.
         *
         * @param qAddr Queue node address.
         * @return Entry partition.
         */
        private int partition(long qAddr) {
            return mem.readInt(qAddr + 2);
        }

        /**
         * Writes partition entry belongs to.
         *
         * @param qAddr Queue node address.
         * @param part Entry partition.
         */
        private void partition(long qAddr, int part) {
            mem.writeInt(qAddr + 2, part);
        }

        /**
         * Reads entry hash code.
         *
         * @param qAddr Queue node address.
         * @return Entry hash code.
         */
        private int hash(long qAddr) {
            return mem.readInt(qAddr + 6);
        }

        /**
         * Writes entry hash code.
         *
         * @param qAddr Queue node address.
         * @param hash Entry hash code.
         */
        private void hash(long qAddr, int hash) {
            mem.writeInt(qAddr + 6, hash);
        }

        /**
         * Checks if queue node is being polled.
         *
         * @param qAddr Queue node address.
         * @return {@code True} if queue node is being polled.
         */
        private boolean polling(long qAddr) {
            return mem.readByte(qAddr + 10) == 1;
        }

        /**
         * Mark entry as being polled or not.
         *
         * @param qAddr Queue node address.
         * @param polling Polling flag.
         */
        private void polling(long qAddr, boolean polling) {
            mem.writeByte(qAddr + 10, (byte)(polling ? 1 : 0));
        }

        /**
         * Reads address of previous queue node.
         *
         * @param qAddr Queue node address.
         * @return Address of previous queue node.
         */
        private long previous(long qAddr) {
            return mem.readLong(qAddr + 11);
        }

        /**
         * Writes address of previous queue node.
         *
         * @param qAddr Queue node address.
         * @param prev Address of previous node.
         */
        private void previous(long qAddr, long prev) {
            mem.writeLong(qAddr + 11, prev);
        }

        /**
         * Reads address of next queue node.
         *
         * @param qAddr Queue node address.
         * @return Address of next queue node.
         */
        private long next(long qAddr) {
            return mem.readLong(qAddr + 19);
        }

        /**
         * Writes address of next queue node.
         *
         * @param qAddr Queue node address.
         * @param next Address of next node.
         */
        private void next(long qAddr, long next) {
            mem.writeLong(qAddr + 19, next);
        }

        /**
         * Reads address of entry.
         *
         * @param qAddr Queue node address.
         * @return Address of entry.
         */
        private long entry(long qAddr) {
            return mem.readLong(qAddr + 27);
        }

        /**
         * Writes address of entry.
         *
         * @param qAddr Queue node address.
         * @param addr Address of entry.
         */
        private void entry(long qAddr, long addr) {
            mem.writeLong(qAddr + 27, addr);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(LruStripe.class, this);
        }
    }
}