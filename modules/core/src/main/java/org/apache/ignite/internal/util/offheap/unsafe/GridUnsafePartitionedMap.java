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

import java.util.NoSuchElementException;
import java.util.Set;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.util.GridCloseableIteratorAdapter;
import org.apache.ignite.internal.util.lang.GridCloseableIterator;
import org.apache.ignite.internal.util.offheap.GridOffHeapEventListener;
import org.apache.ignite.internal.util.offheap.GridOffHeapEvictListener;
import org.apache.ignite.internal.util.offheap.GridOffHeapMap;
import org.apache.ignite.internal.util.offheap.GridOffHeapPartitionedMap;
import org.apache.ignite.internal.util.typedef.CX2;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.lang.IgniteBiTuple;
import org.jetbrains.annotations.Nullable;
import org.jsr166.LongAdder8;

/**
 * Off-heap map based on {@code Unsafe} implementation.
 */
public class GridUnsafePartitionedMap implements GridOffHeapPartitionedMap {
    /** Minimum segment concurrency. */
    private static final int MIN_SEGMENT_CONCURRENCY = 4;

    /** Array holding maps for each partition. */
    private final GridUnsafeMap[] partMap;

    /** Total memory. */
    private final GridUnsafeMemory mem;

    /** Evict closure. */
    private GridOffHeapEvictListener evictLsnr;

    /** Event listener. */
    private GridOffHeapEventListener evtLsnr;

    /** Striped LRU policy. */
    private final GridUnsafeLru lru;

    /** Concurrency. */
    private final int concurrency;

    /** Load factor. */
    private final float load;

    /** Partitions. */
    private final int parts;

    /** */
    private final LongAdder8 totalCnt = new LongAdder8();

    /**
     * @param parts Partitions.
     * @param concurrency Concurrency.
     * @param load Load factor.
     * @param initCap Initial capacity.
     * @param totalMem Total memory.
     * @param lruStripes LRU stripes.
     * @param evictLsnr Eviction callback.
     */
    @SuppressWarnings("unchecked")
    public GridUnsafePartitionedMap(int parts, int concurrency, float load, long initCap, long totalMem,
        short lruStripes, @Nullable GridOffHeapEvictListener evictLsnr) {
        this.parts = parts;
        this.concurrency = concurrency;
        this.load = load;

        // Unchecked assignment to avoid generic array creation.
        partMap = new GridUnsafeMap[parts];

        mem = new GridUnsafeMemory(totalMem);

        if (totalMem > 0)
            this.evictLsnr = evictLsnr;

        lru = totalMem > 0 ? new GridUnsafeLru(lruStripes, mem) : null;

        long cnt = initCap / parts;
        int mod = (int)(initCap % parts);

        // Since we have natural striping provided by partitioning, we can proportionally reduce
        // number of segments in each map.
        concurrency /= parts;

        if (concurrency < MIN_SEGMENT_CONCURRENCY)
            concurrency = MIN_SEGMENT_CONCURRENCY;

        for (int p = 0; p < parts; p++) {
            mod--;

            long init = mod >= 0 ? cnt + 1 : cnt;

            partMap[p] = new GridUnsafeMap(p, concurrency, load, init, totalCnt, mem, lru, evictLsnr,
                new GridUnsafeLruPoller() {
                    @Override public void lruPoll(int size) {
                        if (lru == null)
                            return;

                        int left = size;

                        while (left > 0) {
                            // Pre-poll outside of lock.
                            long qAddr = lru.prePoll();

                            if (qAddr == 0)
                                return; // LRU is empty.

                            short order = lru.order(qAddr);

                            int part = lru.partition(order, qAddr);

                            int released = partMap[part].freeSpace(order, qAddr);

                            if (released == 0)
                                return;

                            left -= released;
                        }
                    }
                }
            );
        }
    }

    /** {@inheritDoc} */
    @Override public float loadFactor() {
        return load;
    }

    /** {@inheritDoc} */
    @Override public int concurrency() {
        return concurrency;
    }

    /** {@inheritDoc} */
    @Override public int partitions() {
        return parts;
    }

    /**
     * @param p Partition.
     * @return Map for partition.
     */
    private GridOffHeapMap mapFor(int p) {
        assert p < parts;

        return partMap[p];
    }

    /** {@inheritDoc} */
    @Override public boolean contains(int part, int hash, byte[] keyBytes) {
        return mapFor(part).contains(hash, keyBytes);
    }

    /** {@inheritDoc} */
    @Override public byte[] get(int p, int hash, byte[] keyBytes) {
        return mapFor(p).get(hash, keyBytes);
    }

    /** {@inheritDoc} */
    @Override public IgniteBiTuple<Long, Integer> valuePointer(int p, int hash, byte[] keyBytes) {
        return mapFor(p).valuePointer(hash, keyBytes);
    }

    /** {@inheritDoc} */
    @Override public void enableEviction(int p, int hash, byte[] keyBytes) {
        if (lru == null)
            return;

        mapFor(p).enableEviction(hash, keyBytes);
    }

    /** {@inheritDoc} */
    @Override public byte[] remove(int p, int hash, byte[] keyBytes) {
        return mapFor(p).remove(hash, keyBytes);
    }

    /** {@inheritDoc} */
    @Override public boolean removex(int p, int hash, byte[] keyBytes) {
        return mapFor(p).removex(hash, keyBytes);
    }

    /** {@inheritDoc} */
    @Override public boolean put(int p, int hash, byte[] keyBytes, byte[] valBytes) {
        return mapFor(p).put(hash, keyBytes, valBytes);
    }

    /** {@inheritDoc} */
    @Override public void insert(int p, int hash, byte[] keyBytes, byte[] valBytes) {
        mapFor(p).insert(hash, keyBytes, valBytes);
    }

    /** {@inheritDoc} */
    @Override public long size() {
        return totalCnt.sum();
    }

    /** {@inheritDoc} */
    @Override public long size(Set<Integer> parts) {
        int cnt = 0;

        for (Integer part : parts)
            cnt += mapFor(part).size();

        return cnt;
    }

    /** {@inheritDoc} */
    @Override public long memorySize() {
        return mem.totalSize();
    }

    /** {@inheritDoc} */
    @Override public long allocatedSize() {
        return mem.allocatedSize();
    }

    /** {@inheritDoc} */
    @Override public long systemAllocatedSize() {
        return mem.systemAllocatedSize();
    }

    /** {@inheritDoc} */
    @Override public long freeSize() {
        return mem.freeSize();
    }

    /** {@inheritDoc} */
    @Override public boolean eventListener(GridOffHeapEventListener evtLsnr) {
        if (this.evtLsnr != null)
            return false;

        this.evtLsnr = evtLsnr;

        for (GridUnsafeMap m : partMap)
            m.eventListener(evtLsnr);

        mem.listen(evtLsnr);

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean evictListener(GridOffHeapEvictListener evictLsnr) {
        if (this.evictLsnr != null)
            return false;

        this.evictLsnr = evictLsnr;

        for (GridUnsafeMap m : partMap)
            m.evictListener(evictLsnr);

        return true;
    }

    /** {@inheritDoc} */
    @Override public void destruct() {
        for (GridUnsafeMap m : partMap)
            m.destruct();

        if (lru != null)
            lru.destruct();
    }

    /** {@inheritDoc} */
    @Override public GridCloseableIterator<IgniteBiTuple<byte[], byte[]>> iterator() {
        return new PartitionedMapCloseableIterator<IgniteBiTuple<byte[], byte[]>>() {
            protected void advance() throws IgniteCheckedException {
                curIt = null;

                while (p < parts) {
                    curIt = mapFor(p++).iterator();

                    if (curIt.hasNext())
                        return;
                    else
                        curIt.close();
                }

                curIt = null;
            }
        };
    }

    /** {@inheritDoc} */
    @Override public <T> GridCloseableIterator<T> iterator(final CX2<T2<Long, Integer>, T2<Long, Integer>, T> c) {
        assert c != null;

        return new PartitionedMapCloseableIterator<T>() {
            protected void advance() throws IgniteCheckedException {
                curIt = null;

                while (p < parts) {
                    curIt = mapFor(p++).iterator(c);

                    if (curIt.hasNext())
                        return;
                    else
                        curIt.close();
                }

                curIt = null;
            }
        };
    }

    /** {@inheritDoc} */
    @Override public <T> GridCloseableIterator<T> iterator(final CX2<T2<Long, Integer>, T2<Long, Integer>, T> c,
       int part) {
       return mapFor(part).iterator(c);
    }

    /** {@inheritDoc} */
    @Override public GridCloseableIterator<IgniteBiTuple<byte[], byte[]>> iterator(int p) {
        return mapFor(p).iterator();
    }

    /**
     * Gets number of LRU stripes.
     *
     * @return Number of LRU stripes.
     */
    public short lruStripes() {
        return lru.concurrency();
    }

    /**
     * Gets memory size occupied by LRU queue.
     *
     * @return Memory size occupied by LRU queue.
     */
    public long lruMemorySize() {
        return lru.memorySize();
    }

    /**
     * Gets number of elements in LRU queue.
     *
     * @return Number of elements in LRU queue.
     */
    public long lruSize() {
        return lru.size();
    }

    /**
     *  Partitioned closable iterator.
     */
    private abstract class PartitionedMapCloseableIterator<T> extends GridCloseableIteratorAdapter<T> {
        /** Current partition. */
        protected int p;

        /** Current iterator. */
        protected GridCloseableIterator<T> curIt;

        {
            try {
                advance();
            }
            catch (IgniteCheckedException e) {
                e.printStackTrace(); // Should never happen.
            }
        }

        /**
         * Switch to next partition.
         *
         * @throws IgniteCheckedException If failed.
         */
        abstract void advance() throws IgniteCheckedException;

        /** {@inheritDoc} */
        @Override protected T onNext() throws IgniteCheckedException {
            if (curIt == null)
                throw new NoSuchElementException();

            T t = curIt.next();

            if (!curIt.hasNext()) {
                curIt.close();

                advance();
            }

            return t;
        }

        /** {@inheritDoc} */
        @Override protected boolean onHasNext() {
            return curIt != null;
        }

        /** {@inheritDoc} */
        @Override protected void onRemove() {
            throw new UnsupportedOperationException();
        }

        /** {@inheritDoc} */
        @Override protected void onClose() throws IgniteCheckedException {
            if (curIt != null)
                curIt.close();
        }
    }
}