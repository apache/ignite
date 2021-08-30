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

package org.apache.ignite.internal.binary.streams;

import java.util.ArrayDeque;
import java.util.Arrays;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_MARSHAL_BUFFERS_PER_THREAD_POOL_SIZE;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_MARSHAL_BUFFERS_RECHECK;

/**
 * On-heap memory allocator.
 */
public abstract class BinaryMemoryAllocator {
    /** @see IgniteSystemProperties#IGNITE_MARSHAL_BUFFERS_RECHECK */
    public static final int DFLT_MARSHAL_BUFFERS_RECHECK = 10000;

    /** @see IgniteSystemProperties#IGNITE_MARSHAL_BUFFERS_PER_THREAD_POOL_SIZE */
    public static final int DFLT_MARSHAL_BUFFERS_PER_THREAD_POOL_SIZE = 32;

    /** Buffer size re-check frequency. */
    private static final Long CHECK_FREQ = Long.getLong(IGNITE_MARSHAL_BUFFERS_RECHECK, DFLT_MARSHAL_BUFFERS_RECHECK);

    /** */
    private static final int POOL_SIZE =
        Integer.getInteger(IGNITE_MARSHAL_BUFFERS_PER_THREAD_POOL_SIZE, DFLT_MARSHAL_BUFFERS_PER_THREAD_POOL_SIZE);

    /** Thread local allocator instance. */
    public static final BinaryMemoryAllocator THREAD_LOCAL = new ThreadLocalAllocator();

    /** Pooled allocator instance. */
    public static final BinaryMemoryAllocator POOLED = new PooledAllocator();

    /** */
    public abstract BinaryMemoryAllocatorChunk chunk();

    /** */
    public abstract boolean isAcquired();

    /** */
    private static class ThreadLocalAllocator extends BinaryMemoryAllocator {
        /** Holders. */
        private final ThreadLocal<BinaryMemoryAllocatorChunk> holders = new ThreadLocal<>();

        /** {@inheritDoc} */
        @Override public BinaryMemoryAllocatorChunk chunk() {
            BinaryMemoryAllocatorChunk holder = holders.get();

            if (holder == null)
                holders.set(holder = new Chunk());

            return holder;
        }

        /**
         * Checks whether a thread-local array is acquired or not.
         * The function is used by Unit tests.
         *
         * @return {@code true} if acquired {@code false} otherwise.
         */
        @Override public boolean isAcquired() {
            BinaryMemoryAllocatorChunk holder = holders.get();

            return holder != null && holder.isAcquired();
        }

        /**
         * Memory allocator chunk.
         */
        private static class Chunk implements BinaryMemoryAllocatorChunk {
            /** Data array */
            private byte[] data;

            /** Max message size detected between checks. */
            private int maxMsgSize;

            /** Last time array size is checked. */
            private long lastCheckNanos = System.nanoTime();

            /** Whether the holder is acquired or not. */
            private boolean acquired;

            /** {@inheritDoc} */
            @Override public byte[] allocate(int size) {
                if (acquired)
                    return new byte[size];

                acquired = true;

                if (data == null || size > data.length)
                    data = new byte[size];

                return data;
            }

            /** {@inheritDoc} */
            @Override public byte[] reallocate(byte[] data, int size) {
                byte[] newData = new byte[size];

                if (this.data == data)
                    this.data = newData;

                System.arraycopy(data, 0, newData, 0, data.length);

                return newData;
            }

            /** {@inheritDoc} */
            @Override public void release(byte[] data, int maxMsgSize) {
                if (this.data != data)
                    return;

                if (maxMsgSize > this.maxMsgSize)
                    this.maxMsgSize = maxMsgSize;

                acquired = false;

                long nowNanos = System.nanoTime();

                if (U.nanosToMillis(nowNanos - lastCheckNanos) >= CHECK_FREQ) {
                    int halfSize = data.length >> 1;

                    if (this.maxMsgSize < halfSize)
                        this.data = new byte[halfSize];

                    lastCheckNanos = nowNanos;
                }
            }

            /** {@inheritDoc} */
            @Override public boolean isAcquired() {
                return acquired;
            }
        }
    }

    /** */
    private static class PooledAllocator extends BinaryMemoryAllocator {
        /** */
        private final ThreadLocal<DataHoldersPool> holders = ThreadLocal.withInitial(DataHoldersPool::new);

        /** {@inheritDoc} */
        @Override public BinaryMemoryAllocatorChunk chunk() {
            return new Chunk(holders.get());
        }

        /** {@inheritDoc} */
        @Override public boolean isAcquired() {
            return false;
        }

        /** */
        private static class DataHoldersPool {
            /** */
            private final ArrayDeque<DataHolder> pool = new ArrayDeque<>(POOL_SIZE);

            /** */
            public synchronized DataHolder acquire() {
                return pool.isEmpty() ? new DataHolder() : pool.pop();
            }

            /** */
            public synchronized void release(DataHolder holder) {
                if (pool.size() < POOL_SIZE) pool.push(holder);
            }
        }

        /** */
        private static class DataHolder {
            /** Size history. */
            private final int[] history = new int[128];

            /** Size history cntr. */
            private int cntr;

            /** Last time array size is checked. */
            private long lastCheckNanos = System.nanoTime();

            /** Data array */
            private byte[] data;

            /** */
            public byte[] ensureCapacity(int size, boolean copy) {
                if (data == null)
                    data = new byte[size];
                else if (data.length < size)
                    data = copy ? Arrays.copyOf(data, size) : new byte[size];

                return data;
            }

            /** */
            public boolean corresponds(byte[] data) {
                return data != null && this.data == data;
            }

            /** */
            public void adjustSize(int msgSize) {
                history[cntr % history.length] = msgSize;
                cntr = cntr == Integer.MAX_VALUE ? 0 : cntr + 1;

                long now = System.nanoTime();
                if (U.nanosToMillis(now - lastCheckNanos) >= CHECK_FREQ && cntr > history.length) {
                    lastCheckNanos = now;

                    int[] tmp = Arrays.copyOf(history, history.length);
                    Arrays.sort(tmp);
                    int adjusted = U.nextPowerOf2(tmp[tmp.length / 2]);

                    if (adjusted < data.length)
                        data = new byte[adjusted];
                }
            }
        }

        /** */
        private static class Chunk implements BinaryMemoryAllocatorChunk {
            /** */
            private volatile DataHolder holder;

            /** */
            private final DataHoldersPool pool;

            /** */
            private Chunk(DataHoldersPool pool) {
                this.pool = pool;
            }

            /** {@inheritDoc} */
            @Override public byte[] allocate(int size) {
                if (holder != null)
                    return new byte[size];

                holder = pool.acquire();
                return holder.ensureCapacity(size, false);
            }

            /** {@inheritDoc} */
            @Override public byte[] reallocate(byte[] data, int size) {
                DataHolder holder0 = holder;
                if (holder0 != null && holder0.corresponds(data))
                    return holder0.ensureCapacity(size, true);
                else
                    return Arrays.copyOf(data, size);
            }

            /** {@inheritDoc} */
            @Override public void release(byte[] data, int msgSize) {
                DataHolder holder0 = holder;
                if (holder0 == null || !holder0.corresponds(data))
                    return;

                holder.adjustSize(msgSize);

                pool.release(holder);
                holder = null;
            }

            /** {@inheritDoc} */
            @Override public boolean isAcquired() {
                return holder != null;
            }
        }
    }
}
