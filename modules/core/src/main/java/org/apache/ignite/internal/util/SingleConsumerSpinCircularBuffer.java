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

package org.apache.ignite.internal.util;

import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.locks.LockSupport;

/**
 * This class implements a circular buffer for efficient data exchange.
 */
public class SingleConsumerSpinCircularBuffer<T> {
    /** */
    private static final int SPINS_CNT = 32;

    /** */
    private static final AtomicLongFieldUpdater<SingleConsumerSpinCircularBuffer> writePosUpd =
        AtomicLongFieldUpdater.newUpdater(SingleConsumerSpinCircularBuffer.class, "writePos");

    /** */
    private volatile long readPos;

    /** */
    @SuppressWarnings("unused")
    private long p01, p02, p03, p04, p05, p06, p07;

    /** */
    private volatile long writePos;

    /** */
    @SuppressWarnings("unused")
    private long p11, p12, p13, p14, p15, p16, p17;

    /** */
    private final long sizeMask;

    /** */
    private final Item<T>[] arr;

    /** */
    private volatile Thread consumer;

    /** */
    private volatile boolean parked;

    /**
     * @param size Size.
     */
    @SuppressWarnings("unchecked")
    public SingleConsumerSpinCircularBuffer(
        int size
    ) {
        sizeMask = size - 1;

        arr = (Item<T>[])new Item[size];

        // Fill the array.
        for (int i = 0; i < arr.length; i++)
            arr[i] = new Item<>(-(arr.length + i));

        readPos = writePos = arr.length * 2;
    }

    /**
     * @return Head element or {@code null}.
     */
    public T poll() {
        try {
            return poll0(false);
        }
        catch (InterruptedException e) {
            assert false; // should never happen.

            throw new Error();
        }
    }

    /**
     * @param take {@code false} to poll, {@code true} to take.
     * @return Head element or {@code null}.
     * @throws InterruptedException If interrupted.
     */
    private T poll0(boolean take) throws InterruptedException {
        if (consumer == null)
            consumer = Thread.currentThread();

        long readPos0 = readPos;

        if (readPos0 == writePos) {
            if (take) {
                parked = true;

                try {
                    for (int i = 0; readPos0 == writePos; i++) {
                        if ((i & (SPINS_CNT - 1)) == 0) {
                            LockSupport.park();

                            if (Thread.interrupted())
                                throw new InterruptedException();
                        }
                    }
                }
                finally {
                    parked = false;
                }
            }
            else
                return null;
        }

        Item<T> item = arr[(int)(readPos0 & sizeMask)];

        readPos = readPos0 + 1;

        return item.item(readPos0);
    }

    /**
     * @return Head element or blocks until buffer is not empty.
     * @throws InterruptedException If interrupted.
     */
    public T take() throws InterruptedException {
        return poll0(true);
    }

    /**
     * @return Size.
     */
    public int size() {
        return (int)(writePos - readPos);
    }

    /**
     * @param t Element to put.
     * @return Current size.
     */
    public int add(T t) {
        long writePos0;

        for (;;) {
            writePos0 = writePos;

            if (writePosUpd.compareAndSet(this, writePos0, writePos0 + 1))
                break;
        }

        Item<T> item = arr[(int)(writePos0 & sizeMask)];

        item.update(writePos0, arr.length, t);

        if (parked)
            LockSupport.unpark(consumer);

        return (int)(writePos0 + 1 - readPos);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return SingleConsumerSpinCircularBuffer.class.toString();
    }

    /**
     *
     */
    private static class Item<V> {
        /** */
        private volatile long idx;

        /** */
        private V item;

        /** Padding. */
        @SuppressWarnings("unused")
        private long p01, p02, p03, p04, p05, p06;

        /**
         *
         */
        Item(long idx) {
            this.idx = idx;
        }

        /**
         * @return Item.
         */
        V item(long readPos) {
            for (;;) {
                if (idx == readPos) {
                    V item1 = this.item;

                    idx = -readPos;

                    return item1;
                }
            }
        }

        /**
         * @param writePos Index.
         * @param newItem Item.
         */
        void update(long writePos, long diff, V newItem) {
            for (;;) {
                if (idx == -(writePos - diff))
                    break;
            }

            item = newItem;

            idx = writePos;
        }

        /** {@inheritDoc} */
        @Override public synchronized String toString() {
            return "Item [idx=" + idx + ']';
        }
    }
}
