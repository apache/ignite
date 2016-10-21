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

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.locks.LockSupport;
import org.jsr166.LongAdder8;
import sun.java2d.pipe.SpanIterator;

/**
 * This class implements a circular buffer for efficient data exchange.
 */
public class SingleConsumerSpinCircularBuffer<T> {
    private static final int PARK_FREQ = 3;
    private static final int SPINS_CNT = 32;

    private static final AtomicLongFieldUpdater<SingleConsumerSpinCircularBuffer> writePosUpd =
        AtomicLongFieldUpdater.newUpdater(SingleConsumerSpinCircularBuffer.class, "writePos");

    private volatile long readPos;

    private long p01, p02, p03, p04, p05, p06, p07;

    private volatile long writePos;

    private long p11, p12, p13, p14, p15, p16, p17;

    /** */
    private final long sizeMask;

    /** */
    private final Item<T>[] arr;

    private volatile Thread consumer;

    private volatile boolean parked;

    /**
     * @param size Size.
     */
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
     * @return
     */
    public T poll() {
        return poll0(false);
    }

    private T poll0(boolean take) {
        if (consumer == null)
            consumer = Thread.currentThread();

        long readPos0 = readPos;

        if (readPos0 == writePos) {
            if (take) {
                parked = true;

                try {
                    for (; readPos0 == writePos; ) {
//                        System.out.println("parked " + consumer.getId() + "readPos=" + readPos + ", writePos=" + writePos);

                        LockSupport.park();
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

        T item0 = item.item(readPos0, SPINS_CNT);

        if (item0 == null) {
            parked = true;

            try {
                for (item0 = item.item(readPos0, 1); item0 == null; item0 = item.item(readPos0, 1)) {
                    LockSupport.park();

//                    System.out.println("readPos=" + readPos + ", writePos=" + writePos + ", item=" + item);
                }
            }
            finally {
                parked = false;
            }

            assert item != null;
        }

        return item0;
    }

    public T take() throws InterruptedException {
        return poll0(true);
    }

    public int size() {
        return (int)(writePos - readPos);
    }

    /**
     * @param t
     * @return
     */
    public int put(T t) {
        long writePos0;

        for (;;) {
            writePos0 = writePos;

            if (writePosUpd.compareAndSet(this, writePos0, writePos0 + 1))
                break;
        }

        Item<T> item = arr[(int)(writePos0 & sizeMask)];

        item.update(writePos0, arr.length, t);

        if (parked) {
//            System.out.println("unpark " + consumer.getId() + "readPos=" + readPos + ", writePos=" + writePos + " " + item);

            LockSupport.unpark(consumer);
        }

        return (int)(writePos0 + 1 - readPos);
    }

    public static void main_(String[] args) {
        final ConcurrentLinkedDeque<Long> b = new ConcurrentLinkedDeque<>();

        final LongAdder8 cnt = new LongAdder8();

        new Thread(
            new Runnable() {
                @Override public void run() {
                    for (;;) {
                        try {
                            Thread.sleep(1000);
                        }
                        catch (InterruptedException e) {
                            e.printStackTrace();
                        }

                        System.out.println("TPS: " + cnt.sumThenReset());
                    }
                }
            }
        ).start();

        final Semaphore sem = new Semaphore(8192);

        new Thread(
            new Runnable() {
                @Override public void run() {
                    for (;;) {
                        Long poll = b.poll();

                        if (poll != null) {
                            cnt.increment();

                            sem.release();
                        }
                    }
                }
            }
        ).start();

        for (int i = 0; i < 4; i++) {
            new Thread(
                new Runnable() {
                    @Override public void run() {
                        for (long i = 0; ; i++) {
                            sem.acquireUninterruptibly();
                            b.add(i);
                        }
                    }
                }
            ).start();
        }
    }
    public static void main(String[] args) throws InterruptedException {
        final SingleConsumerSpinCircularBuffer<Long> b = new SingleConsumerSpinCircularBuffer<>(
            1024);

//        b.put(1L);
//        b.put(2L);
//        b.put(3L);
//
//        System.out.println(b.take());
//        System.out.println(b.poll());
//        System.out.println(b.poll());

        final LongAdder8 cnt = new LongAdder8();

        new Thread(
            new Runnable() {
                @Override public void run() {
                    for (;;) {
                        try {
                            Thread.sleep(1000);
                        }
                        catch (InterruptedException e) {
                            e.printStackTrace();
                        }

                        System.out.println("TPS: " + cnt.sumThenReset());
                    }
                }
            }
        ).start();

        final CyclicBarrier bar = new CyclicBarrier(2);

        new Thread(
            new Runnable() {
                @Override public void run() {
                    for (;;) {
                        Long poll = null;

                        try {
                            poll = b.take();

                            //bar.await();
                        }
                        catch (Exception e) {
                            e.printStackTrace();
                        }

                        if (poll != null)
                            cnt.increment();

                    }
                }
            }
        ).start();

        for (int i = 0; i < 4; i++) {
            new Thread(
                new Runnable() {
                    @Override public void run() {
                        for (long i = 0; ; i++) {
                            b.put(i);

//                            LockSupport.parkNanos(1L);

//                            try {
//                                bar.await();
//                            }
//                            catch (InterruptedException | BrokenBarrierException e) {
//                                e.printStackTrace();
//                            }
                        }
                    }
                }
            ).start();
        }
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
        private V item;

        /** */
        private volatile long idx;

        private long p01, p02, p03, p04, p05, p06;
        private int i01;

        /**
         *
         */
        Item(long idx) {
            this.idx = idx;
        }

        /**
         * @return Item.
         */
        V item(long readPos, int spins) {
            for (int i = 0; i < spins; i++) {
                if (idx == readPos) {
                    V item1 = this.item;

                    idx = -readPos;

                    return item1;
                }
            }

            return null;
        }

        /**
         * @param writePos Index.
         * @param newItem Item.
         */
        void update(long writePos, long diff, V newItem) {
            int i = 0;

            for (;;) {
                if (idx == -(writePos - diff))
                    break;

                i++;

                if ((i & 3) == 0)
                    LockSupport.parkNanos(1L);
            }

            if (i > 100)
                System.out.println("Spins [i=" + i + ", writePos=" + writePos + ']');

            item = newItem;

            idx = writePos;
        }

        /** {@inheritDoc} */
        @Override public synchronized String toString() {
            return "Item [idx=" + idx + ']';
        }
    }
}
