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

package org.apache.ignite.lang;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.GridLeanMap;
import org.apache.ignite.internal.util.GridTimer;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.lang.GridTuple;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.testframework.GridTestUtils;
import org.jetbrains.annotations.Nullable;
import org.jsr166.ConcurrentLinkedDeque8;
import org.jsr166.ThreadLocalRandom8;
import sun.misc.Unsafe;

/**
 * Tests synchronization performance vs. lock.
 */
public class GridBasicPerformanceTest {
    /** Max tries. */
    private static final long MAX = 100000000;

    /** Random. */
    private static final Random RAND = new Random();

    /** Mutex. */
    private static final Object mux = new Object();

    /** Lock. */
    private static final Lock lock = new ReentrantLock();

    /** Condition. */
    private static final Condition cond = lock.newCondition();

    /** */
    private static final ReadWriteLock rwLock = new ReentrantReadWriteLock();

    /** Test variable. */
    @SuppressWarnings({"UnusedDeclaration", "FieldAccessedSynchronizedAndUnsynchronized"})
    private static int n;

    /** Volatile variable. */
    @SuppressWarnings({"FieldCanBeLocal", "UnusedDeclaration"})
    private static volatile int v;

    /** Atomic integer. */
    private static AtomicInteger a = new AtomicInteger();

    /** */
    private static final int ARR_SIZE = 50000;

    /** Thread local variable. */
    private static ThreadLocal<GridTuple<Integer>> t = new ThreadLocal<GridTuple<Integer>>() {
        @Override protected GridTuple<Integer> initialValue() {
            return new GridTuple<>(0);
        }
    };

    /** Map. */
    private static final ConcurrentMap<Long, GridTuple<Integer>> map =
        new ConcurrentHashMap<>();

    /**
     * Initialize per-thread map.
     */
    static {
        map.put(Thread.currentThread().getId(), new GridTuple<>(0));
    }

    /**
     *
     */
    private GridBasicPerformanceTest() {
        // No-op.
    }

    /**
     * @param args Arguments.
     * @throws Exception If failed.
     */
    public static void main(String[] args) throws Exception {
        for (int i = 0; i < 6; i++) {
            X.println(">>>");
            X.println(">>> Starting tests: " + i);
            X.println(">>>");

//            testLoop();
//            testNewObject();
//            testVolatileLoop();
//            testVolatileReadLoop();
//            testAtomicIntegerLoop();
//            testAtomicIntegerGetLoop();
//            testThreadLocal();
//            testThreadMap();
//            testSynchronizedMethod();
//            testSynchronization();
//            testSynchronizationReentry();
//            testSynchronizationWithNotifyAll();
//            testLock();
//            testTryLock();
//            testLockReentry();
//            testLockWithSignalAll();
//            testReadLock();
//            testWriteLock();
//            testReadTimedLock();
//            testReadLockInterruptibly();
//            testLinkedBlockingQueue();
//            testLinkedBlockingDeque();
//            testGridDeque();
//            testConcurrentQueue();
//            testNew();
//            testUnsafe();
//            testDirectBuffers();
//            testLeanMap();
//            testHashMap();
//            testFuture();
//            testFutureWithGet();
            testArrayInt();
            testArrayLong();
//            testThreadCount(4, ARR_SIZE);
//            testThreadCount(8, ARR_SIZE);
//            testThreadCount(256, ARR_SIZE);
        }
    }

    /**
     *
     */
    private static void testHashMap() {
        long start = System.currentTimeMillis();

        Map<Object, Object> m = null;

        for (long i = 0; i < MAX; i++) {
            m = new HashMap<>(3);

            m.put(1, 1);
            m.put(2, 2);
            m.put(3, 3);
        }

        long time = System.currentTimeMillis() - start;

        X.println("HashMap [time=" + time + ", size=" + m.size() + ']');
    }

    /**
     *
     */
    private static void testLeanMap() {
        long start = System.currentTimeMillis();

        Map<Object, Object> m = null;

        for (long i = 0; i < MAX; i++) {
            m = new GridLeanMap<>(3);

            m.put(1, 1);
            m.put(2, 2);
            m.put(3, 3);
        }

        long time = System.currentTimeMillis() - start;

        X.println("GridLeanMap [time=" + time + ", size=" + m.size() + ']');
    }

    /**
     *
     */
    private static void testLoop() {
        n = 0;

        long start = System.currentTimeMillis();

        for (long i = 0; i < MAX; i++)
            n += 2;

        long time = System.currentTimeMillis() - start;

        X.println("Loop time: " + time);
    }

    /**
     *
     */
    private static void testVolatileLoop() {
        v = 0;

        long start = System.currentTimeMillis();

        for (long i = 0; i < MAX; i++)
            v += 2;

        long time = System.currentTimeMillis() - start;

        X.println("Loop volatile time: " + time);
    }

    /**
     *
     */
    private static void testVolatileReadLoop() {
        int n = 0;

        long start = System.currentTimeMillis();

        for (long i = 0; i < MAX; i++)
            n = v;

        long time = System.currentTimeMillis() - start;

        X.println("Loop volatile read time [time=" + time + ", n=" + n + ']');
    }

    /**
     *
     */
    private static void testAtomicIntegerLoop() {
        a.set(0);

        long start = System.currentTimeMillis();

        for (long i = 0; i < MAX; i++)
            a.incrementAndGet();

        long time = System.currentTimeMillis() - start;

        X.println("Loop atomic integer time: " + time);
    }

    /**
     *
     */
    private static void testAtomicIntegerGetLoop() {
        a.set(0);

        long start = System.currentTimeMillis();

        for (long i = 0; i < MAX; i++)
            a.get();

        long time = System.currentTimeMillis() - start;

        X.println("Loop atomic integer get time: " + time);
    }

    /**
     * @return Dummy object.
     */
    @Nullable public static Object testNewObject() {
        System.gc();

        Object o = null;

        long start = System.currentTimeMillis();

        for (long i = 0; i < MAX; i++)
            o = new Object();

        long time = System.currentTimeMillis() - start;

        X.println("Loop new object time: " + time);

        return o;
    }

    /**
     *
     */
    private static void testSynchronization() {
        n = 0;

        long start = System.currentTimeMillis();

        for (int i = 0; i < MAX; i++) {
            synchronized (mux) {
                n += 2;
            }
        }

        long time = System.currentTimeMillis() - start;

        X.println("Synchronization time: " + time);
    }

    /**
     *
     */
    private static void testSynchronizationReentry() {
        n = 0;

        long start = System.currentTimeMillis();

        for (int i = 0; i < MAX; i++) {
            synchronized (mux) {
                synchronized (mux) {
                    n += 2;
                }
            }
        }

        long time = System.currentTimeMillis() - start;

        X.println("Synchronization w/reentry time: " + time);
    }

    /**
     *
     */
    private static void testSynchronizedMethod() {
        n = 0;

        long start = System.currentTimeMillis();

        for (long i = 0; i < MAX; i++)
            increment();

        long time = System.currentTimeMillis() - start;

        X.println("Synchronized method time: " + time);
    }

    /**
     *
     */
    private static synchronized void increment() {
        n += 2;
    }

    /**
     *
     */
    private static void testThreadLocal() {
        long start = System.currentTimeMillis();

        for (long i = 0; i < MAX; i++) {
            GridTuple<Integer> v = t.get();

            v.set(v.get() + 2);
        }

        long time = System.currentTimeMillis() - start;

        X.println("Thread local time: " + time);
    }

    /**
     *
     */
    private static void testThreadMap() {
        long start = System.currentTimeMillis();

        for (long i = 0; i < MAX; i++) {
            GridTuple<Integer> v = map.get(Thread.currentThread().getId());

            v.set(v.get() + 2);
        }

        long time = System.currentTimeMillis() - start;

        X.println("Thread map time: " + time);
    }

    /**
     *
     */
    @SuppressWarnings({"NotifyWithoutCorrespondingWait"})
    private static void testSynchronizationWithNotifyAll() {
        n = 0;

        long start = System.currentTimeMillis();

        for (int i = 0; i < MAX; i++) {
            synchronized (mux) {
                n += 2;

                mux.notifyAll();
            }
        }

        long time = System.currentTimeMillis() - start;

        X.println("Synchronization with notifyAll time: " + time);
    }

    /**
     *
     */
    private static void testLock() {
        n = 0;

        long start = System.currentTimeMillis();

        for (int i = 0; i < MAX; i++) {
            lock.lock();

            try {
                n += 2;
            }
            finally {
                lock.unlock();
            }
        }

        long time = System.currentTimeMillis() - start;

        X.println("Lock time: " + time);
    }

    /**
     *
     */
    private static void testTryLock() {
        n = 0;

        long start = System.currentTimeMillis();

        for (int i = 0; i < MAX; i++) {
            lock.tryLock();

            try {
                n += 2;
            }
            finally {
                lock.unlock();
            }
        }

        long time = System.currentTimeMillis() - start;

        X.println("Try lock time: " + time);
    }

    /**
     *
     */
    private static void testLockReentry() {
        n = 0;

        long start = System.currentTimeMillis();

        for (int i = 0; i < MAX; i++) {
            lock.lock();

            try {
                lock.lock();

                try {
                    n += 2;
                }
                finally {
                    lock.unlock();
                }
            }
            finally {
                lock.unlock();
            }
        }

        long time = System.currentTimeMillis() - start;

        X.println("Lock w/reentry time: " + time);
    }

    /**
     *
     */
    @SuppressWarnings({"SignalWithoutCorrespondingAwait"})
    private static void testLockWithSignalAll() {
        n = 0;

        long start = System.currentTimeMillis();

        for (int i = 0; i < MAX; i++) {
            lock.lock();

            try {
                n += 2;

                cond.signalAll();
            }
            finally {
                lock.unlock();
            }
        }

        long time = System.currentTimeMillis() - start;

        X.println("Lock with signalAll time: " + time);
    }

    /**
     *
     */
    private static void testReadLock() {
        n = 0;

        long start = System.currentTimeMillis();

        for (int i = 0; i < MAX; i++) {
            rwLock.readLock().lock();

            try {
                n += 2;
            }
            finally {
                rwLock.readLock().unlock();
            }
        }

        long time = System.currentTimeMillis() - start;

        X.println("Read Lock time: " + time);
    }

    /**
     * @throws InterruptedException If interrupted.
     */
    private static void testReadTimedLock() throws InterruptedException {
        n = 0;

        long start = System.currentTimeMillis();

        for (int i = 0; i < MAX; i++) {
            rwLock.readLock().tryLock(200, TimeUnit.MILLISECONDS);

            try {
                n += 2;
            }
            finally {
                rwLock.readLock().unlock();
            }
        }

        long time = System.currentTimeMillis() - start;

        X.println("Read Timed Lock time: " + time);
    }

    /**
     * @throws InterruptedException If interrupted.
     */
    private static void testReadLockInterruptibly() throws InterruptedException {
        n = 0;

        long start = System.currentTimeMillis();

        for (int i = 0; i < MAX; i++) {
            rwLock.readLock().lockInterruptibly();

            try {
                n += 2;
            }
            finally {
                rwLock.readLock().unlock();
            }
        }

        long time = System.currentTimeMillis() - start;

        X.println("Read LockInterruptibly time: " + time);
    }

    /**
     *
     */
    private static void testWriteLock() {
        n = 0;

        long start = System.currentTimeMillis();

        for (int i = 0; i < MAX; i++) {
            rwLock.writeLock().lock();

            try {
                n += 2;
            }
            finally {
                rwLock.writeLock().unlock();
            }
        }

        long time = System.currentTimeMillis() - start;

        X.println("Write Lock time: " + time);
    }

    /**
     *
     */
    private static void testFuture() {
        n = 0;

        long start = System.currentTimeMillis();

        for (int i = 0; i < MAX; i++) {
            new GridFutureAdapter<Integer>().onDone(n);

            n += 2;
        }

        long time = System.currentTimeMillis() - start;

        X.println("Future time: " + time);
    }

    /**
     * @throws Exception If failed.
     */
    private static void testFutureWithGet() throws Exception {
        n = 0;

        long start = System.currentTimeMillis();

        for (int i = 0; i < MAX; i++) {
            GridFutureAdapter<Integer> f = new GridFutureAdapter<>();

            f.onDone(n);

            f.get();

            n += 2;
        }

        long time = System.currentTimeMillis() - start;

        X.println("Future with get time: " + time);
    }

    /**
     *
     */
    private static void testSystemCurrentTimeMillis() {
        n = 0;

        long start = System.currentTimeMillis();

        long l = 0L;

        for (int i = 0; i < MAX; i++)
            l = System.currentTimeMillis();

        long time = l - start;

        X.println("Current time millis time: " + time);
    }

    /**
     *
     */
    private static void testArrayInt() {
        for (int i = 1; i <= 512; i *= 2) {
            doArray(new int[i], false);
            doArray(new int[i], true);
        }
    }

    /**
     *
     */
    private static void testArrayLong() {
        for (int i = 1; i <= 512; i *= 2) {
            doArray(new long[i], false);
            doArray(new long[i], true);
        }
    }

    /**
     * @param arr Array.
     * @param sort {@code True} to sort.
     */
    private static void doArray(int[] arr, boolean sort) {
        int lim = 10000;

        for (int i = 0; i < arr.length; i++)
            arr[i] = ThreadLocalRandom8.current().nextInt(lim);

        Arrays.sort(arr);

        long start = System.currentTimeMillis();

        for (int i = 0; i < MAX; i++) {
            if (sort)
                Arrays.binarySearch(arr, ThreadLocalRandom8.current().nextInt(lim));
            else
                F.contains(arr, ThreadLocalRandom8.current().nextInt(lim));
        }

        long time =  System.currentTimeMillis() - start;

        X.println("Array test time [time=" + time + ", len=" + arr.length + ", sort=" + sort + ']');
    }

    /**
     * @param arr Array.
     * @param sort {@code True} to sort.
     */
    private static void doArray(long[] arr, boolean sort) {
        int lim = 10000;

        for (int i = 0; i < arr.length; i++)
            arr[i] = ThreadLocalRandom8.current().nextLong(lim);

        Arrays.sort(arr);

        long start = System.currentTimeMillis();

        for (int i = 0; i < MAX; i++) {
            if (sort)
                Arrays.binarySearch(arr, ThreadLocalRandom8.current().nextInt(lim));
            else
                F.contains(arr, ThreadLocalRandom8.current().nextInt(lim));
        }

        long time =  System.currentTimeMillis() - start;

        X.println("Array long test time [time=" + time + ", len=" + arr.length + ", sort=" + sort + ']');
    }

    /**
     * @throws Exception If failed.
     */
    private static void testLinkedBlockingQueue() throws Exception {
        X.println("Linked blocking queue...");

        testQueue(new LinkedBlockingQueue<Integer>());

        testQueueMultiThreaded(new LinkedBlockingQueue<Integer>());
    }

    /**
     * @throws Exception If failed.
     */
    private static void testLinkedBlockingDeque() throws Exception {
        X.println("Linked blocking deque...");

        testQueue(new LinkedBlockingDeque<Integer>());

        testQueueMultiThreaded(new LinkedBlockingDeque<Integer>());
    }

    /**
     * @throws Exception If failed.
     */
    private static void testGridDeque() throws Exception {
        X.println("Grid deque...");

        testQueue(new ConcurrentLinkedDeque8<Integer>());

        testQueueMultiThreaded(new ConcurrentLinkedDeque8<Integer>());
    }

    /**
     * @throws Exception If failed.
     */
    private static void testConcurrentQueue() throws Exception {
        X.println("Concurrent queue...");

        testQueue(new ConcurrentLinkedQueue<Integer>());

        testQueueMultiThreaded(new ConcurrentLinkedQueue<Integer>());
    }

    /**
     * @param q Queue.
     */
    @SuppressWarnings("StatementWithEmptyBody")
    private static void testQueue(Queue<Integer> q) {
        System.gc();

        long start = System.currentTimeMillis();

        for (int i = 0; i < 20000000; i++)
            q.add(i);

        long time = System.currentTimeMillis() - start;

        X.println("    Add time: " + time);

        start = System.currentTimeMillis();

        while (q.poll() != null) {
            // No-op.
        }

        time = System.currentTimeMillis() - start;

        X.println("    Poll time: " + time);
    }

    /**
     * @param q Queue.
     * @throws Exception If failed.
     */
    private static void testQueueMultiThreaded(final Queue<Integer> q) throws Exception {
        System.gc();

        final AtomicInteger idx = new AtomicInteger();

        final CountDownLatch latch1 = new CountDownLatch(1);

        IgniteInternalFuture<?> fut1 = GridTestUtils.runMultiThreadedAsync(
            new Callable<Object>() {
                @Nullable @Override public Object call() throws Exception {
                    latch1.await();

                    int i;

                    do {
                        i = idx.getAndIncrement();

                        q.add(i);
                    }
                    while (i < 20000000);

                    return null;
                }
            },
            10,
            "test-thread"
        );

        long start = System.currentTimeMillis();

        latch1.countDown();

        fut1.get();

        long time = System.currentTimeMillis() - start;

        X.println("    Multithreaded add time: " + time);

        final CountDownLatch latch2 = new CountDownLatch(1);

        IgniteInternalFuture<?> fut2 = GridTestUtils.runMultiThreadedAsync(
            new Callable<Object>() {
                @SuppressWarnings("StatementWithEmptyBody")
                @Nullable @Override public Object call() throws Exception {
                    latch2.await();

                    while (q.poll() != null) {
                        // No-op.
                    }

                    return null;
                }
            },
            10,
            "test-thread"
        );

        start = System.currentTimeMillis();

        latch2.countDown();

        fut2.get();

        time = System.currentTimeMillis() - start;

        X.println("    Multithreaded poll time: " + time);
    }

    /**
     * Test unsafe vs. new.
     *
     * @throws InterruptedException If failed.
     */
    private static void testNew() throws InterruptedException {
        X.println("Testing new...");

        int MAX = 1000000;

        byte[][] arrs = new byte[MAX][];

        System.gc();

        GridTimer t = new GridTimer("new");

        byte v = 0;

        int mem = 1024;

        for (int i = 0; i < MAX; i++) {
            arrs[i] = new byte[mem];

            arrs[i][RAND.nextInt(mem)] = (byte)RAND.nextInt(mem);

            v = arrs[i][RAND.nextInt(mem)];
        }

        X.println("New [time=" + t.stop() + "ms, v=" + v + ']');

        Thread.sleep(5000L);
    }

    /**
     * Test unsafe vs. new.
     */
    @SuppressWarnings("JavaDoc")
    private static void testUnsafe() throws InterruptedException {
        X.println("Testing unsafe...");

        int MAX = 1000000;

        long[] addrs = new long[MAX];

        System.gc();

        GridTimer t = new GridTimer("unsafe");

        Unsafe unsafe = GridUnsafe.unsafe();

        int mem = 1024;

        for (int i = 0; i < MAX; i++) {
            addrs[i] = unsafe.allocateMemory(mem);

            unsafe.putByte(addrs[i] + RAND.nextInt(mem), (byte)RAND.nextInt(mem));

            v = unsafe.getByte(addrs[i] + RAND.nextInt(mem));
        }

        X.println("Unsafe [time=" + t.stop() + "ms, v=" + v + ']');

        Thread.sleep(5000L);

        for (long l : addrs)
            unsafe.freeMemory(l);
    }


    /**
     * Test unsafe vs. new.
     * @throws InterruptedException If failed.
     */
    private static void testDirectBuffers() throws InterruptedException {
        X.println("Testing direct buffers...");

        int MAX = 1000000;

        ByteBuffer[] arrs = new ByteBuffer[MAX];

        System.gc();

        GridTimer t = new GridTimer("new");

        byte v = 0;

        int mem = 1024;

        for (int i = 0; i < MAX; i++) {
            arrs[i] = ByteBuffer.allocateDirect(mem);

            arrs[i].put(RAND.nextInt(mem), (byte)RAND.nextInt(mem));

            v = arrs[i].get(RAND.nextInt(mem));
        }

        X.println("Direct buffers [time=" + t.stop() + "ms, v=" + v + ']');

        Thread.sleep(5000L);
    }

    /**
     * @param threadCnt Thread count.
     * @param arrSize Array size per thread.
     * @throws InterruptedException If failed.
     */
    private static void testThreadCount(int threadCnt, int arrSize) throws InterruptedException {
        final AtomicBoolean finish = new AtomicBoolean();
        CountDownLatch latch = new CountDownLatch(1);

        Thread[] threads = new Thread[threadCnt];
        Worker[] workers = new Worker[threadCnt];

        for (int i = 0; i < threadCnt; i++) {
            workers[i] = new Worker(arrSize, finish, latch );

            threads[i] = new Thread(workers[i]);

            threads[i].start();
        }

        latch.countDown();

        Thread.sleep(5000L);

        finish.set(true);

        for (Thread t : threads)
            t.join();

        long sum = 0;

        for (Worker worker : workers)
            sum += worker.sum();

        X.println("Thread count results [sum=" + sum + ", arrSize=" + arrSize + ", threadCnt=" + threadCnt + ']');
    }

    /**
     *
     */
    private static class Worker implements Runnable {
        /** */
        private final long[] arr;

        /** */
        private final AtomicBoolean finish;

        /** */
        private final CountDownLatch startLatch;

        /**
         * @param arrSize Size.
         * @param finish Flag.
         * @param startLatch Latch.
         */
        private Worker(int arrSize, AtomicBoolean finish, CountDownLatch startLatch) {
            arr = new long[arrSize];

            this.finish = finish;
            this.startLatch = startLatch;
        }

        /** {@inheritDoc} */
        @Override public void run() {
            try {
                startLatch.await();
            }
            catch (InterruptedException e) {
                throw new RuntimeException("Thread has been interrupted.", e);
            }

            while (!finish.get()) {
                for (int i = 0; i < ARR_SIZE; i++)
                    arr[i]++;
            }
        }

        /**
         * @return Sum.
         */
        @SuppressWarnings("ForLoopReplaceableByForEach")
        long sum() {
            long res = 0;

            for (int i = 0; i < arr.length; i++)
                res += arr[i];

            return res;
        }
    }
}