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

package org.apache.ignite.internal.tx;

import java.util.Collection;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Tests a LockManager implementation.
 */
public abstract class AbstractLockManagerTest extends IgniteAbstractTest {
    private LockManager lockManager;

    @BeforeEach
    public void before() {
        lockManager = newInstance();
    }

    protected abstract LockManager newInstance();

    @Test
    public void testSingleKeyWrite() throws LockException {
        Timestamp ts1 = Timestamp.nextVersion();

        Object key = new String("test");

        CompletableFuture<Void> fut0 = lockManager.tryAcquire(key, ts1);

        assertTrue(fut0.isDone());

        Collection<Timestamp> queue = lockManager.queue(key);

        assertTrue(queue.size() == 1 && queue.iterator().next().equals(ts1));

        Waiter waiter = lockManager.waiter(key, ts1);

        assertTrue(waiter.locked());

        lockManager.tryRelease(key, ts1);
    }

    @Test
    public void testSingleKeyWriteLock() throws LockException {
        Timestamp ts1 = Timestamp.nextVersion();

        Object key = new String("test");

        CompletableFuture<Void> fut0 = lockManager.tryAcquire(key, ts1);

        assertTrue(fut0.isDone());

        Timestamp ts2 = Timestamp.nextVersion();

        assertTrue(ts1.compareTo(ts2) < 0);

        CompletableFuture<Void> fut1 = lockManager.tryAcquire(key, ts2);

        assertFalse(fut1.isDone());

        assertTrue(lockManager.waiter(key, ts1).locked());
        assertFalse(lockManager.waiter(key, ts2).locked());

        lockManager.tryRelease(key, ts1);

        assertTrue(fut1.isDone());

        assertNull(lockManager.waiter(key, ts1));
        assertTrue(lockManager.waiter(key, ts2).locked());

        lockManager.tryRelease(key, ts2);

        assertNull(lockManager.waiter(key, ts1));
        assertNull(lockManager.waiter(key, ts2));
    }

    @Test
    public void testSingleKeyReadWriteLock() throws LockException {
        Timestamp ts0 = Timestamp.nextVersion();
        Timestamp ts1 = Timestamp.nextVersion();
        Timestamp ts2 = Timestamp.nextVersion();
        Timestamp ts3 = Timestamp.nextVersion();
        assertTrue(ts0.compareTo(ts1) < 0);
        assertTrue(ts1.compareTo(ts2) < 0);
        assertTrue(ts2.compareTo(ts3) < 0);
        Object key = new String("test");

        CompletableFuture<Void> fut0 = lockManager.tryAcquireShared(key, ts0);
        assertTrue(fut0.isDone());

        CompletableFuture<Void> fut2 = lockManager.tryAcquireShared(key, ts2);
        assertTrue(fut2.isDone());

        CompletableFuture<Void> fut1 = lockManager.tryAcquireShared(key, ts1);
        assertTrue(fut1.isDone());

        CompletableFuture<Void> fut3 = lockManager.tryAcquire(key, ts3);
        assertFalse(fut3.isDone());

        assertTrue(lockManager.waiter(key, ts0).locked());
        assertTrue(lockManager.waiter(key, ts1).locked());
        assertTrue(lockManager.waiter(key, ts2).locked());
        assertFalse(lockManager.waiter(key, ts3).locked());

        lockManager.tryReleaseShared(key, ts2);

        assertTrue(lockManager.waiter(key, ts0).locked());
        assertTrue(lockManager.waiter(key, ts1).locked());
        assertNull(lockManager.waiter(key, ts2));
        assertFalse(lockManager.waiter(key, ts3).locked());

        lockManager.tryReleaseShared(key, ts0);

        assertNull(lockManager.waiter(key, ts0));
        assertTrue(lockManager.waiter(key, ts1).locked());
        assertNull(lockManager.waiter(key, ts2));
        assertFalse(lockManager.waiter(key, ts3).locked());

        lockManager.tryReleaseShared(key, ts1);

        assertNull(lockManager.waiter(key, ts0));
        assertNull(lockManager.waiter(key, ts1));
        assertNull(lockManager.waiter(key, ts2));
        assertTrue(lockManager.waiter(key, ts3).locked());
    }

    @Test
    public void testSingleKeyReadWriteConflict() throws LockException {
        Timestamp ts0 = Timestamp.nextVersion();
        Timestamp ts1 = Timestamp.nextVersion();
        Object key = new String("test");

        // Lock in order
        CompletableFuture<Void> fut0 = lockManager.tryAcquireShared(key, ts0);
        assertTrue(fut0.isDone());

        CompletableFuture<Void> fut1 = lockManager.tryAcquire(key, ts1);
        assertFalse(fut1.isDone());

        lockManager.tryReleaseShared(key, ts0);
        assertTrue(fut1.isDone());

        lockManager.tryRelease(key, ts1);

        assertTrue(lockManager.queue(key).isEmpty());

        // Lock not in order
        fut0 = lockManager.tryAcquireShared(key, ts1);
        assertTrue(fut0.isDone());

        try {
            lockManager.tryAcquire(key, ts0);

            fail();
        }
        catch (LockException e) {
            // Expected.
        }
    }

    @Test
    public void testSingleKeyReadWriteConflict2() throws LockException {
        Timestamp[] ts = generate(3);
        Object key = new String("test");

        // Lock in order
        CompletableFuture<Void> fut0 = lockManager.tryAcquireShared(key, ts[1]);
        assertTrue(fut0.isDone());

        CompletableFuture<Void> fut1 = lockManager.tryAcquire(key, ts[2]);
        assertFalse(fut1.isDone());

        CompletableFuture<Void> fut2 = lockManager.tryAcquireShared(key, ts[0]);
        assertTrue(fut2.isDone());

        lockManager.tryReleaseShared(key, ts[1]);
        lockManager.tryReleaseShared(key, ts[0]);

        assertTrue(fut1.isDone());
    }

    @Test
    public void testSingleKeyReadWriteConflict3() throws LockException {
        Timestamp ts0 = Timestamp.nextVersion();
        Timestamp ts1 = Timestamp.nextVersion();
        Timestamp ts2 = Timestamp.nextVersion();
        Object key = new String("test");

        // Lock in order
        CompletableFuture<Void> fut0 = lockManager.tryAcquireShared(key, ts0);
        assertTrue(fut0.isDone());

        CompletableFuture<Void> fut1 = lockManager.tryAcquire(key, ts2);
        assertFalse(fut1.isDone());

        CompletableFuture<Void> fut2 = lockManager.tryAcquireShared(key, ts1);
        assertTrue(fut2.isDone());

        assertFalse(lockManager.waiter(key, ts2).locked());

        lockManager.tryReleaseShared(key, ts1);
        lockManager.tryReleaseShared(key, ts0);

        assertTrue(fut1.isDone());
    }

    @Test
    public void testSingleKeyReadWriteConflict4() throws LockException {
        Timestamp ts0 = Timestamp.nextVersion();
        Timestamp ts1 = Timestamp.nextVersion();
        Timestamp ts2 = Timestamp.nextVersion();
        Timestamp ts3 = Timestamp.nextVersion();
        Object key = new String("test");

        CompletableFuture<Void> fut0 = lockManager.tryAcquireShared(key, ts0);
        assertTrue(fut0.isDone());

        CompletableFuture<Void> fut1 = lockManager.tryAcquire(key, ts1);
        assertFalse(fut1.isDone());

        CompletableFuture<Void> fut2 = lockManager.tryAcquire(key, ts3);
        assertFalse(fut2.isDone());

        CompletableFuture<Void> fut3 = lockManager.tryAcquire(key, ts2);
        assertFalse(fut3.isDone());
    }

    @Test
    public void testSingleKeyWriteWriteConflict() throws LockException {
        Timestamp ts0 = Timestamp.nextVersion();
        Timestamp ts1 = Timestamp.nextVersion();
        Timestamp ts2 = Timestamp.nextVersion();
        Object key = new String("test");

        // Lock in order
        CompletableFuture<Void> fut0 = lockManager.tryAcquire(key, ts1);
        assertTrue(fut0.isDone());

        CompletableFuture<Void> fut1 = lockManager.tryAcquire(key, ts2);
        assertFalse(fut1.isDone());

        try {
            lockManager.tryAcquire(key, ts0);

            fail();
        }
        catch (LockException e) {
            // Expected.
        }
    }

    @Test
    public void testSingleKeyWriteWriteConflict2() throws LockException {
        Timestamp ts0 = Timestamp.nextVersion();
        Timestamp ts1 = Timestamp.nextVersion();
        Timestamp ts2 = Timestamp.nextVersion();
        Object key = new String("test");

        // Lock in order
        CompletableFuture<Void> fut0 = lockManager.tryAcquire(key, ts0);
        assertTrue(fut0.isDone());

        CompletableFuture<Void> fut1 = lockManager.tryAcquire(key, ts2);
        assertFalse(fut1.isDone());

        CompletableFuture<Void> fut2 = lockManager.tryAcquire(key, ts1);
        assertFalse(fut2.isDone());
    }

    @Test
    public void testSingleKeyMultithreadedRead() throws InterruptedException {
        LongAdder rLocks = new LongAdder();
        LongAdder wLocks = new LongAdder();
        LongAdder fLocks = new LongAdder();

        doTestSingleKeyMultithreaded(5_000, rLocks, wLocks, fLocks, 0);

        assertTrue(wLocks.sum() == 0);
        assertTrue(fLocks.sum() == 0);
    }

    @Test
    public void testSingleKeyMultithreadedWrite() throws InterruptedException {
        LongAdder rLocks = new LongAdder();
        LongAdder wLocks = new LongAdder();
        LongAdder fLocks = new LongAdder();

        doTestSingleKeyMultithreaded(5_000, rLocks, wLocks, fLocks, 1);

        assertTrue(rLocks.sum() == 0);
    }

    @Test
    public void testSingleKeyMultithreadedRandom() throws InterruptedException {
        LongAdder rLocks = new LongAdder();
        LongAdder wLocks = new LongAdder();
        LongAdder fLocks = new LongAdder();

        doTestSingleKeyMultithreaded(5_000, rLocks, wLocks, fLocks, 2);
    }

    @Test
    public void testDeadlock() throws LockException {
        Timestamp ts0 = Timestamp.nextVersion();
        Timestamp ts1 = Timestamp.nextVersion();
        Object key = new String("test");

        CompletableFuture<Void> fut0 = lockManager.tryAcquireShared(key, ts0);
        assertTrue(fut0.isDone());

        CompletableFuture<Void> fut1 = lockManager.tryAcquireShared(key, ts1);
        assertTrue(fut1.isDone());

        try {
            lockManager.tryAcquire(key, ts0);

            fail();
        }
        catch (LockException e) {
            // Expected.
        }
    }

    @Test
    public void testDeadlock2() throws LockException {
        Timestamp ts0 = Timestamp.nextVersion();
        Timestamp ts1 = Timestamp.nextVersion();
        Object key1 = new String("test");
        Object key2 = new String("test2");

        CompletableFuture<Void> fut0 = lockManager.tryAcquire(key1, ts0);
        assertTrue(fut0.isDone());

        CompletableFuture<Void> fut1 = lockManager.tryAcquire(key2, ts1);
        assertTrue(fut1.isDone());

        try {
            lockManager.tryAcquire(key2, ts0);

            fail();
        }
        catch (LockException e) {
            // Expected.
        }

    }

    /**
     * @param duration The duration.
     * @param rLocks Read lock accumulator.
     * @param wLocks Write lock accumulator.
     * @param fLocks Failed lock accumulator.
     * @param mode Mode: 0 - read only, 1 - write only, 2 - mixed random.
     * @throws InterruptedException If interrupted while waiting.
     */
    private void doTestSingleKeyMultithreaded(
        long duration,
        LongAdder rLocks,
        LongAdder wLocks,
        LongAdder fLocks,
        int mode
    ) throws InterruptedException {
        Object key = new String("test");

        Thread[] threads = new Thread[Runtime.getRuntime().availableProcessors() * 2];;

        CyclicBarrier startBar = new CyclicBarrier(threads.length, () -> log.info("Before test"));

        AtomicBoolean stop = new AtomicBoolean();

        Random r = new Random();

        try {
            for (int i = 0; i < threads.length; i++) {
                threads[i] = new Thread(() -> {
                    try {
                        startBar.await();
                    }
                    catch (Exception e) {
                        fail();
                    }

                    while (!stop.get()) {
                        Timestamp timestamp = Timestamp.nextVersion();

                        if (mode == 0 ? false : mode == 1 ? true : r.nextBoolean()) {
                            try {
                                CompletableFuture<Void> fut = lockManager.tryAcquire(key, timestamp);
                                try {
                                    fut.get();
                                    wLocks.increment();
                                }
                                catch (Exception e) {
                                    fail("Expected normal execution");
                                }
                            }
                            catch (LockException e) {
                                fLocks.increment();
                                continue;
                            }

                            try {
                                lockManager.tryRelease(key, timestamp);
                            }
                            catch (LockException e) {
                                fail(e.getMessage());
                            }
                        }
                        else {
                            try {
                                CompletableFuture<Void> fut = lockManager.tryAcquireShared(key, timestamp);
                                try {
                                    fut.get();
                                    rLocks.increment();
                                }
                                catch (Exception e) {
                                    fail("Expected normal execution");
                                }
                            }
                            catch (LockException e) {
                                fLocks.increment();
                                continue;
                            }

                            try {
                                lockManager.tryReleaseShared(key, timestamp);
                            }
                            catch (LockException e) {
                                fail(e.getMessage());
                            }
                        }
                    }
                });

                threads[i].setName("Worker" + i);
                threads[i].start();
            }

            Thread.sleep(duration);

            stop.set(true);
        } finally {
            for (Thread thread : threads)
                thread.join();
        }

        log.info("After test rLocks={} wLocks={} fLocks={}", rLocks.sum(), wLocks.sum(), fLocks.sum());

        assertTrue(lockManager.queue(key).isEmpty());
    }

    private Timestamp[] generate(int num) {
        Timestamp[] tmp = new Timestamp[num];

        for (int i = 0; i < tmp.length; i++)
            tmp[i] = Timestamp.nextVersion();

        for (int i = 1; i < tmp.length; i++)
            assertTrue(tmp[i - 1].compareTo(tmp[i]) < 0);

        return tmp;
    }
}
