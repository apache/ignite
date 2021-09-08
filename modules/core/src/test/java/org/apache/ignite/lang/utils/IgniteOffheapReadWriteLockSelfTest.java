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

package org.apache.ignite.lang.utils;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.internal.util.OffheapReadWriteLock;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 *
 */
@SuppressWarnings("BusyWait")
public class IgniteOffheapReadWriteLockSelfTest extends GridCommonAbstractTest {
    /** */
    private static final int TAG_0 = 1;

    /** Number of 1-second iterations in every test. */
    public static final int ROUNDS_PER_TEST = 5;

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testConcurrentUpdatesSingleLock() throws Exception {
        final int numPairs = 100;
        final Pair[] data = new Pair[numPairs];

        for (int i = 0; i < numPairs; i++)
            data[i] = new Pair();

        final OffheapReadWriteLock lock = new OffheapReadWriteLock(16);

        final long ptr = GridUnsafe.allocateMemory(OffheapReadWriteLock.LOCK_SIZE);

        lock.init(ptr, TAG_0);

        final AtomicInteger reads = new AtomicInteger();
        final AtomicInteger writes = new AtomicInteger();
        final AtomicBoolean done = new AtomicBoolean(false);

        IgniteInternalFuture<Long> fut = GridTestUtils.runMultiThreadedAsync(new Callable<Object>() {
            /** {@inheritDoc} */
            @Override public Object call() {
                try {
                    ThreadLocalRandom rnd = ThreadLocalRandom.current();

                    while (!done.get()) {
                        boolean write = rnd.nextInt(10) < 2;

                        if (write) {
                            boolean locked = lock.writeLock(ptr, TAG_0);

                            try {
                                // No tag change in this test.
                                assert locked;

                                assertTrue(lock.isWriteLocked(ptr));
                                assertFalse(lock.isReadLocked(ptr));

                                int idx = rnd.nextInt(numPairs);
                                int delta = rnd.nextInt(100_000);

                                data[idx].a += delta;
                                data[idx].b -= delta;
                            }
                            finally {
                                lock.writeUnlock(ptr, TAG_0);
                            }

                            writes.incrementAndGet();
                        }
                        else {
                            boolean locked = lock.readLock(ptr, TAG_0);

                            try {
                                assert locked;

                                assertFalse(lock.isWriteLocked(ptr));
                                assertTrue(lock.isReadLocked(ptr));

                                for (int i1 = 0; i1 < data.length; i1++) {
                                    Pair pair = data[i1];

                                    assertEquals("Failed check for index: " + i1, pair.a, -pair.b);
                                }
                            }
                            finally {
                                lock.readUnlock(ptr);
                            }

                            reads.incrementAndGet();
                        }
                    }
                }
                catch (Throwable e) {
                    e.printStackTrace();
                }

                return null;
            }
        }, 32, "tester");

        for (int i = 0; i < ROUNDS_PER_TEST; i++) {
            Thread.sleep(1_000);

            info("Reads: " + reads.getAndSet(0) + ", writes=" + writes.getAndSet(0));
        }

        done.set(true);

        fut.get();

        validate(data);
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testConcurrentUpdatesMultipleLocks() throws Exception {
        final int numPairs = 100;
        final Pair[] data = new Pair[numPairs];

        final OffheapReadWriteLock lock = new OffheapReadWriteLock(16);

        final long ptr = GridUnsafe.allocateMemory(OffheapReadWriteLock.LOCK_SIZE * numPairs);

        for (int i = 0; i < numPairs; i++) {
            data[i] = new Pair();

            lock.init(ptr + i * OffheapReadWriteLock.LOCK_SIZE, TAG_0);
        }

        final AtomicInteger reads = new AtomicInteger();
        final AtomicInteger writes = new AtomicInteger();
        final AtomicBoolean done = new AtomicBoolean(false);

        IgniteInternalFuture<Long> fut = GridTestUtils.runMultiThreadedAsync(new Callable<Object>() {
            /** {@inheritDoc} */
            @Override public Object call() {
                ThreadLocalRandom rnd = ThreadLocalRandom.current();

                while (!done.get()) {
                    boolean write = rnd.nextInt(10) < 2;
                    int idx = rnd.nextInt(numPairs);

                    long lPtr = ptr + idx * OffheapReadWriteLock.LOCK_SIZE;

                    if (write) {
                        lock.writeLock(lPtr, TAG_0);

                        try {
                            assertTrue(lock.isWriteLocked(lPtr));
                            assertFalse(lock.isReadLocked(lPtr));

                            int delta = rnd.nextInt(100_000);

                            data[idx].a += delta;
                            data[idx].b -= delta;
                        }
                        finally {
                            lock.writeUnlock(lPtr, TAG_0);
                        }

                        writes.incrementAndGet();
                    }
                    else {
                        lock.readLock(lPtr, TAG_0);

                        try {
                            assertFalse(lock.isWriteLocked(lPtr));
                            assertTrue(lock.isReadLocked(lPtr));

                            Pair pair = data[idx];

                            assertEquals("Failed check for index: " + idx, pair.a, -pair.b);
                        }
                        finally {
                            lock.readUnlock(lPtr);
                        }

                        reads.incrementAndGet();
                    }
                }

                return null;
            }
        }, 32, "tester");

        for (int i = 0; i < ROUNDS_PER_TEST; i++) {
            Thread.sleep(1_000);

            info("Reads: " + reads.getAndSet(0) + ", writes=" + writes.getAndSet(0));
        }

        done.set(true);

        fut.get();

        validate(data);
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testLockUpgradeMultipleLocks() throws Exception {
        final int numPairs = 100;
        final Pair[] data = new Pair[numPairs];

        final OffheapReadWriteLock lock = new OffheapReadWriteLock(16);

        final long ptr = GridUnsafe.allocateMemory(OffheapReadWriteLock.LOCK_SIZE * numPairs);

        for (int i = 0; i < numPairs; i++) {
            data[i] = new Pair();

            lock.init(ptr + i * OffheapReadWriteLock.LOCK_SIZE, TAG_0);
        }

        final AtomicInteger reads = new AtomicInteger();
        final AtomicInteger writes = new AtomicInteger();
        final AtomicInteger successfulUpgrades = new AtomicInteger();
        final AtomicBoolean done = new AtomicBoolean(false);

        IgniteInternalFuture<Long> fut = GridTestUtils.runMultiThreadedAsync(new Callable<Object>() {
            /** {@inheritDoc} */
            @Override public Object call() {
                ThreadLocalRandom rnd = ThreadLocalRandom.current();

                while (!done.get()) {
                    int idx = rnd.nextInt(numPairs);

                    long lPtr = ptr + idx * OffheapReadWriteLock.LOCK_SIZE;

                    boolean locked = lock.readLock(lPtr, TAG_0);

                    boolean write = false;

                    try {
                        assert locked;

                        Pair pair = data[idx];

                        assertEquals("Failed check for index: " + idx, pair.a, -pair.b);

                        write = rnd.nextInt(10) < 2;

                        if (write) {
                            // TAG fail will cause NPE.
                            boolean upg = lock.upgradeToWriteLock(lPtr, TAG_0);

                            writes.incrementAndGet();

                            if (upg)
                                successfulUpgrades.incrementAndGet();

                            int delta = rnd.nextInt(100_000);

                            pair.a += delta;
                            pair.b -= delta;
                        }
                    }
                    finally {
                        if (write)
                            lock.writeUnlock(lPtr, TAG_0);
                        else
                            lock.readUnlock(lPtr);
                    }

                    reads.incrementAndGet();
                }

                return null;
            }
        }, 32, "tester");

        for (int i = 0; i < ROUNDS_PER_TEST; i++) {
            Thread.sleep(1_000);

            info("Reads=" + reads.getAndSet(0) + ", writes=" + writes.getAndSet(0) + ", upgrades=" + successfulUpgrades.getAndSet(0));
        }

        done.set(true);

        fut.get();

        validate(data);
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testTagIdUpdateWait() throws Exception {
        checkTagIdUpdate(true);
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testTagIdUpdateContinuous() throws Exception {
        checkTagIdUpdate(false);
    }

    /**
     * @throws Exception if failed.
     */
    private void checkTagIdUpdate(final boolean waitBeforeSwitch) throws Exception {
        final int numPairs = 100;
        final Pair[] data = new Pair[numPairs];

        for (int i = 0; i < numPairs; i++)
            data[i] = new Pair();

        final OffheapReadWriteLock lock = new OffheapReadWriteLock(16);

        final long ptr = GridUnsafe.allocateMemory(OffheapReadWriteLock.LOCK_SIZE);

        lock.init(ptr, TAG_0);

        final AtomicInteger reads = new AtomicInteger();
        final AtomicInteger writes = new AtomicInteger();
        final AtomicBoolean done = new AtomicBoolean(false);
        final AtomicBoolean run = new AtomicBoolean(true);

        final int threadCnt = 32;

        final CyclicBarrier barr = new CyclicBarrier(threadCnt, () -> { if (done.get()) run.set(false); });

        IgniteInternalFuture<Long> fut = GridTestUtils.runMultiThreadedAsync(new Callable<Object>() {
            /** {@inheritDoc} */
            @Override public Object call() {
                try {
                    ThreadLocalRandom rnd = ThreadLocalRandom.current();

                    int tag = TAG_0;

                    long lastSwitch = System.currentTimeMillis();

                    while (run.get()) {
                        boolean write = rnd.nextInt(10) < 2;

                        boolean locked;

                        boolean switched = false;

                        if (write) {
                            locked = lock.writeLock(ptr, tag);

                            if (locked) {
                                try {
                                    assertTrue(lock.isWriteLocked(ptr));
                                    assertFalse(lock.isReadLocked(ptr));

                                    int idx = rnd.nextInt(numPairs);
                                    int delta = rnd.nextInt(100_000);

                                    data[idx].a += delta;
                                    data[idx].b -= delta;
                                }
                                finally {
                                    switched = System.currentTimeMillis() - lastSwitch > 1_000 || !waitBeforeSwitch;

                                    if (switched && waitBeforeSwitch)
                                        info("Switching...");

                                    int tag1 = (tag + (switched ? 1 : 0)) & 0xFFFF;

                                    if (tag1 == 0)
                                        tag1 = 1;

                                    lock.writeUnlock(ptr, tag1);
                                }

                                writes.incrementAndGet();
                            }
                        }
                        else {
                            locked = lock.readLock(ptr, tag);

                            if (locked) {
                                try {
                                    assert locked;

                                    assertFalse(lock.isWriteLocked(ptr));
                                    assertTrue(lock.isReadLocked(ptr));

                                    for (int i1 = 0; i1 < data.length; i1++) {
                                        Pair pair = data[i1];

                                        assertEquals("Failed check for index: " + i1, pair.a, -pair.b);
                                    }
                                }
                                finally {
                                    lock.readUnlock(ptr);
                                }

                                reads.incrementAndGet();
                            }
                        }

                        if (!locked || switched) {
                            try {
                                barr.await();
                            }
                            catch (BrokenBarrierException e) {
                                // Done.
                                e.printStackTrace();

                                return null;
                            }

                            tag = (tag + 1) & 0xFFFF;

                            if (tag == 0)
                                tag = 1;

                            if (waitBeforeSwitch || (!waitBeforeSwitch && tag == 1))
                                info("Switch to a new tag: " + tag);

                            lastSwitch = System.currentTimeMillis();
                        }
                    }
                }
                catch (Throwable e) {
                    e.printStackTrace();
                }

                return null;
            }
        }, threadCnt, "tester");

        for (int i = 0; i < ROUNDS_PER_TEST; i++) {
            Thread.sleep(1_000);

            info("Reads: " + reads.getAndSet(0) + ", writes=" + writes.getAndSet(0));
        }

        done.set(true);

        fut.get();

        validate(data);
    }

    /**
     * Validates data integrity.
     *
     * @param data Data to validate.
     */
    private void validate(Pair[] data) {
        for (int i = 0; i < data.length; i++) {
            Pair pair = data[i];

            assertEquals("Failed for index: " + i, pair.a, -pair.b);
        }
    }

    private static class Pair {
        /** */
        private int a;

        /** */
        private int b;
    }
}
