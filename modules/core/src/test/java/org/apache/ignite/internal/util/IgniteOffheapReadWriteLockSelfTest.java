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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.junit.jupiter.api.Test;

/** Tests basic invariants of {@link OffheapReadWriteLock}. */
public class IgniteOffheapReadWriteLockSelfTest extends BaseIgniteAbstractTest {
    /** Initial value for tag in tests. */
    private static final int TAG_0 = 1;

    /** Number of 1-second iterations in every test. */
    private static final int ROUNDS_PER_TEST = 3;

    /** Sleep interval for the test. */
    private static final long SLEEP_TIME = 50L;

    @Test
    public void testConcurrentUpdatesSingleLock() throws Exception {
        final int numPairs = 100;
        final Pair[] data = new Pair[numPairs];

        for (int i = 0; i < numPairs; i++) {
            data[i] = new Pair();
        }

        final OffheapReadWriteLock lock = new OffheapReadWriteLock(16);

        final long ptr = GridUnsafe.allocateMemory(OffheapReadWriteLock.LOCK_SIZE);

        lock.init(ptr, TAG_0);

        final AtomicInteger reads = new AtomicInteger();
        final AtomicInteger writes = new AtomicInteger();
        final AtomicBoolean done = new AtomicBoolean(false);

        CompletableFuture<Long> fut = IgniteTestUtils.runMultiThreadedAsync(() -> {
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

                            data[idx].left += delta;
                            data[idx].right -= delta;
                        } finally {
                            lock.writeUnlock(ptr, TAG_0);
                        }

                        writes.incrementAndGet();
                    } else {
                        boolean locked = lock.readLock(ptr, TAG_0);

                        try {
                            assert locked;

                            assertFalse(lock.isWriteLocked(ptr));
                            assertTrue(lock.isReadLocked(ptr));

                            for (int i1 = 0; i1 < data.length; i1++) {
                                Pair pair = data[i1];

                                assertEquals(pair.left, -pair.right, "Failed check for index: " + i1);
                            }
                        } finally {
                            lock.readUnlock(ptr);
                        }

                        reads.incrementAndGet();
                    }
                }
            } catch (Throwable e) {
                log.error(e.getMessage(), e);
            }

            return null;
        }, 32, "tester");

        for (int i = 0; i < ROUNDS_PER_TEST; i++) {
            Thread.sleep(SLEEP_TIME);

            log.info("Reads: " + reads.getAndSet(0) + ", writes=" + writes.getAndSet(0));
        }

        done.set(true);

        fut.get();

        validate(data);
    }

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

        CompletableFuture<Long> fut = IgniteTestUtils.runMultiThreadedAsync(() -> {
            ThreadLocalRandom rnd = ThreadLocalRandom.current();

            while (!done.get()) {
                boolean write = rnd.nextInt(10) < 2;
                int idx = rnd.nextInt(numPairs);

                long lockPtr = ptr + idx * OffheapReadWriteLock.LOCK_SIZE;

                if (write) {
                    lock.writeLock(lockPtr, TAG_0);

                    try {
                        assertTrue(lock.isWriteLocked(lockPtr));
                        assertFalse(lock.isReadLocked(lockPtr));

                        int delta = rnd.nextInt(100_000);

                        data[idx].left += delta;
                        data[idx].right -= delta;
                    } finally {
                        lock.writeUnlock(lockPtr, TAG_0);
                    }

                    writes.incrementAndGet();
                } else {
                    lock.readLock(lockPtr, TAG_0);

                    try {
                        assertFalse(lock.isWriteLocked(lockPtr));
                        assertTrue(lock.isReadLocked(lockPtr));

                        Pair pair = data[idx];

                        assertEquals(pair.left, -pair.right, "Failed check for index: " + idx);
                    } finally {
                        lock.readUnlock(lockPtr);
                    }

                    reads.incrementAndGet();
                }
            }

            return null;
        }, 32, "tester");

        for (int i = 0; i < ROUNDS_PER_TEST; i++) {
            Thread.sleep(SLEEP_TIME);

            log.info("Reads: " + reads.getAndSet(0) + ", writes=" + writes.getAndSet(0));
        }

        done.set(true);

        fut.get();

        validate(data);
    }

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

        CompletableFuture<Long> fut = IgniteTestUtils.runMultiThreadedAsync(() -> {
            ThreadLocalRandom rnd = ThreadLocalRandom.current();

            while (!done.get()) {
                int idx = rnd.nextInt(numPairs);

                long lockPtr = ptr + idx * OffheapReadWriteLock.LOCK_SIZE;

                boolean locked = lock.readLock(lockPtr, TAG_0);

                boolean write = false;

                try {
                    assert locked;

                    Pair pair = data[idx];

                    assertEquals(pair.left, -pair.right, "Failed check for index: " + idx);

                    write = rnd.nextInt(10) < 2;

                    if (write) {
                        // TAG fail will cause NPE.
                        boolean upg = lock.upgradeToWriteLock(lockPtr, TAG_0);

                        writes.incrementAndGet();

                        if (upg) {
                            successfulUpgrades.incrementAndGet();
                        }

                        int delta = rnd.nextInt(100_000);

                        pair.left += delta;
                        pair.right -= delta;
                    }
                } finally {
                    if (write) {
                        lock.writeUnlock(lockPtr, TAG_0);
                    } else {
                        lock.readUnlock(lockPtr);
                    }
                }

                reads.incrementAndGet();
            }

            return null;
        }, 32, "tester");

        for (int i = 0; i < ROUNDS_PER_TEST; i++) {
            Thread.sleep(SLEEP_TIME);

            log.info("Reads=" + reads.getAndSet(0) + ", writes=" + writes.getAndSet(0) + ", upgrades=" + successfulUpgrades.getAndSet(0));
        }

        done.set(true);

        fut.get();

        validate(data);
    }

    @Test
    public void testTagIdUpdateWait() throws Exception {
        checkTagIdUpdate(true);
    }

    @Test
    public void testTagIdUpdateContinuous() throws Exception {
        checkTagIdUpdate(false);
    }

    private void checkTagIdUpdate(final boolean waitBeforeSwitch) throws Exception {
        final int numPairs = 100;
        final Pair[] data = new Pair[numPairs];

        for (int i = 0; i < numPairs; i++) {
            data[i] = new Pair();
        }

        final OffheapReadWriteLock lock = new OffheapReadWriteLock(16);

        final long ptr = GridUnsafe.allocateMemory(OffheapReadWriteLock.LOCK_SIZE);

        lock.init(ptr, TAG_0);

        final AtomicInteger reads = new AtomicInteger();
        final AtomicInteger writes = new AtomicInteger();
        final AtomicBoolean done = new AtomicBoolean(false);
        final AtomicBoolean run = new AtomicBoolean(true);

        final int threadCnt = 32;

        final CyclicBarrier barr = new CyclicBarrier(threadCnt, () -> {
            if (done.get()) {
                run.set(false);
            }
        });

        CompletableFuture<Long> fut = IgniteTestUtils.runMultiThreadedAsync(() -> {
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

                                data[idx].left += delta;
                                data[idx].right -= delta;
                            } finally {
                                switched = System.currentTimeMillis() - lastSwitch > 20 || !waitBeforeSwitch;

                                if (switched && waitBeforeSwitch) {
                                    log.info("Switching...");
                                }

                                int tag1 = (tag + (switched ? 1 : 0)) & 0xFFFF;

                                if (tag1 == 0) {
                                    tag1 = 1;
                                }

                                lock.writeUnlock(ptr, tag1);
                            }

                            writes.incrementAndGet();
                        }
                    } else {
                        locked = lock.readLock(ptr, tag);

                        if (locked) {
                            try {
                                assert locked;

                                assertFalse(lock.isWriteLocked(ptr));
                                assertTrue(lock.isReadLocked(ptr));

                                for (int i1 = 0; i1 < data.length; i1++) {
                                    Pair pair = data[i1];

                                    assertEquals(pair.left, -pair.right, "Failed check for index: " + i1);
                                }
                            } finally {
                                lock.readUnlock(ptr);
                            }

                            reads.incrementAndGet();
                        }
                    }

                    if (!locked || switched) {
                        try {
                            barr.await();
                        } catch (BrokenBarrierException e) {
                            // Done.
                            log.error(e.getMessage(), e);

                            return null;
                        }

                        tag = (tag + 1) & 0xFFFF;

                        if (tag == 0) {
                            tag = 1;
                        }

                        if (waitBeforeSwitch || (!waitBeforeSwitch && tag == 1)) {
                            log.info("Switch to a new tag: " + tag);
                        }

                        lastSwitch = System.currentTimeMillis();
                    }
                }
            } catch (Throwable e) {
                log.error(e.getMessage(), e);
            }

            return null;
        }, threadCnt, "tester");

        for (int i = 0; i < ROUNDS_PER_TEST; i++) {
            Thread.sleep(SLEEP_TIME);

            log.info("Reads: " + reads.getAndSet(0) + ", writes=" + writes.getAndSet(0));
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

            assertEquals(pair.left, -pair.right, "Failed for index: " + i);
        }
    }

    /** Pair of integers. */
    private static class Pair {
        /** Left value of the pair. */
        private int left;

        /** Right value of the pair. */
        private int right;
    }
}
