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

package org.apache.ignite.internal.processors.datastructures;

import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteLock;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.AtomicConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Tests for {@link org.apache.ignite.internal.processors.datastructures.GridCacheLockImpl2Fair} and for {@link
 * org.apache.ignite.internal.processors.datastructures.GridCacheLockImpl2Unfair}
 */
public class IgniteReentrantLockTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final int NODES_CNT = 4;

    /** */
    private static final int THREAD_CNT = 5;

    /** */
    private static final Random RND = new Random();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        TcpDiscoverySpi spi = new TcpDiscoverySpi();

        spi.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(spi);

        AtomicConfiguration atomicCfg = new AtomicConfiguration();

        assertNotNull(atomicCfg);

        atomicCfg.setCacheMode(CacheMode.REPLICATED);

        cfg.setAtomicConfiguration(atomicCfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        startGridsMultiThreaded(NODES_CNT);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * Test a creating of the reentrant lock.
     *
     * @throws Exception If failed.
     */
    public void testInitialization() throws Exception {
        try (IgniteLock lock = createReentrantLock(0, "unfair lock", false)) {
        }

        try (IgniteLock lock = createReentrantLock(0, "fair lock", true)) {
        }
    }

    /**
     * Test a sequential lock acquiring and releasing.
     *
     * @throws Exception If failed.
     */
    public void testReentrantLock() throws Exception {
        testReentrantLock(true);
        testReentrantLock(false);
    }

    /**
     * Test a sequential lock acquiring and releasing.
     *
     * @throws Exception If failed.
     */
    public void testReentrantLock(boolean fair) throws Exception {
        for (int i = 0; i < NODES_CNT; i++) {
            final IgniteLock lock = createReentrantLock(i, fair ? "fair lock" : "unfair lock", fair);

            // Other node can still don't see update, but eventually we reach lock.isLocked == false.
            GridTestUtils.waitForCondition(new GridAbsPredicate() {
                @Override public boolean apply() {
                    return !lock.isLocked();
                }
            }, 500L);

            lock.lock();

            assertTrue(lock.isLocked());
            assertTrue(lock.isHeldByCurrentThread());

            lock.lock();

            assertTrue(lock.isLocked());
            assertTrue(lock.isHeldByCurrentThread());

            lock.unlock();

            assertTrue(lock.isLocked());
            assertTrue(lock.isHeldByCurrentThread());

            lock.unlock();

            assertFalse(lock.isLocked());
            assertFalse(lock.isHeldByCurrentThread());
        }
    }

    /**
     * Test a sequential lock acquiring and releasing with timeout.
     *
     * @throws Exception If failed.
     */
    public void testReentrantLockTimeout() throws Exception {
        testReentrantLockTimeout(false);
        testReentrantLockTimeout(true);
    }

    /**
     * Test a sequential lock acquiring and releasing with timeout.
     *
     * @throws Exception If failed.
     */
    public void testReentrantLockTimeout(boolean fair) throws Exception {
        for (int i = 0; i < NODES_CNT; i++) {
            IgniteLock lock1 = createReentrantLock(i, fair ? "fair lock" : "unfair lock", fair);
            IgniteLock lock2 = createReentrantLock((i + 1) % NODES_CNT, fair ? "fair lock" : "unfair lock", fair);

            assertTrue(lock1.tryLock(10, TimeUnit.MILLISECONDS));

            assertTrue(lock1.isLocked());
            assertTrue(lock1.isHeldByCurrentThread());

            long start = System.nanoTime();

            assertFalse(lock2.tryLock(500, TimeUnit.MILLISECONDS));

            long delta = (System.nanoTime() - start) / 1_000_000L;

            assertTrue(delta >= 500L);
            assertTrue(delta < 600L);

            lock1.unlock();

            assertFalse(lock1.isHeldByCurrentThread());
        }
    }

    /**
     * Test an async lock acquiring and releasing with many nodes.
     *
     * @throws Exception If failed.
     */
    public void testReentrantLockMultinode() throws Exception {
        testReentrantLockMultinode(false);
        testReentrantLockMultinode(true);
    }

    /**
     * Test an async lock acquiring and releasing with many nodes.
     *
     * @throws Exception If failed.
     */
    public void testReentrantLockMultinode(final boolean fair) throws Exception {
        final AtomicInteger inx = new AtomicInteger(0);

        GridTestUtils.runMultiThreadedAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                IgniteLock lock = createReentrantLock(inx.getAndIncrement() % NODES_CNT,
                    fair ? "fair lock" : "unfair lock", fair);

                lock.lock();

                try {
                    assertTrue(lock.isLocked());
                }
                finally {
                    lock.unlock();
                }

                return null;
            }
        }, NODES_CNT * THREAD_CNT, "worker").get(30_000L);
    }

    /**
     * Test an async lock acquiring and releasing with many nodes and timeout.
     *
     * @throws Exception If failed.
     */
    public void testReentrantLockMultinodeTimeout() throws Exception {
        testReentrantLockMultinodeTimeout(false, false);
        testReentrantLockMultinodeTimeout(false, true);
        testReentrantLockMultinodeTimeout(true, false);
        testReentrantLockMultinodeTimeout(true, true);
    }

    /**
     * Test an async lock acquiring and releasing with many nodes and timeout.
     *
     * @throws Exception If failed.
     */
    public void testReentrantLockMultinodeTimeout(final boolean fair, final boolean time) throws Exception {
        final AtomicInteger inx = new AtomicInteger(0);

        GridTestUtils.runMultiThreadedAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                IgniteLock lock = createReentrantLock(inx.getAndIncrement() % NODES_CNT,
                    fair ? "fair lock" : "unfair lock", fair);

                if (time ? lock.tryLock(10, TimeUnit.NANOSECONDS) : lock.tryLock()) {
                    try {
                        assertTrue(lock.isLocked());
                    }
                    finally {
                        lock.unlock();
                    }
                }

                return null;
            }
        }, NODES_CNT * THREAD_CNT, "worker").get(30_000L);
    }

    /**
     * Test an async lock acquiring and releasing with many nodes with {@link IgniteLock#tryLock()}.
     *
     * @throws Exception If failed.
     */
    public void testReentrantLockMultinodeTryLock() throws Exception {
        testReentrantLockMultinodeTryLock(false);
        testReentrantLockMultinodeTryLock(true);
    }

    /**
     * Test an async lock acquiring and releasing with many nodes with {@link IgniteLock#tryLock()}.
     *
     * @throws Exception If failed.
     */
    public void testReentrantLockMultinodeTryLock(final boolean fair) throws Exception {
        final AtomicInteger inx = new AtomicInteger(0);

        GridTestUtils.runMultiThreadedAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                IgniteLock lock = createReentrantLock(inx.getAndIncrement() % NODES_CNT,
                    fair ? "fair lock1" : "unfair lock", fair);

                if (lock.tryLock()) {
                    try {
                        assertTrue(lock.isLocked());
                    }
                    finally {
                        lock.unlock();
                    }
                }

                return null;
            }
        }, NODES_CNT * THREAD_CNT, "worker").get(30_000L);
    }

    /**
     * Test an async lock acquiring and releasing with many nodes with nodes failing in unfair mode.
     *
     * @throws Exception If failed.
     */
    public void testReentrantLockMultinodeFailoverUnfair() throws Exception {
        testReentrantLockMultinodeFailover(false);
    }

    /**
     * Test an async lock acquiring and releasing with many nodes with nodes failing in fair mode.
     *
     * @throws Exception If failed.
     */
    public void testReentrantLockMultinodeFailoverFair() throws Exception {
        testReentrantLockMultinodeFailover(true);
    }

    /**
     * Test an async lock acquiring and releasing with many nodes with nodes failing.
     *
     * @throws Exception If failed.
     */
    public void testReentrantLockMultinodeFailover(final boolean fair) throws Exception {
        final AtomicInteger inx = new AtomicInteger(0);

        GridTestUtils.runMultiThreadedAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                int i = inx.getAndIncrement();

                boolean shouldClose;

                synchronized (RND) {
                    shouldClose = RND.nextBoolean();
                }

                IgniteLock lock = createReentrantLock(i, fair ? "fair lock" : "unfair lock", fair);

                lock.lock();

                if (shouldClose) {
                    try {
                        grid(i).close();
                    }
                    catch (Exception ignored) {
                        lock.unlock();
                    }

                    return null;
                }

                try {
                    assertTrue(lock.isLocked());
                }
                finally {
                    lock.unlock();
                }

                return null;
            }
        }, NODES_CNT, "worker").get(30_000L);
    }

    /**
     * Test an async lock acquiring and releasing with many nodes with nodes failing with two locks with fair mode.
     *
     * @throws Exception If failed.
     */
    public void testReentrantLockMultinodeFailoverMultilocksUnfair() throws Exception {
        testReentrantLockMultinodeFailoverMultilocks(false);
    }

    /**
     * Test an async lock acquiring and releasing with many nodes with nodes failing with two locks with fair mode.
     *
     * @throws Exception If failed.
     */
    public void testReentrantLockMultinodeFailoverMultilocksFair() throws Exception {
        testReentrantLockMultinodeFailoverMultilocks(true);
    }

    /**
     * Test an async lock acquiring and releasing with many nodes with nodes failing with two locks.
     *
     * @throws Exception If failed.
     */
    public void testReentrantLockMultinodeFailoverMultilocks(final boolean fair) throws Exception {
        final AtomicInteger inx = new AtomicInteger(0);

        GridTestUtils.runMultiThreadedAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                int i = inx.getAndIncrement();

                boolean shouldClose;

                synchronized (RND) {
                    shouldClose = RND.nextBoolean();
                }

                IgniteLock lock1 = createReentrantLock(i, fair ? "lock1f" : "lock1u", fair);
                IgniteLock lock2 = createReentrantLock(i, fair ? "lock2f" : "lock2u", fair);

                lock1.lock();
                lock2.lock();

                if (shouldClose) {
                    try {
                        grid(i).close();
                    }
                    catch (Exception ignored) {
                        lock2.unlock();
                        lock1.unlock();
                    }

                    return null;
                }

                try {
                    assertTrue(lock2.isLocked());
                    assertTrue(lock1.isLocked());
                }
                finally {
                    lock2.unlock();
                    lock1.unlock();
                }

                return null;
            }
        }, NODES_CNT, "worker").get(30_000L);
    }

    /**
     * Create a lock.
     *
     * @param lockName Reentrant lock name.
     * @param fair Fairness flag.
     * @return Distributed reentrant lock.
     * @throws Exception If failed.
     */
    private IgniteLock createReentrantLock(int cnt, String lockName, boolean fair) {
        assert lockName != null;
        assert cnt >= 0;

        IgniteLock lock = grid(cnt).reentrantLock(lockName, fair, true);

        assertNotNull(lock);
        assertEquals(lockName, lock.name());
        assertTrue(lock.isFailoverSafe());
        assertEquals(lock.isFair(), fair);

        return lock;
    }
}
