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

package org.apache.ignite.internal.processors.cache.datastructures;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Callable;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteLock;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.AtomicConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/** */
public class IgniteReentrantLockTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final int NODES_CNT = 4;

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

        /*atomicCfg.setCacheMode(CacheMode.PARTITIONED);

        atomicCfg.setBackups(1);*/

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
     * @throws Exception If failed.
     */
    public void testInitialization() throws Exception {
        try (IgniteLock lock = createReentrantLock(0, "lock1", false)) {
        }

        try (IgniteLock lock = createReentrantLock(0, "lock2", true)) {
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testReentrantLock() throws Exception {
        testReentrantLock(true);
        testReentrantLock(false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testReentrantLock(final boolean fair) throws Exception {
        for (int i = 0; i < NODES_CNT; i++) {
            IgniteLock lock = createReentrantLock(i, fair?"lock1":"lock2", fair);

            while (lock.isLocked());

            lock.lock();

            assertTrue(lock.isLocked());

            lock.unlock();

            // Method unlock() is async, so we may not see the result for a while.
            while (lock.isLocked());
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testReentrantLockMultinode() throws Exception {
        testReentrantLockMultinode(false);
        testReentrantLockMultinode(true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testReentrantLockMultinode(final boolean fair) throws Exception {
        List<IgniteInternalFuture<Void>> futs = new ArrayList<>();

        for (int i = 0; i < NODES_CNT; i++) {
            final Ignite ignite = grid(i);
            final int inx = i;

            futs.add(GridTestUtils.runAsync(new Callable<Void>() {
                @Override public Void call() throws Exception {
                    IgniteLock lock = createReentrantLock(inx, fair?"lock1":"lock2", fair);

                    lock.lock();

                    try {
                        assertTrue(lock.isLocked());
                        Thread.sleep(1_000L);
                    } finally {
                        lock.unlock();
                    }

                    return null;
                }
            }));
        }

        for (IgniteInternalFuture<?> fut : futs)
            fut.get(30_000L);
    }

    /**
     * @throws Exception If failed.
     */
    public void testReentrantLockMultinodeFailoverUnfair() throws Exception {
        testReentrantLockMultinodeFailover(false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testReentrantLockMultinodeFailoverFair() throws Exception {
        testReentrantLockMultinodeFailover(true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testReentrantLockMultinodeFailover(final boolean fair) throws Exception {
        List<IgniteInternalFuture<Void>> futs = new ArrayList<>(NODES_CNT);

        for (int i = 0; i < NODES_CNT; i++) {
            final int inx = i;
            final boolean flag = RND.nextBoolean();

            futs.add(GridTestUtils.runAsync(new Callable<Void>() {
                @Override public Void call() throws Exception {
                    IgniteLock lock = createReentrantLock(inx, fair ? "lock1": "lock2", fair);

                    UUID id = grid(inx).cluster().localNode().id();

                    lock.lock();

                    if (flag) {
                        try {
                            grid(inx).close();
                        }
                        catch (Exception ignored) {
                            lock.unlock();
                        }

                        return null;
                    }

                    try {
                        assertTrue(lock.isLocked());

                        Thread.sleep(1_000L);
                    }
                    finally {
                        lock.unlock();
                    }

                    return null;
                }
            }));
        }

        for (IgniteInternalFuture<?> fut : futs)
            fut.get(60_000L);
    }

    /**
     * @throws Exception If failed.
     */
    public void testReentrantLockMultinodeFailoverMultilocksUnfair() throws Exception {
        testReentrantLockMultinodeFailoverMultilocks(false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testReentrantLockMultinodeFailoverMultilocksFair() throws Exception {
        testReentrantLockMultinodeFailoverMultilocks(true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testReentrantLockMultinodeFailoverMultilocks(final boolean fair) throws Exception {
        List<IgniteInternalFuture<Void>> futs = new ArrayList<>(NODES_CNT*2);

        for (int i = 0; i < NODES_CNT; i++) {
            final int inx = i;
            final boolean flag = RND.nextBoolean();

            futs.add(GridTestUtils.runAsync(new Callable<Void>() {
                @Override public Void call() throws Exception {
                    IgniteLock lock1 = createReentrantLock(inx, fair ? "lock1f": "lock1u", fair);
                    IgniteLock lock2 = createReentrantLock(inx, fair ? "lock2f": "lock2u", fair);

                    UUID id = grid(inx).cluster().localNode().id();

                    lock1.lock();
                    lock2.lock();

                    if (flag) {
                        try {
                            grid(inx).close();
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

                        Thread.sleep(1_000L);
                    }
                    finally {
                        lock2.unlock();
                        lock1.unlock();
                    }

                    return null;
                }
            }));
        }


        for (IgniteInternalFuture<?> fut : futs)
            fut.get(60_000L);
    }

    /**
     * @param lockName Reentrant lock name.
     * @param fair Fairness flag.
     * @return Distributed reentrant lock.
     * @throws Exception If failed.
     */
    private IgniteLock createReentrantLock(int cnt, String lockName, boolean fair) throws Exception {
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
