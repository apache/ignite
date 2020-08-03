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

package org.apache.ignite.internal.processors.cache.distributed;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteInterruptedException;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.PartitionsExchangeAware;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Test covering semantics of {@link PartitionsExchangeAware} interface for components.
 */
public class PartitionsExchangeAwareTest extends GridCommonAbstractTest {
    /** Nodes count. */
    private static final int NODES_CNT = 2;

    /** Ip finder. */
    private static final TcpDiscoveryVmIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Atomic cache name. */
    private static final String ATOMIC_CACHE_NAME = "atomic";

    /** Transactional cache name. */
    private static final String TX_CACHE_NAME = "tx";

    /** Timeout seconds. */
    public static final int TIMEOUT_SECONDS = 10;

    /** Initialize before lock reached latch. */
    private CountDownLatch initBeforeLockReachedLatch;

    /** Initialize before lock wait latch. */
    private CountDownLatch initBeforeLockWaitLatch;

    /** Initialize after lock reached latch. */
    private CountDownLatch initAfterLockReachedLatch;

    /** Initialize after lock wait latch. */
    private CountDownLatch initAfterLockWaitLatch;

    /** On done before lock reached latch. */
    private CountDownLatch onDoneBeforeUnlockReachedLatch;

    /** On done before lock wait latch. */
    private CountDownLatch onDoneBeforeUnlockWaitLatch;

    /** On done after lock reached latch. */
    private CountDownLatch onDoneAfterUnlockReachedLatch;

    /** On done after lock wait latch. */
    private CountDownLatch onDoneAfterUnlockWaitLatch;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(IP_FINDER));

        cfg.setDataStorageConfiguration(new DataStorageConfiguration().setDefaultDataRegionConfiguration(
            new DataRegionConfiguration().setMaxSize(100 * 1024 * 1024)));

        CacheConfiguration atomicCfg = new CacheConfiguration()
            .setName(ATOMIC_CACHE_NAME)
            .setAffinity(new RendezvousAffinityFunction(false, 16));

        CacheConfiguration txCfg = new CacheConfiguration()
            .setName(TX_CACHE_NAME)
            .setAffinity(new RendezvousAffinityFunction(false, 16));

        cfg.setCacheConfiguration(atomicCfg, txCfg);

        return cfg;
    }

    /**
     * Init before test.
     */
    @Before
    public void init() {
        initBeforeLockReachedLatch = new CountDownLatch(NODES_CNT);
        initBeforeLockWaitLatch = new CountDownLatch(1);

        initAfterLockReachedLatch = new CountDownLatch(NODES_CNT);
        initAfterLockWaitLatch = new CountDownLatch(1);

        onDoneBeforeUnlockReachedLatch = new CountDownLatch(NODES_CNT);
        onDoneBeforeUnlockWaitLatch = new CountDownLatch(1);

        onDoneAfterUnlockReachedLatch = new CountDownLatch(NODES_CNT);
        onDoneAfterUnlockWaitLatch = new CountDownLatch(1);

        stopAllGrids();
    }

    /**
     * Cleanup after test.
     */
    @After
    public void cleanUp() {
        initBeforeLockWaitLatch.countDown();
        initAfterLockWaitLatch.countDown();
        onDoneBeforeUnlockWaitLatch.countDown();
        onDoneAfterUnlockWaitLatch.countDown();

        stopAllGrids();
    }

    /**
     * Checks that updates are impossible during PME exactly from the moment topologies are locked
     * and until exchange future is completed.
     *
     * @throws Exception if failed.
     */
    @Test
    public void testPartitionsExchangeAware() throws Exception {
        startGrids(2);

        awaitPartitionMapExchange();

        IgniteEx ig0 = grid(0);
        IgniteEx ig1 = grid(1);

        IgniteCache<Integer, Integer> atomicCache = ig0.cache(ATOMIC_CACHE_NAME);
        IgniteCache<Integer, Integer> txCache = ig1.cache(TX_CACHE_NAME);

        PartitionsExchangeAware exchangeAware = new PartitionsExchangeAware() {
            /** {@inheritDoc} */
            @Override public void onInitBeforeTopologyLock(GridDhtPartitionsExchangeFuture fut) {
                try {
                    initBeforeLockReachedLatch.countDown();
                    initBeforeLockWaitLatch.await();
                }
                catch (InterruptedException e) {
                    throw new IgniteInterruptedException(e);
                }
            }

            /** {@inheritDoc} */
            @Override public void onInitAfterTopologyLock(GridDhtPartitionsExchangeFuture fut) {
                try {
                    initAfterLockReachedLatch.countDown();
                    initAfterLockWaitLatch.await();
                }
                catch (InterruptedException e) {
                    throw new IgniteInterruptedException(e);
                }
            }

            /** {@inheritDoc} */
            @Override public void onDoneBeforeTopologyUnlock(GridDhtPartitionsExchangeFuture fut) {
                try {
                    onDoneBeforeUnlockReachedLatch.countDown();
                    onDoneBeforeUnlockWaitLatch.await();
                }
                catch (InterruptedException e) {
                    throw new IgniteInterruptedException(e);
                }
            }

            /** {@inheritDoc} */
            @Override public void onDoneAfterTopologyUnlock(GridDhtPartitionsExchangeFuture fut) {
                try {
                    onDoneAfterUnlockReachedLatch.countDown();
                    onDoneAfterUnlockWaitLatch.await();
                }
                catch (InterruptedException e) {
                    throw new IgniteInterruptedException(e);
                }
            }
        };

        ig0.context().cache().context().exchange().registerExchangeAwareComponent(exchangeAware);
        ig1.context().cache().context().exchange().registerExchangeAwareComponent(exchangeAware);

        GridTestUtils.runAsync(() -> startGrid(2));

        assertTrue(initBeforeLockReachedLatch.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
        assertUpdateIsPossible(atomicCache, txCache, true);

        initBeforeLockWaitLatch.countDown();

        assertTrue(initAfterLockReachedLatch.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
        assertUpdateIsPossible(atomicCache, txCache, false);

        initAfterLockWaitLatch.countDown();

        assertTrue(onDoneBeforeUnlockReachedLatch.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
        assertUpdateIsPossible(atomicCache, txCache, false);

        onDoneBeforeUnlockWaitLatch.countDown();

        assertTrue(onDoneAfterUnlockReachedLatch.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
        assertUpdateIsPossible(atomicCache, txCache, true);

        onDoneAfterUnlockWaitLatch.countDown();

        System.out.println("^^^^success");
    }

    /**
     * Asserts that update operations do (or don't) hang according to the passed flag.
     *
     * @param atomicCache Atomic cache.
     * @param txCache Tx cache.
     * @param updatePossible Falg whether update is possible.
     */
    private static void assertUpdateIsPossible(
        IgniteCache<Integer, Integer> atomicCache,
        IgniteCache<Integer, Integer> txCache,
        boolean updatePossible
    ) throws IgniteInterruptedCheckedException {
        Map<Integer, Integer> putAllArg = new HashMap<>();
        IntStream.of(100).forEach(i -> putAllArg.put(i, i));

        final IgniteInternalFuture txUpdateFut = GridTestUtils.runAsync(new Runnable() {
            @Override public void run() {
                txCache.putAll(putAllArg);
            }
        });

        final IgniteInternalFuture atomicUpdateFut = GridTestUtils.runAsync(new Runnable() {
            @Override public void run() {
                atomicCache.putAll(putAllArg);
            }
        });

        assertEquals(updatePossible, GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                return atomicUpdateFut.isDone() && txUpdateFut.isDone();
            }
        }, TIMEOUT_SECONDS * 1000));

        if (!updatePossible) {
            assertFalse(atomicUpdateFut.isDone());

            assertFalse(txUpdateFut.isDone());
        }
    }
}
