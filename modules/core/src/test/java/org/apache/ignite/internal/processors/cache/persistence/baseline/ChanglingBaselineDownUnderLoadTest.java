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
package org.apache.ignite.internal.processors.cache.persistence.baseline;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import javax.cache.CacheException;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.BaselineNode;
import org.apache.ignite.cluster.ClusterTopologyException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;

/**
 *
 */
public class ChanglingBaselineDownUnderLoadTest extends GridCommonAbstractTest {
    /** Nodes count. */
    private static final int NODES_COUNT = 6;

    /** Tx cache name. */
    private static final String PARTITIONED_TX_CACHE_NAME = "p-tx-cache";

    /** Tx cache name. */
    private static final String PARTITIONED_TX_PRIM_SYNC_CACHE_NAME = "prim-sync";

    /** Atomic cache name. */
    private static final String PARTITIONED_ATOMIC_CACHE_NAME = "p-atomic-cache";

    /** Tx cache name. */
    private static final String REPLICATED_TX_CACHE_NAME = "r-tx-cache";

    /** Atomic cache name. */
    private static final String REPLICATED_ATOMIC_CACHE_NAME = "r-atomic-cache";

    /** Client grid name. */
    protected static final String CLIENT_GRID_NAME = "client";

    /** Entries. */
    private static final int ENTRIES = 20_000;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDataStorageConfiguration(
            new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration()
                        .setPersistenceEnabled(true)
                        .setInitialSize(800 * 1024 * 1024)
                        .setMaxSize(800 * 1024 * 1024)
                        .setCheckpointPageBufferSize(200 * 1024 * 1024)
                )
        );

        if (igniteInstanceName.startsWith(CLIENT_GRID_NAME))
            cfg.setClientMode(true);

        CacheConfiguration partAtomicCfg = new CacheConfiguration();
        partAtomicCfg.setName(PARTITIONED_ATOMIC_CACHE_NAME);
        partAtomicCfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);
        partAtomicCfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        partAtomicCfg.setAffinity(new RendezvousAffinityFunction(false, 32));
        partAtomicCfg.setBackups(2);

        CacheConfiguration partTxCfg = new CacheConfiguration();
        partTxCfg.setName(PARTITIONED_TX_CACHE_NAME);
        partTxCfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        partTxCfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        partTxCfg.setAffinity(new RendezvousAffinityFunction(false, 32));
        partTxCfg.setBackups(2);

        CacheConfiguration partTxPrimSyncCfg = new CacheConfiguration();
        partTxPrimSyncCfg.setName(PARTITIONED_TX_PRIM_SYNC_CACHE_NAME);
        partTxPrimSyncCfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        partTxPrimSyncCfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.PRIMARY_SYNC);
        partTxPrimSyncCfg.setAffinity(new RendezvousAffinityFunction(false, 41));
        partTxPrimSyncCfg.setBackups(3);

        CacheConfiguration replAtCfg = new CacheConfiguration();
        replAtCfg.setName(REPLICATED_ATOMIC_CACHE_NAME);
        replAtCfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);
        replAtCfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        replAtCfg.setAffinity(new RendezvousAffinityFunction(false, 32));
        replAtCfg.setCacheMode(CacheMode.REPLICATED);

        CacheConfiguration replTxCfg = new CacheConfiguration();
        replTxCfg.setName(REPLICATED_TX_CACHE_NAME);
        replTxCfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        replTxCfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        replTxCfg.setAffinity(new RendezvousAffinityFunction(false, 32));
        replAtCfg.setCacheMode(CacheMode.REPLICATED);

        cfg.setCacheConfiguration(partAtomicCfg, partTxCfg, replAtCfg, replTxCfg, partTxPrimSyncCfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        cleanPersistenceDir();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     *
     */
    public void testPartitionedAtomicCache() throws Exception {
        testCache(PARTITIONED_ATOMIC_CACHE_NAME);
    }

    /**
     *
     */
    public void testPartitionedTxCache() throws Exception {
        testCache(PARTITIONED_TX_CACHE_NAME);
    }

    /**
     *
     */
    public void testReplicatedAtomicCache() throws Exception {
        testCache(REPLICATED_ATOMIC_CACHE_NAME);
    }

    /**
     *
     */
    public void testReplicatedTxCache() throws Exception {
        testCache(REPLICATED_TX_CACHE_NAME);
    }

    /**
     * Tests that changing baseline down under load won't break cache.
     */
    private void testCache(String cacheName) throws Exception {
        IgniteEx ig0 = (IgniteEx)startGrids(NODES_COUNT);

        ig0.cluster().active(true);

        AtomicBoolean stopLoad = new AtomicBoolean(false);

        AtomicReference<Throwable> loadError = new AtomicReference<>(null);

        IgniteCache<Integer, String> cache = ig0.cache(cacheName);

        System.out.println("### Starting preloading");

        for (int i = 0; i < ENTRIES; i++) {
            ThreadLocalRandom r = ThreadLocalRandom.current();

            byte[] randBytes = new byte[r.nextInt(10, 100)];

            cache.put(r.nextInt(ENTRIES), new String(randBytes));
        }

        System.out.println("### Preloading is finished");

        IgniteEx client1 = (IgniteEx)startGrid("client1");
        IgniteEx client2 = (IgniteEx)startGrid("client2");

        ConcurrentMap<Long, Long> threadProgressTracker = new ConcurrentHashMap<>();

        startSimpleLoadThread(client1, cacheName, stopLoad, loadError, threadProgressTracker);
        startSimpleLoadThread(client1, cacheName, stopLoad, loadError, threadProgressTracker);
        startSimpleLoadThread(client1, cacheName, stopLoad, loadError, threadProgressTracker);
        startTxLoadThread(client2, cacheName, stopLoad, loadError, threadProgressTracker);
        startTxLoadThread(client2, cacheName, stopLoad, loadError, threadProgressTracker);
        startTxLoadThread(client2, cacheName, stopLoad, loadError, threadProgressTracker);

        awaitProgressInAllLoaders(10_000, loadError, threadProgressTracker);

        List<BaselineNode> fullBlt = new ArrayList<>();
        for (int i = 0; i < NODES_COUNT; i++)
            fullBlt.add(grid(i).localNode());

        stopGrid(NODES_COUNT - 1, true);
        stopGrid(NODES_COUNT - 2, true);

        awaitProgressInAllLoaders(10_000, loadError, threadProgressTracker);

        tryChangeBaselineDown(ig0, fullBlt, 5, loadError, threadProgressTracker);
        tryChangeBaselineDown(ig0, fullBlt, 4, loadError, threadProgressTracker);

        stopLoad.set(true);
    }

    /**
     * Test that changing baseline down
     */
    public void testCrossCacheTxs() throws Exception {
        IgniteEx ig0 = (IgniteEx)startGrids(NODES_COUNT);

        ig0.cluster().active(true);

        AtomicBoolean stopLoad = new AtomicBoolean(false);

        AtomicReference<Throwable> loadError = new AtomicReference<>(null);

        String cacheName1 = PARTITIONED_TX_CACHE_NAME;
        String cacheName2 = PARTITIONED_TX_PRIM_SYNC_CACHE_NAME;

        IgniteCache<Integer, String> cache1 = ig0.cache(PARTITIONED_TX_CACHE_NAME);
        IgniteCache<Integer, String> cache2 = ig0.cache(PARTITIONED_TX_PRIM_SYNC_CACHE_NAME);

        System.out.println("### Starting preloading");

        for (int i = 0; i < ENTRIES; i++) {
            ThreadLocalRandom r = ThreadLocalRandom.current();

            byte[] randBytes1 = new byte[r.nextInt(10, 100)];
            byte[] randBytes2 = new byte[r.nextInt(10, 100)];

            cache1.put(r.nextInt(ENTRIES), new String(randBytes1));
            cache2.put(r.nextInt(ENTRIES), new String(randBytes2));
        }

        System.out.println("### Preloading is finished");

        IgniteEx client1 = (IgniteEx)startGrid("client1");
        IgniteEx client2 = (IgniteEx)startGrid("client2");

        ConcurrentMap<Long, Long> threadProgressTracker = new ConcurrentHashMap<>();

        startCrossCacheTxLoadThread(client1, cacheName1, cacheName2, stopLoad, loadError, threadProgressTracker);
        startCrossCacheTxLoadThread(client1, cacheName1, cacheName2, stopLoad, loadError, threadProgressTracker);
        startCrossCacheTxLoadThread(client1, cacheName2, cacheName1, stopLoad, loadError, threadProgressTracker);
        startCrossCacheTxLoadThread(client2, cacheName1, cacheName2, stopLoad, loadError, threadProgressTracker);
        startCrossCacheTxLoadThread(client2, cacheName1, cacheName2, stopLoad, loadError, threadProgressTracker);
        startCrossCacheTxLoadThread(client2, cacheName2, cacheName1, stopLoad, loadError, threadProgressTracker);

        awaitProgressInAllLoaders(10_000, loadError, threadProgressTracker);

        List<BaselineNode> fullBlt = new ArrayList<>();
        for (int i = 0; i < NODES_COUNT; i++)
            fullBlt.add(grid(i).localNode());

        stopGrid(NODES_COUNT - 1, true);
        stopGrid(NODES_COUNT - 2, true);

        awaitProgressInAllLoaders(10_000, loadError, threadProgressTracker);

        tryChangeBaselineDown(ig0, fullBlt, 5, loadError, threadProgressTracker);
        tryChangeBaselineDown(ig0, fullBlt, 4, loadError, threadProgressTracker);

        stopLoad.set(true);

    }

    /**
     * @param ig0 Ignite.
     * @param fullBlt Initial BLT list.
     * @param newBaselineSize New baseline size.
     * @param threadProgressTracker Thread progress tracker.
     */
    private void tryChangeBaselineDown(
        IgniteEx ig0,
        List<BaselineNode> fullBlt,
        int newBaselineSize,
        AtomicReference<Throwable> loadError,
        ConcurrentMap<Long, Long> threadProgressTracker
    ) throws Exception {
        System.out.println("### Changing BLT: " + (newBaselineSize + 1) + " -> " + newBaselineSize);
        ig0.cluster().setBaselineTopology(fullBlt.subList(0, newBaselineSize));

        System.out.println("### Starting rebalancing after BLT change: " + (newBaselineSize + 1) + " -> " + newBaselineSize);
        waitForRebalancing();
        System.out.println("### Rebalancing is finished after BLT change: " + (newBaselineSize + 1) + " -> " + newBaselineSize);

        awaitProgressInAllLoaders(10_000, loadError, threadProgressTracker);

        if (loadError.get() != null) {
            loadError.get().printStackTrace();

            fail("Unexpected error in load thread: " + loadError.get().toString());
        }
    }

    /**
     * @param ig Ignite instance.
     * @param cacheName Cache name.
     * @param stopFlag Stop flag.
     * @param loadError Load error reference.
     * @param threadProgressTracker Progress tracker.
     */
    private void startSimpleLoadThread(
        IgniteEx ig,
        String cacheName,
        AtomicBoolean stopFlag,
        AtomicReference<Throwable> loadError,
        ConcurrentMap<Long, Long> threadProgressTracker
    ) {
        GridTestUtils.runAsync(new Runnable() {
            @Override public void run() {
                ThreadLocalRandom r = ThreadLocalRandom.current();

                IgniteCache<Integer, String> cache = ig.cache(cacheName);

                try {
                    while (!stopFlag.get()) {
                        try {
                            int op = r.nextInt(3);

                            switch (op) {
                                case 0:
                                    byte[] randBytes = new byte[r.nextInt(10, 100)];

                                    cache.put(r.nextInt(ENTRIES), new String(randBytes));

                                    break;
                                case 1:
                                    cache.remove(r.nextInt(ENTRIES));

                                    break;
                                case 2:
                                    cache.get(r.nextInt(ENTRIES));

                                    break;
                            }

                            threadProgressTracker.compute(Thread.currentThread().getId(),
                                (tId, ops) -> ops == null ? 1 : ops + 1);
                        }
                        catch (CacheException e) {
                            if (e.getCause() instanceof ClusterTopologyException)
                                ((ClusterTopologyException)e.getCause()).retryReadyFuture().get();
                        }
                        catch (ClusterTopologyException e) {
                            e.retryReadyFuture().get();
                        }
                    }
                }
                catch (Throwable t) {
                    loadError.compareAndSet(null, t);

                    stopFlag.set(true);
                }
            }
        });
    }

    /**
     * @param ig Ignite instance.
     * @param cacheName Cache name.
     * @param stopFlag Stop flag.
     * @param loadError Load error reference.
     * @param threadProgressTracker Progress tracker.
     */
    private void startTxLoadThread(
        IgniteEx ig,
        String cacheName,
        AtomicBoolean stopFlag,
        AtomicReference<Throwable> loadError,
        ConcurrentMap<Long, Long> threadProgressTracker
    ) {
        GridTestUtils.runAsync(new Runnable() {
            @Override public void run() {
                ThreadLocalRandom r = ThreadLocalRandom.current();

                IgniteCache<Integer, String> cache = ig.cache(cacheName);

                boolean pessimistic = r.nextBoolean();

                boolean rollback = r.nextBoolean();

                try {
                    while (!stopFlag.get()) {
                        try (Transaction tx = ig.transactions().txStart(
                            pessimistic ? TransactionConcurrency.PESSIMISTIC : TransactionConcurrency.OPTIMISTIC,
                            TransactionIsolation.REPEATABLE_READ
                        )) {
                            int key1 = -1;
                            String val1 = null;
                            while (val1 == null) {
                                key1 = r.nextInt(ENTRIES);
                                val1 = cache.get(key1);
                            }

                            int key2 = -1;
                            String val2 = null;
                            while (val2 == null) {
                                key2 = r.nextInt(ENTRIES);
                                val2 = cache.get(key2);
                            }

                            cache.put(key1, val2);
                            cache.put(key2, val1);

                            if (rollback)
                                tx.rollback();
                            else
                                tx.commit();

                            threadProgressTracker.compute(Thread.currentThread().getId(),
                                (tId, ops) -> ops == null ? 1 : ops + 1);
                        }
                        catch (CacheException e) {
                            if (e.getCause() instanceof ClusterTopologyException)
                                ((ClusterTopologyException)e.getCause()).retryReadyFuture().get();
                        }
                        catch (ClusterTopologyException e) {
                            e.retryReadyFuture().get();
                        }
                    }
                }
                catch (Throwable t) {
                    loadError.compareAndSet(null, t);

                    stopFlag.set(true);
                }
            }
        });
    }

    /**
     * @param ig Ignite instance.
     * @param cacheName1 Cache name 1.
     * @param cacheName2 Cache name 2.
     * @param stopFlag Stop flag.
     * @param loadError Load error reference.
     * @param threadProgressTracker Progress tracker.
     */
    private void startCrossCacheTxLoadThread(
        IgniteEx ig,
        String cacheName1,
        String cacheName2,
        AtomicBoolean stopFlag,
        AtomicReference<Throwable> loadError,
        ConcurrentMap<Long, Long> threadProgressTracker
    ) {
        GridTestUtils.runAsync(new Runnable() {
            @Override public void run() {
                ThreadLocalRandom r = ThreadLocalRandom.current();

                IgniteCache<Integer, String> cache1 = ig.cache(cacheName1);
                IgniteCache<Integer, String> cache2 = ig.cache(cacheName2);

                boolean pessimistic = r.nextBoolean();

                boolean rollback = r.nextBoolean();

                try {
                    while (!stopFlag.get()) {
                        try (Transaction tx = ig.transactions().txStart(
                            pessimistic ? TransactionConcurrency.PESSIMISTIC : TransactionConcurrency.OPTIMISTIC,
                            TransactionIsolation.REPEATABLE_READ
                        )) {
                            int key1 = -1;
                            String val1 = null;
                            while (val1 == null) {
                                key1 = r.nextInt(ENTRIES);
                                val1 = cache1.get(key1);
                            }

                            int key2 = -1;
                            String val2 = null;
                            while (val2 == null) {
                                key2 = r.nextInt(ENTRIES);
                                val2 = cache2.get(key2);
                            }

                            cache1.put(key1, val2);
                            cache2.put(key2, val1);

                            if (rollback)
                                tx.rollback();
                            else
                                tx.commit();

                            threadProgressTracker.compute(Thread.currentThread().getId(),
                                (tId, ops) -> ops == null ? 1 : ops + 1);
                        }
                        catch (CacheException e) {
                            if (e.getCause() instanceof ClusterTopologyException)
                                ((ClusterTopologyException)e.getCause()).retryReadyFuture().get();
                        }
                        catch (ClusterTopologyException e) {
                            e.retryReadyFuture().get();
                        }
                    }
                }
                catch (Throwable t) {
                    loadError.compareAndSet(null, t);

                    stopFlag.set(true);
                }
            }
        });
    }

    /**
     * @param waitMs Wait milliseconds.
     * @param loadError Load error.
     * @param threadProgressTracker Thread progress tracker.
     */
    private void awaitProgressInAllLoaders(
        long waitMs,
        AtomicReference<Throwable> loadError,
        ConcurrentMap<Long, Long> threadProgressTracker
    ) throws Exception {
        Map<Long, Long> view1 = new HashMap<>(threadProgressTracker);

        long startTs = U.currentTimeMillis();

        while (U.currentTimeMillis() < startTs + waitMs) {
            Map<Long, Long> view2 = new HashMap<>(threadProgressTracker);

            if (loadError.get() != null) {
                loadError.get().printStackTrace();

                fail("Unexpected error in load thread: " + loadError.get().toString());
            }

            boolean frozenThreadExists = false;

            for (Map.Entry<Long, Long> entry : view1.entrySet()) {
                if (entry.getValue().equals(view2.get(entry.getKey())))
                    frozenThreadExists = true;
            }

            if (!frozenThreadExists)
                return;

            U.sleep(100);
        }

        fail("No progress in load thread");
    }
}
