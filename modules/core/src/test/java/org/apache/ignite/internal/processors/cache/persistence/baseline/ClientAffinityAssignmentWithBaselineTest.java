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

import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import javax.cache.CacheException;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.BaselineNode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.cluster.ClusterTopologyException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteNodeAttributes;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.apache.ignite.transactions.TransactionRollbackException;
import org.junit.Ignore;
import org.junit.Test;

import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.DFLT_STORE_DIR;

/**
 * Checks that client affinity assignment cache is calculated correctly regardless of current baseline topology.
 */
public class ClientAffinityAssignmentWithBaselineTest extends GridCommonAbstractTest {
    /** Nodes count. */
    private static final int DEFAULT_NODES_COUNT = 5;

    /** Tx cache name. */
    private static final String PARTITIONED_TX_CACHE_NAME = "p-tx-cache";

    /** Tx cache name with shifted affinity. */
    private static final String PARTITIONED_TX_PRIM_SYNC_CACHE_NAME = "prim-sync";

    /** Tx cache name from client static configuration. */
    private static final String PARTITIONED_TX_CLIENT_CACHE_NAME = "p-tx-client-cache";

    /** Atomic cache name. */
    private static final String PARTITIONED_ATOMIC_CACHE_NAME = "p-atomic-cache";

    /** Tx cache name. */
    private static final String REPLICATED_TX_CACHE_NAME = "r-tx-cache";

    /** Atomic cache name. */
    private static final String REPLICATED_ATOMIC_CACHE_NAME = "r-atomic-cache";

    /** Client grid name. */
    private static final String CLIENT_GRID_NAME = "client";

    /** Flaky node name */
    private static final String FLAKY_NODE_NAME = "flaky";

    /** Entries. */
    private static final int ENTRIES = 3_000;

    /** Flaky node wal path. */
    public static final String FLAKY_WAL_PATH = "flakywal";

    /** Flaky node wal archive path. */
    public static final String FLAKY_WAL_ARCHIVE_PATH = "flakywalarchive";

    /** Flaky node storage path. */
    public static final String FLAKY_STORAGE_PATH = "flakystorage";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        if (!igniteInstanceName.startsWith(CLIENT_GRID_NAME)) {
            cfg.setDataStorageConfiguration(
                new DataStorageConfiguration()
                    .setWalSegmentSize(4 * 1024 * 1024)
                    .setDefaultDataRegionConfiguration(
                        new DataRegionConfiguration()
                            .setPersistenceEnabled(true)
                            .setMaxSize(200 * 1024 * 1024)
                    )
            );
        }

        if (igniteInstanceName.contains(FLAKY_NODE_NAME)) {
            File store = U.resolveWorkDirectory(U.defaultWorkDirectory(), DFLT_STORE_DIR, false);

            cfg.getDataStorageConfiguration().setWalPath(new File(store, FLAKY_WAL_PATH).getAbsolutePath());
            cfg.getDataStorageConfiguration().setWalArchivePath(new File(store, FLAKY_WAL_ARCHIVE_PATH).getAbsolutePath());
            cfg.getDataStorageConfiguration().setStoragePath(new File(store, FLAKY_STORAGE_PATH).getAbsolutePath());
        }

        cfg.setConsistentId(igniteInstanceName);

        List<CacheConfiguration> srvConfigs = new ArrayList<>();
        srvConfigs.add(cacheConfig(PARTITIONED_TX_CACHE_NAME));
        srvConfigs.add(cacheConfig(PARTITIONED_TX_PRIM_SYNC_CACHE_NAME));
        srvConfigs.add(cacheConfig(REPLICATED_ATOMIC_CACHE_NAME));

        List<CacheConfiguration> clientConfigs = new ArrayList<>(srvConfigs);

        // Skip some configs in client static configuration to check that clients receive correct cache descriptors.
        srvConfigs.add(cacheConfig(PARTITIONED_ATOMIC_CACHE_NAME));
        srvConfigs.add(cacheConfig(REPLICATED_TX_CACHE_NAME));

        // Skip config in server static configuration to check that caches received on client join start correctly.
        clientConfigs.add(cacheConfig(PARTITIONED_TX_CLIENT_CACHE_NAME));

        if (igniteInstanceName.startsWith(CLIENT_GRID_NAME))
            cfg.setCacheConfiguration(clientConfigs.toArray(new CacheConfiguration[clientConfigs.size()]));
        else
            cfg.setCacheConfiguration(srvConfigs.toArray(new CacheConfiguration[srvConfigs.size()]));

        // Enforce different mac adresses to emulate distributed environment by default.
        cfg.setUserAttributes(Collections.singletonMap(
            IgniteNodeAttributes.ATTR_MACS_OVERRIDE, UUID.randomUUID().toString()));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     * @param cacheName Cache name.
     */
    private CacheConfiguration<Integer, String> cacheConfig(String cacheName) {
        CacheConfiguration<Integer, String> cfg = new CacheConfiguration<>();

        if (PARTITIONED_ATOMIC_CACHE_NAME.equals(cacheName)) {
            cfg.setName(PARTITIONED_ATOMIC_CACHE_NAME);
            cfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);
            cfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
            cfg.setAffinity(new RendezvousAffinityFunction(false, 32));
            cfg.setBackups(2);
        }
        else if (PARTITIONED_TX_CACHE_NAME.equals(cacheName)) {
            cfg.setName(PARTITIONED_TX_CACHE_NAME);
            cfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
            cfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
            cfg.setAffinity(new RendezvousAffinityFunction(false, 32));
            cfg.setBackups(2);
        }
        else if (PARTITIONED_TX_CLIENT_CACHE_NAME.equals(cacheName)) {
            cfg.setName(PARTITIONED_TX_CLIENT_CACHE_NAME);
            cfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
            cfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
            cfg.setAffinity(new RendezvousAffinityFunction(false, 32));
            cfg.setBackups(2);
        }
        else if (PARTITIONED_TX_PRIM_SYNC_CACHE_NAME.equals(cacheName)) {
            cfg.setName(PARTITIONED_TX_PRIM_SYNC_CACHE_NAME);
            cfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
            cfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.PRIMARY_SYNC);
            cfg.setAffinity(new RendezvousAffinityFunction(false, 41)); // To break collocation.
            cfg.setBackups(2);
        }
        else if (REPLICATED_ATOMIC_CACHE_NAME.equals(cacheName)) {
            cfg.setName(REPLICATED_ATOMIC_CACHE_NAME);
            cfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);
            cfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
            cfg.setAffinity(new RendezvousAffinityFunction(false, 32));
            cfg.setCacheMode(CacheMode.REPLICATED);
        }
        else if (REPLICATED_TX_CACHE_NAME.equals(cacheName)) {
            cfg.setName(REPLICATED_TX_CACHE_NAME);
            cfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
            cfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
            cfg.setAffinity(new RendezvousAffinityFunction(false, 32));
            cfg.setCacheMode(CacheMode.REPLICATED);
        }
        else
            throw new IllegalArgumentException("Unexpected cache name");

        return cfg;
    }

    /**
     *
     */
    @Test
    public void testPartitionedAtomicCache() throws Exception {
        testChangingBaselineDown(PARTITIONED_ATOMIC_CACHE_NAME, false);
    }

    /**
     *
     */
    @Test
    public void testPartitionedTxCache() throws Exception {
        testChangingBaselineDown(PARTITIONED_TX_CACHE_NAME, false);
    }

    /**
     * Test that activation after client join won't break cache.
     */
    @Test
    public void testLateActivation() throws Exception {
        testChangingBaselineDown(PARTITIONED_TX_CACHE_NAME, true);
    }

    /**
     *
     */
    @Test
    public void testReplicatedAtomicCache() throws Exception {
        testChangingBaselineDown(REPLICATED_ATOMIC_CACHE_NAME, false);
    }

    /**
     *
     */
    @Test
    public void testReplicatedTxCache() throws Exception {
        testChangingBaselineDown(REPLICATED_TX_CACHE_NAME, false);
    }

    /**
     * Tests that changing baseline down under load won't break cache.
     */
    private void testChangingBaselineDown(String cacheName, boolean lateActivation) throws Exception {
        IgniteEx ig0 = (IgniteEx)startGrids(DEFAULT_NODES_COUNT);

        ig0.cluster().baselineAutoAdjustEnabled(false);
        IgniteEx client1 = null;
        IgniteEx client2 = null;

        if (lateActivation) {
            client1 = startClientGrid("client1");
            client2 = startClientGrid("client2");
        }
        else
            ig0.cluster().active(true);

        AtomicBoolean stopLoad = new AtomicBoolean(false);

        AtomicReference<Throwable> loadError = new AtomicReference<>(null);

        if (lateActivation)
            ig0.cluster().active(true);

        IgniteCache<Integer, String> cache = ig0.cache(cacheName);

        System.out.println("### Starting preloading");

        for (int i = 0; i < ENTRIES; i++) {
            ThreadLocalRandom r = ThreadLocalRandom.current();

            byte[] randBytes = new byte[r.nextInt(10, 100)];

            cache.put(r.nextInt(ENTRIES), new String(randBytes));
        }

        System.out.println("### Preloading is finished");

        if (!lateActivation) {
            client1 = startClientGrid("client1");
            client2 = startClientGrid("client2");
        }

        ConcurrentMap<Long, Long> threadProgressTracker = new ConcurrentHashMap<>();

        startSimpleLoadThread(client1, cacheName, stopLoad, loadError, threadProgressTracker);
        startSimpleLoadThread(client1, cacheName, stopLoad, loadError, threadProgressTracker);
        startSimpleLoadThread(client1, cacheName, stopLoad, loadError, threadProgressTracker);
        startTxLoadThread(client2, cacheName, stopLoad, loadError, threadProgressTracker);
        startTxLoadThread(client2, cacheName, stopLoad, loadError, threadProgressTracker);
        startTxLoadThread(client2, cacheName, stopLoad, loadError, threadProgressTracker);

        awaitProgressInAllLoaders(10_000, loadError, threadProgressTracker);

        List<BaselineNode> fullBlt = new ArrayList<>();
        for (int i = 0; i < DEFAULT_NODES_COUNT; i++)
            fullBlt.add(grid(i).localNode());

        stopGrid(DEFAULT_NODES_COUNT - 1, true);
        stopGrid(DEFAULT_NODES_COUNT - 2, true);

        awaitProgressInAllLoaders(10_000, loadError, threadProgressTracker);

        tryChangeBaselineDown(ig0, fullBlt, DEFAULT_NODES_COUNT - 1, loadError, threadProgressTracker);
        tryChangeBaselineDown(ig0, fullBlt, DEFAULT_NODES_COUNT - 2, loadError, threadProgressTracker);

        stopLoad.set(true);
    }

    /**
     * Tests that rejoin of baseline node with clear LFS under load won't break cache.
     */
    @Test
    public void testRejoinWithCleanLfs() throws Exception {
        IgniteEx ig0 = (IgniteEx)startGrids(DEFAULT_NODES_COUNT - 1);
        startGrid("flaky");

        ig0.cluster().active(true);

        AtomicBoolean stopLoad = new AtomicBoolean(false);

        AtomicReference<Throwable> loadError = new AtomicReference<>(null);

        IgniteCache<Integer, String> cache1 = ig0.cache(PARTITIONED_ATOMIC_CACHE_NAME);
        IgniteCache<Integer, String> cache2 = ig0.cache(PARTITIONED_TX_CACHE_NAME);
        IgniteCache<Integer, String> cache3 = ig0.cache(REPLICATED_ATOMIC_CACHE_NAME);
        IgniteCache<Integer, String> cache4 = ig0.cache(REPLICATED_TX_CACHE_NAME);

        System.out.println("### Starting preloading");

        for (int i = 0; i < ENTRIES; i++) {
            ThreadLocalRandom r = ThreadLocalRandom.current();

            cache1.put(r.nextInt(ENTRIES), new String(new byte[r.nextInt(10, 100)]));
            cache2.put(r.nextInt(ENTRIES), new String(new byte[r.nextInt(10, 100)]));
            cache3.put(r.nextInt(ENTRIES), new String(new byte[r.nextInt(10, 100)]));
            cache4.put(r.nextInt(ENTRIES), new String(new byte[r.nextInt(10, 100)]));
        }

        System.out.println("### Preloading is finished");

        IgniteEx client1 = startClientGrid("client1");
        IgniteEx client2 = startClientGrid("client2");

        ConcurrentMap<Long, Long> threadProgressTracker = new ConcurrentHashMap<>();

        startSimpleLoadThread(client1, PARTITIONED_ATOMIC_CACHE_NAME, stopLoad, loadError, threadProgressTracker);
        startSimpleLoadThread(client1, PARTITIONED_TX_CACHE_NAME, stopLoad, loadError, threadProgressTracker);
        startSimpleLoadThread(client1, REPLICATED_ATOMIC_CACHE_NAME, stopLoad, loadError, threadProgressTracker);
        startTxLoadThread(client2, PARTITIONED_ATOMIC_CACHE_NAME, stopLoad, loadError, threadProgressTracker);
        startTxLoadThread(client2, PARTITIONED_TX_CACHE_NAME, stopLoad, loadError, threadProgressTracker);
        startTxLoadThread(client2, REPLICATED_TX_CACHE_NAME, stopLoad, loadError, threadProgressTracker);

        awaitProgressInAllLoaders(10_000, loadError, threadProgressTracker);

        stopGrid("flaky");

        awaitProgressInAllLoaders(10_000, loadError, threadProgressTracker);

        File store = U.resolveWorkDirectory(U.defaultWorkDirectory(), DFLT_STORE_DIR, false);

        U.delete(new File(store, FLAKY_WAL_PATH));
        U.delete(new File(store, FLAKY_WAL_ARCHIVE_PATH));
        U.delete(new File(store, FLAKY_STORAGE_PATH));

        startGrid("flaky");

        System.out.println("### Starting rebalancing after flaky node join");
        awaitPartitionMapExchange();
        System.out.println("### Rebalancing is finished after flaky node join");

        awaitProgressInAllLoaders(10_000, loadError, threadProgressTracker);

        stopLoad.set(true);
    }

    /**
     * Test that changing baseline down under cross-cache txs load won't break cache.
     */
    @Test
    public void testCrossCacheTxs() throws Exception {
        IgniteEx ig0 = (IgniteEx)startGrids(DEFAULT_NODES_COUNT);

        ig0.cluster().baselineAutoAdjustEnabled(false);
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

        IgniteEx client1 = startClientGrid("client1");
        IgniteEx client2 = startClientGrid("client2");

        ConcurrentMap<Long, Long> threadProgressTracker = new ConcurrentHashMap<>();

        startCrossCacheTxLoadThread(client1, cacheName1, cacheName2, stopLoad, loadError, threadProgressTracker);
        startCrossCacheTxLoadThread(client1, cacheName1, cacheName2, stopLoad, loadError, threadProgressTracker);
        startCrossCacheTxLoadThread(client1, cacheName2, cacheName1, stopLoad, loadError, threadProgressTracker);
        startCrossCacheTxLoadThread(client2, cacheName1, cacheName2, stopLoad, loadError, threadProgressTracker);
        startCrossCacheTxLoadThread(client2, cacheName1, cacheName2, stopLoad, loadError, threadProgressTracker);
        startCrossCacheTxLoadThread(client2, cacheName2, cacheName1, stopLoad, loadError, threadProgressTracker);

        awaitProgressInAllLoaders(10_000, loadError, threadProgressTracker);

        List<BaselineNode> fullBlt = new ArrayList<>();
        for (int i = 0; i < DEFAULT_NODES_COUNT; i++)
            fullBlt.add(grid(i).localNode());

        stopGrid(DEFAULT_NODES_COUNT - 1, true);
        stopGrid(DEFAULT_NODES_COUNT - 2, true);

        awaitProgressInAllLoaders(10_000, loadError, threadProgressTracker);

        tryChangeBaselineDown(ig0, fullBlt, DEFAULT_NODES_COUNT - 1, loadError, threadProgressTracker);
        tryChangeBaselineDown(ig0, fullBlt, DEFAULT_NODES_COUNT - 2, loadError, threadProgressTracker);

        stopLoad.set(true);
    }

    /**
     * Tests that join of non-baseline node while long transactions are running won't break dynamically started cache.
     */
    @Test
    public void testDynamicCacheLongTransactionNodeStart() throws Exception {
        IgniteEx ig0 = (IgniteEx)startGrids(4);

        ig0.cluster().active(true);

        IgniteEx client = startClientGrid("client");

        CacheConfiguration<Integer, String> dynamicCacheCfg = cacheConfig(REPLICATED_TX_CACHE_NAME);
        dynamicCacheCfg.setName("dyn");

        IgniteCache<Integer, String> dynamicCache = client.getOrCreateCache(dynamicCacheCfg);

        for (int i = 0; i < ENTRIES; i++)
            dynamicCache.put(i, "abacaba" + i);

        AtomicBoolean releaseTx = new AtomicBoolean(false);
        CountDownLatch allTxsDoneLatch = new CountDownLatch(10);

        for (int i = 0; i < 10; i++) {
            final int i0 = i;

            GridTestUtils.runAsync(new Runnable() {
                @Override public void run() {
                    try (Transaction tx = client.transactions().txStart(TransactionConcurrency.PESSIMISTIC,
                        TransactionIsolation.REPEATABLE_READ)) {
                        dynamicCache.put(i0, "txtxtxtx" + i0);

                        while (!releaseTx.get())
                            LockSupport.parkNanos(1_000_000);

                        tx.commit();

                        System.out.println("Tx #" + i0 + " committed");
                    }
                    catch (Throwable t) {
                        System.out.println("Tx #" + i0 + " failed");

                        t.printStackTrace();
                    }
                    finally {
                        allTxsDoneLatch.countDown();
                    }
                }
            });
        }

        GridTestUtils.runAsync(new Runnable() {
            @Override public void run() {
                try {
                    startGrid(4);
                }
                catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });

        U.sleep(1_000);

        releaseTx.set(true);

        allTxsDoneLatch.await();

        for (int i = 0; i < 10_000; i++)
            assertEquals("txtxtxtx" + (i % 10), dynamicCache.get(i % 10));
    }

    /**
     * Tests that if dynamic cache has no affinity nodes at the moment of start,
     * it will still work correctly when affinity nodes will appear.
     */
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-8652")
    @Test
    public void testDynamicCacheStartNoAffinityNodes() throws Exception {
        IgniteEx ig0 = startGrid(0);

        ig0.cluster().active(true);

        IgniteEx client = startClientGrid("client");

        CacheConfiguration<Integer, String> dynamicCacheCfg = new CacheConfiguration<Integer, String>()
            .setName("dyn")
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
            .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
            .setAffinity(new RendezvousAffinityFunction(false, 32))
            .setBackups(2)
            .setNodeFilter(new ConsistentIdNodeFilter((Serializable)ig0.localNode().consistentId()));

        IgniteCache<Integer, String> dynamicCache = client.getOrCreateCache(dynamicCacheCfg);

        for (int i = 1; i < 4; i++)
            startGrid(i);

        resetBaselineTopology();

        for (int i = 0; i < ENTRIES; i++)
            dynamicCache.put(i, "abacaba" + i);

        AtomicBoolean releaseTx = new AtomicBoolean(false);
        CountDownLatch allTxsDoneLatch = new CountDownLatch(10);

        for (int i = 0; i < 10; i++) {
            final int i0 = i;

            GridTestUtils.runAsync(new Runnable() {
                @Override public void run() {
                    try (Transaction tx = client.transactions().txStart(TransactionConcurrency.PESSIMISTIC,
                        TransactionIsolation.REPEATABLE_READ)) {
                        dynamicCache.put(i0, "txtxtxtx" + i0);

                        while (!releaseTx.get())
                            LockSupport.parkNanos(1_000_000);

                        tx.commit();

                        System.out.println("Tx #" + i0 + " committed");
                    }
                    catch (Throwable t) {
                        System.out.println("Tx #" + i0 + " failed");

                        t.printStackTrace();
                    }
                    finally {
                        allTxsDoneLatch.countDown();
                    }
                }
            });
        }

        GridTestUtils.runAsync(new Runnable() {
            @Override public void run() {
                try {
                    startGrid(4);
                }
                catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });

        U.sleep(1_000);

        releaseTx.set(true);

        allTxsDoneLatch.await();

        for (int i = 0; i < 10_000; i++)
            assertEquals("txtxtxtx" + (i % 10), dynamicCache.get(i % 10));
    }

    /**
     * Tests that join of non-baseline node while long transactions are running won't break cache started on client join.
     */
    @Test
    public void testClientJoinCacheLongTransactionNodeStart() throws Exception {
        IgniteEx ig0 = (IgniteEx)startGrids(4);

        ig0.cluster().active(true);

        IgniteEx client = startClientGrid("client");

        IgniteCache<Integer, String> clientJoinCache = client.cache(PARTITIONED_TX_CLIENT_CACHE_NAME);

        for (int i = 0; i < ENTRIES; i++)
            clientJoinCache.put(i, "abacaba" + i);

        AtomicBoolean releaseTx = new AtomicBoolean(false);
        CountDownLatch allTxsDoneLatch = new CountDownLatch(10);

        for (int i = 0; i < 10; i++) {
            final int i0 = i;

            GridTestUtils.runAsync(new Runnable() {
                @Override public void run() {
                    try (Transaction tx = client.transactions().txStart(TransactionConcurrency.PESSIMISTIC,
                        TransactionIsolation.REPEATABLE_READ)) {
                        clientJoinCache.put(i0, "txtxtxtx" + i0);

                        while (!releaseTx.get())
                            LockSupport.parkNanos(1_000_000);

                        tx.commit();

                        System.out.println("Tx #" + i0 + " committed");
                    }
                    catch (Throwable t) {
                        System.out.println("Tx #" + i0 + " failed");

                        t.printStackTrace();
                    }
                    finally {
                        allTxsDoneLatch.countDown();
                    }
                }
            });
        }

        GridTestUtils.runAsync(new Runnable() {
            @Override public void run() {
                try {
                    startGrid(4);
                }
                catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });

        U.sleep(1_000);

        releaseTx.set(true);

        allTxsDoneLatch.await();

        for (int i = 0; i < 10_000; i++)
            assertEquals("txtxtxtx" + (i % 10), clientJoinCache.get(i % 10));
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
        awaitPartitionMapExchange();
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

                IgniteCache<Integer, String> cache = ig.cache(cacheName).withAllowAtomicOpsInTx();

                boolean pessimistic = atomicityMode(cache) == CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT || r.nextBoolean();

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
                            if (e.getCause() instanceof ClusterTopologyException) {
                                IgniteFuture retryFut = ((ClusterTopologyException)e.getCause()).retryReadyFuture();

                                if (retryFut != null)
                                    retryFut.get();
                            }
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

                boolean pessimistic = atomicityMode(cache1) == CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT ||
                    atomicityMode(cache2) == CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT || r.nextBoolean();

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
                        catch (CacheException | ClusterTopologyException | TransactionRollbackException e) {
                            awaitTopology(e);
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
     * Extract cause of type {@link ClusterTopologyException} (if exists) and awaits retry topology.
     *
     * @param e Original exception.
     */
    private void awaitTopology(Throwable e) throws IgniteCheckedException {
        if (e instanceof TransactionRollbackException) {
            TransactionRollbackException e0 = (TransactionRollbackException) e;

            ClusterTopologyCheckedException e00 = X.cause(e0, ClusterTopologyCheckedException.class);
            IgniteInternalFuture f;

            if (e00 != null && (f = e00.retryReadyFuture()) != null)
                f.get();
        }

        ClusterTopologyException ex = X.cause(e, ClusterTopologyException.class);
        IgniteFuture f;

        // For now in MVCC case the topology exception doesn't have a remap future.
        if (ex != null && (f = ex.retryReadyFuture()) != null)
            f.get();
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

            Throwable t;

            if ((t = loadError.get()) != null)
                fail("Unexpected error in load thread: " + X.getFullStackTrace(t));

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

    /**
     * Accepts all nodes except one with specified consistent ID.
     */
    private static class ConsistentIdNodeFilter implements IgnitePredicate<ClusterNode> {
        /** Consistent ID. */
        private final Serializable consId0;

        /**
         * @param consId0 Consistent ID.
         */
        public ConsistentIdNodeFilter(Serializable consId0) {
            this.consId0 = consId0;
        }

        /** {@inheritDoc} */
        @Override public boolean apply(ClusterNode node) {
            return !node.consistentId().equals(consId0);
        }
    }
}
