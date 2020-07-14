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

package org.apache.ignite.internal.processors.cache;

import java.lang.management.ManagementFactory;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import javax.cache.CacheManager;
import javax.cache.Caching;
import javax.management.MBeanServer;
import javax.management.MBeanServerInvocationHandler;
import javax.management.ObjectName;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMetrics;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.TransactionsMXBeanImpl;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxFinishResponse;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxPrepareResponse;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxManager;
import org.apache.ignite.internal.util.future.GridCompoundFuture;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.mxbean.CacheMetricsMXBean;
import org.apache.ignite.mxbean.TransactionsMXBean;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.MvccFeatureChecker;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.junit.Assume;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_DUMP_TX_COLLISIONS_INTERVAL;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.READ_COMMITTED;

/**
 *
 */
@SuppressWarnings("unchecked")
public class CacheMetricsManageTest extends GridCommonAbstractTest {
    /** */
    private static final String CACHE1 = "cache1";

    /** */
    private static final String CACHE2 = "cache2";

    /** */
    private static final String GROUP = "group1";

    /** Wait condition timeout. */
    private static final long WAIT_CONDITION_TIMEOUT = 3_000L;

    /** Persistence. */
    private boolean persistence;

    /** Use test spi flag.  */
    private boolean useTestCommSpi;

    /** Backups count. */
    private int backups = -1;

    /** Client flag. */
    private boolean client;

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testJmxNoPdsStatisticsEnable() throws Exception {
        Assume.assumeFalse("https://issues.apache.org/jira/browse/IGNITE-9224", MvccFeatureChecker.forcedMvcc());

        testJmxStatisticsEnable(false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testJmxPdsStatisticsEnable() throws Exception {
        Assume.assumeFalse("https://issues.apache.org/jira/browse/IGNITE-9224", MvccFeatureChecker.forcedMvcc());

        testJmxStatisticsEnable(true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCacheManagerStatisticsEnable() throws Exception {
        final CacheManager mgr1 = Caching.getCachingProvider().getCacheManager();
        final CacheManager mgr2 = Caching.getCachingProvider().getCacheManager();

        CacheConfiguration<Object, Object> cfg1 = new CacheConfiguration<>()
            .setName(CACHE1)
            .setGroupName(GROUP)
            .setCacheMode(CacheMode.PARTITIONED)
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);

        mgr1.createCache(CACHE1, cfg1);

        CacheConfiguration<Object, Object> cfg2 = new CacheConfiguration<>(cfg1)
            .setName(CACHE2)
            .setStatisticsEnabled(true);

        mgr1.createCache(CACHE2, cfg2);

        assertTrue(GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                return !isStatisticsEnabled(mgr1, CACHE1) && !isStatisticsEnabled(mgr2, CACHE1)
                    && isStatisticsEnabled(mgr1, CACHE2) && isStatisticsEnabled(mgr2, CACHE2);
            }
        }, WAIT_CONDITION_TIMEOUT));

        mgr1.enableStatistics(CACHE1, true);
        mgr2.enableStatistics(CACHE2, false);

        assertTrue(GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                return isStatisticsEnabled(mgr1, CACHE1) && isStatisticsEnabled(mgr2, CACHE1)
                    && !isStatisticsEnabled(mgr1, CACHE2) && !isStatisticsEnabled(mgr2, CACHE2);
            }
        }, WAIT_CONDITION_TIMEOUT));
    }

    /**
     *
     */
    @Test
    public void testPublicApiStatisticsEnable() throws Exception {
        Ignite ig1 = startGrid(1);
        startGrid(2);

        IgniteCache<Object, Object> cache1 = ig1.cache(CACHE1);

        CacheConfiguration<Object, Object> cacheCfg2 = new CacheConfiguration<Object, Object>(cache1.getConfiguration(CacheConfiguration.class));

        cacheCfg2.setName(CACHE2);
        cacheCfg2.setStatisticsEnabled(true);

        ig1.getOrCreateCache(cacheCfg2);

        assertCachesStatisticsMode(false, true);

        cache1.enableStatistics(true);

        assertCachesStatisticsMode(true, true);

        ig1.cluster().enableStatistics(Arrays.asList(CACHE1, CACHE2), false);

        assertCachesStatisticsMode(false, false);
    }

    /**
     *
     */
    @Test
    public void testMultiThreadStatisticsEnable() throws Exception {
        startGrids(5);

        IgniteCache<?, ?> cache1 = grid(0).cache(CACHE1);

        CacheConfiguration<Object, Object> cacheCfg2 = new CacheConfiguration<Object, Object>(cache1.getConfiguration(CacheConfiguration.class));

        cacheCfg2.setName(CACHE2);

        grid(0).getOrCreateCache(cacheCfg2);

        awaitPartitionMapExchange();

        final CyclicBarrier barrier = new CyclicBarrier(10);

        final AtomicInteger gridIdx = new AtomicInteger(-1);

        IgniteInternalFuture<?> fut = multithreadedAsync(new Runnable() {
            @Override public void run() {
                try {
                    Ignite ignite = grid(gridIdx.incrementAndGet() % 5);

                    barrier.await();

                    ignite.cluster().enableStatistics(Arrays.asList(CACHE1, CACHE2), true);

                    assertCachesStatisticsMode(true, true);

                    barrier.await();

                    ignite.cluster().enableStatistics(Arrays.asList(CACHE1, CACHE2), false);

                    assertCachesStatisticsMode(false, false);

                    barrier.await();

                    ignite.cluster().enableStatistics(Collections.singletonList(CACHE1), true);

                    assertCachesStatisticsMode(true, false);
                }
                catch (InterruptedException | BrokenBarrierException | IgniteCheckedException e) {
                    fail("Unexpected exception: " + e);
                }
            }
        }, 10);

        fut.get();

        startGrid(5);

        assertCachesStatisticsMode(true, false);
    }

    /**
     *
     */
    @Test
    public void testCacheApiClearStatistics() throws Exception {
        startGrids(3);

        IgniteCache<Integer, String> cache = grid(0).cache(CACHE1);

        cache.enableStatistics(true);

        incrementCacheStatistics(cache);

        cache.clearStatistics();

        assertCacheStatisticsIsClear(Collections.singleton(CACHE1));
    }

    /**
     *
     */
    @Test
    public void testClearStatisticsAfterDisableStatistics() throws Exception {
        startGrids(3);

        IgniteCache<Integer, String> cache = grid(0).cache(CACHE1);

        cache.enableStatistics(true);

        incrementCacheStatistics(cache);

        cache.enableStatistics(false);

        cache.clearStatistics();

        cache.enableStatistics(true);

        assertCacheStatisticsIsClear(Collections.singleton(CACHE1));
    }

    /**
     *
     */
    @Test
    public void testClusterApiClearStatistics() throws Exception {
        startGrids(3);

        IgniteCache<Object, Object> cache = grid(0).cache(CACHE1);

        cache.enableStatistics(true);

        grid(0).getOrCreateCache(
            new CacheConfiguration<Object, Object>(cache.getConfiguration(CacheConfiguration.class)).setName(CACHE2)
        ).enableStatistics(true);

        Collection<String> cacheNames = Arrays.asList(CACHE1, CACHE2);

        for (String cacheName : cacheNames)
            incrementCacheStatistics(grid(0).cache(cacheName));

        grid(0).cluster().clearStatistics(cacheNames);

        assertCacheStatisticsIsClear(cacheNames);
    }

    /**
     *
     */
    @Test
    public void testJmxApiClearStatistics() throws Exception {
        startGrids(3);

        IgniteCache<Integer, String> cache = grid(0).cache(CACHE1);

        cache.enableStatistics(true);

        incrementCacheStatistics(cache);

        mxBean(0, CACHE1, CacheClusterMetricsMXBeanImpl.class).clear();

        assertCacheStatisticsIsClear(Collections.singleton(CACHE1));
    }

    /**
     * Cache statistics is cleared on all nodes.
     *
     * @param cacheNames a collection of cache names.
     */
    private void assertCacheStatisticsIsClear(Collection<String> cacheNames) throws Exception {
        for (String cacheName : cacheNames) {
            for (Ignite ig : G.allGrids()) {
                assertTrue(GridTestUtils.waitForCondition(new GridAbsPredicate() {
                    @SuppressWarnings("ErrorNotRethrown")
                    @Override public boolean apply() {
                        boolean res = true;

                        IgniteCache<?, ?> cache = ig.cache(cacheName);

                        try {
                            assertEquals("CachePuts", 0L, cache.mxBean().getCachePuts());
                            assertEquals("CacheHits", 0L, cache.mxBean().getCacheHits());
                            assertEquals("CacheGets", 0L, cache.mxBean().getCacheGets());
                            assertEquals("CacheRemovals", 0L, cache.mxBean().getCacheRemovals());
                            assertEquals("CacheMisses", 0L, cache.mxBean().getCacheMisses());
                            assertEquals("AverageGetTime", 0f, cache.mxBean().getAveragePutTime());
                            assertEquals("AveragePutTime", 0f, cache.mxBean().getAverageGetTime());
                            assertEquals("AverageRemoveTime", 0f, cache.mxBean().getAverageRemoveTime());
                        }
                        catch (AssertionError e) {
                            log.warning(e.toString());

                            res = false;
                        }

                        return res;
                    }
                }, WAIT_CONDITION_TIMEOUT));
            }
        }
    }

    /**
     * Increment cache statistics.
     *
     * @param cache cache.
     */
    private void incrementCacheStatistics(IgniteCache<Integer, String> cache) {
        cache.get(1);
        cache.put(1, "one");
        cache.get(1);
        cache.remove(1);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        cleanPersistenceDir();

        persistence = false;
    }

    /**
     * @param persistence Persistence.
     */
    private void testJmxStatisticsEnable(boolean persistence) throws Exception {
        this.persistence = persistence;

        Ignite ig1 = startGrid(1);
        Ignite ig2 = startGrid(2);

        ig1.cluster().active(true);

        CacheConfiguration<Object, Object> ccfg = ig1.cache(CACHE1).getConfiguration(CacheConfiguration.class);
        CacheConfiguration<Object, Object> cacheCfg2 = new CacheConfiguration<>(ccfg);

        cacheCfg2.setName(CACHE2);
        cacheCfg2.setStatisticsEnabled(true);

        ig2.getOrCreateCache(cacheCfg2);

        CacheMetricsMXBean mxBeanCache1 = mxBean(2, CACHE1, CacheClusterMetricsMXBeanImpl.class);
        CacheMetricsMXBean mxBeanCache2 = mxBean(2, CACHE2, CacheClusterMetricsMXBeanImpl.class);
        CacheMetricsMXBean mxBeanCache1loc = mxBean(2, CACHE1, CacheLocalMetricsMXBeanImpl.class);

        mxBeanCache1.enableStatistics();
        mxBeanCache2.disableStatistics();

        assertCachesStatisticsMode(true, false);

        stopGrid(1);

        startGrid(3);

        assertCachesStatisticsMode(true, false);

        mxBeanCache1loc.disableStatistics();

        assertCachesStatisticsMode(false, false);

        mxBeanCache1.enableStatistics();
        mxBeanCache2.enableStatistics();

        // Start node 1 again.
        startGrid(1);

        if (persistence)
            ig2.resetLostPartitions(Arrays.asList(CACHE1, CACHE2));

        assertCachesStatisticsMode(true, true);

        int gridIdx = 0;

        for (Ignite ignite : G.allGrids()) {
            gridIdx++;

            ignite.cache(CACHE1).put(gridIdx, gridIdx);
            ignite.cache(CACHE2).put(gridIdx, gridIdx);

            ignite.cache(CACHE1).get(gridIdx);
            ignite.cache(CACHE2).get(gridIdx);
        }

        assertTrue(GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                int cnt = G.allGrids().size();

                for (Ignite ignite : G.allGrids()) {
                    CacheMetrics metrics1 = ignite.cache(CACHE1).metrics();
                    CacheMetrics metrics2 = ignite.cache(CACHE2).metrics();

                    if (metrics1.getCacheGets() < cnt || metrics2.getCacheGets() < cnt
                        || metrics1.getCachePuts() < cnt || metrics2.getCachePuts() < cnt)
                        return false;
                }

                return true;
            }
        }, 10_000L));

        stopAllGrids();

        ig1 = startGrid(1);

        ig1.cluster().active(true);

        ig1.getOrCreateCache(cacheCfg2.setStatisticsEnabled(false));

        if (persistence)
            // Both caches restored from pds.
            assertCachesStatisticsMode(true, true);
        else
            assertCachesStatisticsMode(false, false);

        mxBeanCache1 = mxBean(1, CACHE1, CacheLocalMetricsMXBeanImpl.class);
        mxBeanCache2 = mxBean(1, CACHE2, CacheLocalMetricsMXBeanImpl.class);

        mxBeanCache1.enableStatistics();
        mxBeanCache2.disableStatistics();

        startGrid(2);

        assertCachesStatisticsMode(true, false);
    }

    /**
     * Gets CacheGroupMetricsMXBean for given node and group name.
     *
     * @param nodeIdx Node index.
     * @param cacheName Cache name.
     * @return MBean instance.
     */
    private CacheMetricsMXBean mxBean(int nodeIdx, String cacheName, Class<? extends CacheMetricsMXBean> clazz) {
        return getMxBean(getTestIgniteInstanceName(nodeIdx), cacheName, clazz.getName(), CacheMetricsMXBean.class);
    }

    /** Default cache config. */
    private CacheConfiguration<?, ?> getCacheConfiguration() {
        CacheConfiguration<?, ?> cacheCfg = new CacheConfiguration<>()
            .setName(CACHE1)
            .setGroupName(GROUP)
            .setCacheMode(CacheMode.PARTITIONED)
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
            .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);

        if (backups != -1)
            cacheCfg.setBackups(2);

        return cacheCfg;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        CacheConfiguration<?, ?> cacheCfg = getCacheConfiguration();

        cfg.setCacheConfiguration(cacheCfg);

        if (useTestCommSpi)
            cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());

        if (client)
            cfg.setClientMode(client);

        if (persistence)
            cfg.setDataStorageConfiguration(new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration()
                        .setPersistenceEnabled(true)
                        .setMaxSize(DataStorageConfiguration.DFLT_DATA_REGION_INITIAL_SIZE)
                )
                .setWalMode(WALMode.LOG_ONLY)
            );

        return cfg;
    }

    /** Test correct metric for tx key contention. */
    @Test
    @WithSystemProperty(key = IGNITE_DUMP_TX_COLLISIONS_INTERVAL, value = "30000")
    public void testTxContentionMetric() throws Exception {
        Assume.assumeFalse("https://issues.apache.org/jira/browse/IGNITE-9224", MvccFeatureChecker.forcedMvcc());

        backups = 1;

        useTestCommSpi = true;

        Ignite ig = startGridsMultiThreaded(2);

        int contCnt = (int)U.staticField(IgniteTxManager.class, "COLLISIONS_QUEUE_THRESHOLD") * 20;

        CountDownLatch txLatch = new CountDownLatch(contCnt * 2);

        CountDownLatch txLatch0 = new CountDownLatch(contCnt * 2);

        ig.cluster().active(true);

        client = true;

        Ignite cl = startGrid();

        CacheConfiguration<?, ?> dfltCacheCfg = getCacheConfiguration();

        dfltCacheCfg.setStatisticsEnabled(true);

        String cacheName = dfltCacheCfg.getName();

        IgniteCache<Integer, Integer> cache = ig.cache(cacheName);

        IgniteCache<Integer, Integer> cache0 = cl.cache(cacheName);

        CacheMetricsMXBean mxBeanCache = mxBean(0, cacheName, CacheLocalMetricsMXBeanImpl.class);

        final List<Integer> priKeys = primaryKeys(cache, 3, 1);

        final Integer backKey = backupKey(cache);

        IgniteTransactions txMgr = cl.transactions();

        CountDownLatch blockOnce = new CountDownLatch(1);

        for (Ignite ig0 : G.allGrids()) {
            TestRecordingCommunicationSpi commSpi0 =
                (TestRecordingCommunicationSpi)ig0.configuration().getCommunicationSpi();

            commSpi0.blockMessages(new IgniteBiPredicate<ClusterNode, Message>() {
                @Override public boolean apply(ClusterNode node, Message msg) {
                    if (msg instanceof GridNearTxPrepareResponse && blockOnce.getCount() > 0) {
                        blockOnce.countDown();

                        return true;
                    }

                    return false;
                }
            });
        }

        IgniteInternalFuture f = GridTestUtils.runAsync(() -> {
            try (Transaction tx = txMgr.txStart(PESSIMISTIC, READ_COMMITTED)) {
                cache0.put(priKeys.get(0), 0);
                cache0.put(priKeys.get(2), 0);
                tx.commit();
            }
        });

        blockOnce.await();

        GridCompoundFuture<?, ?> finishFut = new GridCompoundFuture<>();

        for (int i = 0; i < contCnt; ++i) {
            IgniteInternalFuture f0 = GridTestUtils.runAsync(() -> {
                try (Transaction tx = txMgr.txStart(PESSIMISTIC, READ_COMMITTED)) {
                    cache0.put(priKeys.get(0), 0);
                    cache0.put(priKeys.get(1), 0);

                    txLatch0.countDown();

                    tx.commit();

                    txLatch.countDown();
                }

                try (Transaction tx = txMgr.txStart(PESSIMISTIC, READ_COMMITTED)) {
                    cache0.put(priKeys.get(2), 0);
                    cache0.put(backKey, 0);

                    txLatch0.countDown();

                    tx.commit();

                    txLatch.countDown();
                }
            });

            finishFut.add(f0);
        }

        finishFut.markInitialized();

        txLatch0.await();

        for (Ignite ig0 : G.allGrids()) {
            TestRecordingCommunicationSpi commSpi0 =
                (TestRecordingCommunicationSpi)ig0.configuration().getCommunicationSpi();

            commSpi0.stopBlock();
        }

        IgniteTxManager txManager = ((IgniteEx) ig).context().cache().context().tm();

        assertTrue(GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                try {
                    U.invoke(IgniteTxManager.class, txManager, "collectTxCollisionsInfo");
                }
                catch (IgniteCheckedException e) {
                    fail(e.toString());
                }

                String coll = mxBeanCache.getTxKeyCollisions();

                if (coll.contains("val=" + priKeys.get(2)) || coll.contains("val=" + priKeys.get(0)));
                    return true;
            }
        }, 10_000));

        f.get();

        finishFut.get();

        txLatch.await();
    }

    /** Tests metric change interval. */
    @Test
    public void testKeyCollisionsMetricDifferentTimeout() throws Exception {
        Assume.assumeFalse("https://issues.apache.org/jira/browse/IGNITE-9224", MvccFeatureChecker.forcedMvcc());

        backups = 2;

        useTestCommSpi = true;

        Ignite ig = startGridsMultiThreaded(3);

        int contCnt = (int)U.staticField(IgniteTxManager.class, "COLLISIONS_QUEUE_THRESHOLD") * 5;

        CountDownLatch txLatch = new CountDownLatch(contCnt);

        ig.cluster().active(true);

        client = true;

        Ignite cl = startGrid();

        IgniteTransactions txMgr = cl.transactions();

        CacheConfiguration<?, ?> dfltCacheCfg = getCacheConfiguration();

        dfltCacheCfg.setStatisticsEnabled(true);

        String cacheName = dfltCacheCfg.getName();

        IgniteCache<Integer, Integer> cache = ig.cache(cacheName);

        IgniteCache<Integer, Integer> cache0 = cl.cache(cacheName);

        final Integer keyId = primaryKey(cache);

        CountDownLatch blockOnce = new CountDownLatch(1);

        for (Ignite ig0 : G.allGrids()) {
            if (ig0.configuration().isClientMode())
                continue;

            TestRecordingCommunicationSpi commSpi0 =
                (TestRecordingCommunicationSpi)ig0.configuration().getCommunicationSpi();

            commSpi0.blockMessages(new IgniteBiPredicate<ClusterNode, Message>() {
                @Override public boolean apply(ClusterNode node, Message msg) {
                    if (msg instanceof GridNearTxFinishResponse && blockOnce.getCount() > 0) {
                        blockOnce.countDown();

                        return true;
                    }

                    return false;
                }
            });
        }

        IgniteInternalFuture f = GridTestUtils.runAsync(() -> {
            try (Transaction tx = txMgr.txStart(PESSIMISTIC, READ_COMMITTED)) {
                cache0.put(keyId, 0);
                tx.commit();
            }
        });

        blockOnce.await();

        GridCompoundFuture<?, ?> finishFut = new GridCompoundFuture<>();

        for (int i = 0; i < contCnt; ++i) {
            IgniteInternalFuture f0 = GridTestUtils.runAsync(() -> {
                try (Transaction tx = txMgr.txStart(PESSIMISTIC, READ_COMMITTED)) {
                    cache0.put(keyId, 0);

                    tx.commit();

                    txLatch.countDown();
                }
            });

            finishFut.add(f0);
        }

        finishFut.markInitialized();

        for (Ignite ig0 : G.allGrids()) {
            TestRecordingCommunicationSpi commSpi0 =
                (TestRecordingCommunicationSpi)ig0.configuration().getCommunicationSpi();

            if (ig0.configuration().isClientMode())
                continue;

            commSpi0.stopBlock();
        }

        CacheMetricsMXBean mxBeanCache = mxBean(0, cacheName, CacheLocalMetricsMXBeanImpl.class);

        IgniteTxManager txManager = ((IgniteEx) ig).context().cache().context().tm();

        final TransactionsMXBean txMXBean1 = txMXBean(0);

        final TransactionsMXBean txMXBean2 = txMXBean(1);

        for (int i = 0; i < 10; ++i) {
            txMXBean1.setTxKeyCollisionsInterval(ThreadLocalRandom.current().nextInt(1000, 1100));

            txMXBean2.setTxKeyCollisionsInterval(ThreadLocalRandom.current().nextInt(1000, 1100));

            mxBeanCache.getTxKeyCollisions();

            mxBeanCache.clear();

            try {
                U.invoke(IgniteTxManager.class, txManager, "collectTxCollisionsInfo");
            }
            catch (IgniteCheckedException e) {
                fail(e.toString());
            }

            U.sleep(500);
        }

        f.get();

        finishFut.get();

        txLatch.await();
    }

    /** @throws Exception If failed. */
    @Test
    public void testCacheSizeOnInactiveCluster() throws Exception {
        persistence = true;

        IgniteEx grid = startGrid(0);

        assertFalse(grid.cluster().state().active());

        CacheMetricsMXBean mxBean = mxBean(0, CACHE1, CacheLocalMetricsMXBeanImpl.class);

        long size = mxBean.getCacheSize();

        assertEquals(-1, size);
    }

    /**
     *
     */
    private TransactionsMXBean txMXBean(int igniteInt) throws Exception {
        ObjectName mbeanName = U.makeMBeanName(getTestIgniteInstanceName(igniteInt), "Transactions",
            TransactionsMXBeanImpl.class.getSimpleName());

        MBeanServer mbeanSrv = ManagementFactory.getPlatformMBeanServer();

        if (!mbeanSrv.isRegistered(mbeanName))
            fail("MBean is not registered: " + mbeanName.getCanonicalName());

        return MBeanServerInvocationHandler.newProxyInstance(mbeanSrv, mbeanName, TransactionsMXBean.class, true);
    }

    /**
     * Check cache statistics enabled/disabled flag for all nodes
     *
     * @param cacheName Cache name.
     * @param enabled Enabled.
     */
    private boolean checkStatisticsMode(String cacheName, boolean enabled) {
        for (Ignite ignite : G.allGrids())
            if (ignite.cache(cacheName).metrics().isStatisticsEnabled() != enabled) {
                log.warning("Wrong cache statistics mode [grid=" + ignite.name() + ", cache=" + cacheName
                    + ", expected=" + enabled);

                return false;
            }

        return true;
    }

    /**
     * @param enabled1 Statistics enabled for cache 1.
     * @param enabled2 Statistics enabled for cache 2.
     */
    private void assertCachesStatisticsMode(final boolean enabled1, final boolean enabled2) throws IgniteInterruptedCheckedException {
        assertTrue(GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                return checkStatisticsMode(CACHE1, enabled1) && checkStatisticsMode(CACHE2, enabled2);
            }
        }, WAIT_CONDITION_TIMEOUT));
    }

    /**
     * @param cacheMgr Cache manager.
     * @param cacheName Cache name.
     */
    private boolean isStatisticsEnabled(CacheManager cacheMgr, String cacheName) {
        return ((IgniteCache)cacheMgr.getCache(cacheName)).metrics().isStatisticsEnabled();
    }
}
