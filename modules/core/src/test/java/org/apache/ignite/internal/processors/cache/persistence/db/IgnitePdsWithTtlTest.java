/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.persistence.db;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.cache.expiry.AccessedExpiryPolicy;
import javax.cache.expiry.CreatedExpiryPolicy;
import javax.cache.expiry.Duration;
import javax.cache.expiry.ExpiryPolicy;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.failure.FailureHandler;
import org.apache.ignite.failure.NoOpFailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteFutureTimeoutCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.IgniteCacheProxy;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.persistence.checkpoint.CheckpointManager;
import org.apache.ignite.internal.util.ReentrantReadWriteLockWithTracking;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.lang.GridCursor;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.PA;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.MvccFeatureChecker;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.cluster.ClusterState.INACTIVE;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;
import static org.apache.ignite.testframework.GridTestUtils.runMultiThreadedAsync;
import static org.apache.ignite.testframework.GridTestUtils.waitForAllFutures;

/**
 * Test TTL worker with persistence enabled
 */
@WithSystemProperty(key = IgniteSystemProperties.IGNITE_UNWIND_THROTTLING_TIMEOUT, value = "5")
public class IgnitePdsWithTtlTest extends GridCommonAbstractTest {
    /** */
    private static final String CACHE_NAME_ATOMIC = "expirable-cache-atomic";

    /** */
    private static final String CACHE_NAME_ATOMIC_NON_PERSISTENT = "expirable-non-persistent-cache-atomic";

    /** */
    private static final String CACHE_NAME_TX = "expirable-cache-tx";

    /** */
    private static final String CACHE_NAME_NEAR_ATOMIC = "expirable-cache-near-atomic";

    /** */
    private static final String CACHE_NAME_NEAR_TX = "expirable-cache-near-tx";

    /** */
    private static final String NON_PERSISTENT_DATA_REGION = "non-persistent-region";

    /** */
    public static final int PART_SIZE = 2;

    /** */
    private static final int EXPIRATION_TIMEOUT = 10;

    /** */
    public static final int ENTRIES = 50_000;

    /** */
    public static final int SMALL_ENTRIES = 10;

    /** */
    private static final int WORKLOAD_THREADS_CNT = 16;

    /** Fail. */
    private volatile boolean failureHndTriggered;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        MvccFeatureChecker.skipIfNotSupported(MvccFeatureChecker.Feature.EXPIRATION);

        super.beforeTest();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        //protection if test failed to finish, e.g. by error
        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        final IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        DataRegionConfiguration dfltRegion = new DataRegionConfiguration()
            .setMaxSize(2L * 1024 * 1024 * 1024)
            .setPersistenceEnabled(true);

        DataRegionConfiguration nonPersistentRegion = new DataRegionConfiguration()
            .setName(NON_PERSISTENT_DATA_REGION)
            .setMaxSize(2L * 1024 * 1024 * 1024)
            .setPersistenceEnabled(false);

        cfg.setDataStorageConfiguration(
            new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(dfltRegion)
                .setDataRegionConfigurations(nonPersistentRegion)
                .setWalMode(WALMode.LOG_ONLY));

        cfg.setCacheConfiguration(
            getCacheConfiguration(CACHE_NAME_ATOMIC).setAtomicityMode(ATOMIC),
            getCacheConfiguration(CACHE_NAME_TX).setAtomicityMode(TRANSACTIONAL),
            getCacheConfiguration(CACHE_NAME_NEAR_ATOMIC).setAtomicityMode(ATOMIC)
                .setNearConfiguration(new NearCacheConfiguration<>()),
            getCacheConfiguration(CACHE_NAME_NEAR_TX).setAtomicityMode(TRANSACTIONAL)
                .setNearConfiguration(new NearCacheConfiguration<>())
        );

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected FailureHandler getFailureHandler(String igniteInstanceName) {
        return new NoOpFailureHandler() {
            @Override protected boolean handle(Ignite ignite, FailureContext failureCtx) {
                failureHndTriggered = true;

                return super.handle(ignite, failureCtx);
            }
        };
    }

    /**
     * Returns a new cache configuration with the given name and {@code GROUP_NAME} group.
     *
     * @param name Cache name.
     * @return Cache configuration.
     */
    private CacheConfiguration<?, ?> getCacheConfiguration(String name) {
        CacheConfiguration<?, ?> ccfg = new CacheConfiguration<>();

        ccfg.setName(name);
        ccfg.setAffinity(new RendezvousAffinityFunction(false, PART_SIZE));
        ccfg.setExpiryPolicyFactory(AccessedExpiryPolicy.factoryOf(new Duration(TimeUnit.MILLISECONDS, EXPIRATION_TIMEOUT)));
        ccfg.setEagerTtl(true);
        ccfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        ccfg.setRebalanceMode(CacheRebalanceMode.SYNC);

        return ccfg;
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testTtlIsApplied() throws Exception {
        loadAndWaitForCleanup(false);
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testTtlIsAppliedForMultipleCaches() throws Exception {
        IgniteEx srv = startGrid(0);
        srv.cluster().state(ACTIVE);

        int cacheCnt = 2;

        // Create a new caches in the same group.
        // It is important that initially created cache CACHE_NAME remains empty.
        for (int i = 0; i < cacheCnt; ++i) {
            String cacheName = CACHE_NAME_ATOMIC + "-" + i;

            srv.getOrCreateCache(getCacheConfiguration(cacheName));

            fillCache(srv.cache(cacheName));
        }

        waitAndCheckExpired(srv, srv.cache(CACHE_NAME_ATOMIC + "-" + (cacheCnt - 1)));

        srv.cluster().state(ACTIVE);

        stopAllGrids();
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testTtlIsAppliedAfterRestart() throws Exception {
        loadAndWaitForCleanup(true);
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testPutOpsIntoCacheWithExpirationConcurrentlyWithCheckpointCompleteSuccessfully() throws Exception {
        IgniteEx ig0 = startGrid(0);

        ig0.cluster().state(ACTIVE);

        IgniteCache<Object, Object> cache = ig0.getOrCreateCache(CACHE_NAME_ATOMIC);

        AtomicBoolean timeoutReached = new AtomicBoolean(false);

        CheckpointManager checkpointManager = U.field(ig0.context().cache().context().database(), "checkpointManager");

        IgniteInternalFuture<?> ldrFut = runMultiThreadedAsync(() -> {
            while (!timeoutReached.get()) {
                Map<Object, Object> map = new TreeMap<>();

                for (int i = 0; i < ENTRIES; i++)
                    map.put(i, i);

                cache.putAll(map);
            }
        }, WORKLOAD_THREADS_CNT, "loader");

        IgniteInternalFuture<?> updaterFut = runMultiThreadedAsync(() -> {
            while (!timeoutReached.get()) {
                for (int i = 0; i < SMALL_ENTRIES; i++)
                    cache.put(i, i * 10);
            }
        }, WORKLOAD_THREADS_CNT, "updater");

        IgniteInternalFuture<?> cpWriteLockUnlockFut = runAsync(() -> {
            Object checkpointReadWriteLock = U.field(
                checkpointManager.checkpointTimeoutLock(), "checkpointReadWriteLock"
            );

            ReentrantReadWriteLockWithTracking lock = U.field(checkpointReadWriteLock, "checkpointLock");

            while (!timeoutReached.get()) {
                try {
                    lock.writeLock().lockInterruptibly();

                    doSleep(30);
                }
                catch (InterruptedException ignored) {
                    break;
                }
                finally {
                    lock.writeLock().unlock();
                }

                doSleep(30);
            }
        }, "cp-write-lock-holder");

        doSleep(10_000);

        timeoutReached.set(true);

        waitForAllFutures(cpWriteLockUnlockFut, ldrFut, updaterFut);
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testConcurrentPutOpsToCacheWithExpirationCompleteSuccesfully() throws Exception {
        final AtomicBoolean end = new AtomicBoolean();

        final IgniteEx srv = startGrids(3);

        srv.cluster().state(ACTIVE);

        // Start high workload.
        IgniteInternalFuture<?> loadFut = runMultiThreadedAsync(() -> {
            List<IgniteCache<Object, Object>> caches = F.asList(
                srv.cache(CACHE_NAME_ATOMIC),
                srv.cache(CACHE_NAME_TX),
                srv.cache(CACHE_NAME_NEAR_ATOMIC),
                srv.cache(CACHE_NAME_NEAR_TX)
            );

            while (!end.get() && !failureHndTriggered) {
                for (IgniteCache<Object, Object> cache : caches) {
                    for (int i = 0; i < SMALL_ENTRIES; i++)
                        cache.put(i, new byte[1024]);

                    cache.putAll(new TreeMap<>(F.asMap(0, new byte[1024], 1, new byte[1024])));

                    for (int i = 0; i < SMALL_ENTRIES; i++)
                        cache.get(i);

                    cache.getAll(new TreeSet<>(F.asList(0, 1)));
                }
            }
        }, WORKLOAD_THREADS_CNT, "high-workload");

        try {
            // Let's wait some time.
            loadFut.get(10, TimeUnit.SECONDS);
        }
        catch (Exception e) {
            assertFalse("Failure handler was called. See log above.", failureHndTriggered);

            assertTrue(X.hasCause(e, IgniteFutureTimeoutCheckedException.class));
        }
        finally {
            end.set(true);
        }

        assertFalse("Failure handler was called. See log above.", failureHndTriggered);
    }

    /**
     * @throws Exception if failed.
     */
    private void loadAndWaitForCleanup(boolean restartGrid) throws Exception {
        IgniteEx srv = startGrid(0);

        srv.cluster().state(ACTIVE);

        fillCache(srv.cache(CACHE_NAME_ATOMIC));

        if (restartGrid) {
            srv.context().cache().context().database().waitForCheckpoint("test-checkpoint");

            stopGrid(0);
            srv = startGrid(0);
            srv.cluster().state(ACTIVE);
        }

        final IgniteCache<Integer, byte[]> cache = srv.cache(CACHE_NAME_ATOMIC);

        printStatistics((IgniteCacheProxy)cache, "After restart from LFS");

        waitAndCheckExpired(srv, cache);

        srv.cluster().state(ACTIVE);

        stopAllGrids();
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testRebalancingWithTtlExpirable() throws Exception {
        IgniteEx srv = startGrid(0);

        srv.cluster().baselineAutoAdjustEnabled(false);
        srv.cluster().state(ACTIVE);

        fillCache(srv.cache(CACHE_NAME_ATOMIC));

        srv = startGrid(1);

        //causes rebalancing start
        srv.cluster().setBaselineTopology(srv.cluster().topologyVersion());

        final IgniteCache<Integer, byte[]> cache = srv.cache(CACHE_NAME_ATOMIC);

        printStatistics((IgniteCacheProxy)cache, "After rebalancing start");

        waitAndCheckExpired(srv, cache);

        srv.cluster().state(INACTIVE);

        stopAllGrids();
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testStartStopAfterRebalanceWithTtlExpirable() throws Exception {
        try {
            IgniteEx srv = startGrid(0);

            srv.cluster().baselineAutoAdjustEnabled(false);

            startGrid(1);
            srv.cluster().active(true);

            ExpiryPolicy plc = CreatedExpiryPolicy.factoryOf(Duration.ONE_DAY).create();

            IgniteCache<Integer, byte[]> cache0 = srv.cache(CACHE_NAME_ATOMIC);

            fillCache(cache0.withExpiryPolicy(plc));

            srv = startGrid(2);

            IgniteCache<Integer, byte[]> cache = srv.cache(CACHE_NAME_ATOMIC);

            //causes rebalancing start
            srv.cluster().setBaselineTopology(srv.cluster().topologyVersion());

            GridTestUtils.waitForCondition(new GridAbsPredicate() {
                @Override public boolean apply() {
                    return Boolean.TRUE.equals(cache.rebalance().get()) && cache.localSizeLong(CachePeekMode.ALL) > 0;
                }
            }, 20_000);

            //check if pds is consistent
            stopGrid(0);
            startGrid(0);

            stopGrid(1);
            startGrid(1);

            srv.cluster().state(INACTIVE);
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * Tests that cache entries (cache related to non persistent region) correctly expired.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testExpirationNonPersistentRegion() throws Exception {
        IgniteEx srv = startGrid(0);

        srv.cluster().baselineAutoAdjustEnabled(false);
        srv.cluster().state(ACTIVE);

        CacheConfiguration<?, ?> cfg =
            getCacheConfiguration(CACHE_NAME_ATOMIC_NON_PERSISTENT)
                .setAtomicityMode(ATOMIC)
                .setDataRegionName(NON_PERSISTENT_DATA_REGION);

        srv.getOrCreateCache(cfg);

        IgniteCache<Integer, byte[]> nonPersistentCache = srv.cache(CACHE_NAME_ATOMIC_NON_PERSISTENT);

        fillCache(nonPersistentCache);

        waitAndCheckExpired(srv, nonPersistentCache);

        stopAllGrids();

        assertFalse("Failure handler should not be triggered.", failureHndTriggered);
    }

    /**
     *
     */
    protected void fillCache(IgniteCache<Integer, byte[]> cache) {
        TreeMap<Integer, byte[]> data = new TreeMap<>();

        for (int i = 0; i < ENTRIES; i++)
            data.put(i, new byte[1024]);

        cache.putAll(data);

        //Touch entries.
        for (int i = 0; i < ENTRIES; i++)
            cache.get(i); // touch entries

        printStatistics((IgniteCacheProxy)cache, "After cache puts");
    }

    /**
     *
     */
    protected void waitAndCheckExpired(
        IgniteEx srv,
        final IgniteCache<Integer, byte[]> cache
    ) throws IgniteCheckedException {
        boolean awaited = GridTestUtils.waitForCondition(new PA() {
            @Override public boolean apply() {
                return cache.size() == 0;
            }
        }, TimeUnit.SECONDS.toMillis(EXPIRATION_TIMEOUT + EXPIRATION_TIMEOUT / 2));

        assertTrue("Cache is not empty. size=" + cache.size(), awaited);

        printStatistics((IgniteCacheProxy)cache, "After timeout");

        GridCacheSharedContext ctx = srv.context().cache().context();
        GridCacheContext cctx = ctx.cacheContext(CU.cacheId(CACHE_NAME_ATOMIC));

        // Check partitions through internal API.
        for (int partId = 0; partId < PART_SIZE; ++partId) {
            GridDhtLocalPartition locPart = cctx.dht().topology().localPartition(partId);

            if (locPart == null)
                continue;

            GridCursor cur = locPart.dataStore().cursor();

            assertFalse(cur.next());
            assertEquals(0, locPart.fullSize());
        }

        for (int i = 0; i < ENTRIES; i++)
            assertNull(cache.get(i));
    }

    /**
     *
     */
    private void printStatistics(IgniteCacheProxy cache, String msg) {
        System.out.println(msg + " {{");
        cache.context().printMemoryStats();
        System.out.println("}} " + msg);
    }
}
