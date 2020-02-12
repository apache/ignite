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

import javax.cache.expiry.AccessedExpiryPolicy;
import javax.cache.expiry.CreatedExpiryPolicy;
import javax.cache.expiry.Duration;
import javax.cache.expiry.ExpiryPolicy;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.failure.FailureHandler;
import org.apache.ignite.failure.NoOpFailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteFutureTimeoutCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.IgniteCacheOffheapManager;
import org.apache.ignite.internal.processors.cache.IgniteCacheProxy;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.lang.GridCursor;
import org.apache.ignite.internal.util.typedef.PA;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.testframework.GridTestUtils.runAsync;
import static org.apache.ignite.testframework.GridTestUtils.runMultiThreadedAsync;
import static org.apache.ignite.testframework.GridTestUtils.waitForAllFutures;

/**
 * Test TTL worker with persistence enabled
 */
public class IgnitePdsWithTtlTest extends GridCommonAbstractTest {
    /** */
    public static final String CACHE_NAME = "expirableCache";

    /** */
    public static final String GROUP_NAME = "group1";

    /** */
    public static final int PART_SIZE = 2;

    /** */
    private static final int EXPIRATION_TIMEOUT = 10;

    /** */
    public static final int ENTRIES = 50_000;

    /** */
    private static final TcpDiscoveryVmIpFinder FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final int SMALL_ENTRIES = 10;

    /** */
    private static final int WORKLOAD_THREADS_CNT = 8;

    /** Fail. */
    private volatile boolean fail;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
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

        TcpDiscoverySpi disco = new TcpDiscoverySpi();
        disco.setIpFinder(FINDER);

        cfg.setDiscoverySpi(disco);

        cfg.setDataStorageConfiguration(
            new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration()
                        .setMaxSize(2L * 1024 * 1024 * 1024)
                        .setPersistenceEnabled(true)
                ).setWalMode(WALMode.LOG_ONLY));

        cfg.setCacheConfiguration(getCacheConfiguration(CACHE_NAME));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected FailureHandler getFailureHandler(String igniteInstanceName) {
        return new NoOpFailureHandler() {
            @Override protected boolean handle(Ignite ignite, FailureContext failureCtx) {
                fail = true;

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
    private CacheConfiguration getCacheConfiguration(String name) {
        CacheConfiguration ccfg = new CacheConfiguration();

        ccfg.setName(name);
        ccfg.setGroupName(GROUP_NAME);
        ccfg.setAffinity(new RendezvousAffinityFunction(false, PART_SIZE));
        ccfg.setExpiryPolicyFactory(AccessedExpiryPolicy.factoryOf(new Duration(TimeUnit.SECONDS, EXPIRATION_TIMEOUT)));
        ccfg.setEagerTtl(true);
        ccfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        ccfg.setRebalanceMode(CacheRebalanceMode.SYNC);

        return ccfg;
    }

    /**
     * @throws Exception if failed.
     */
    public void testTtlIsApplied() throws Exception {
        loadAndWaitForCleanup(false);
    }

    /**
     * @throws Exception if failed.
     */
    public void testTtlIsAppliedForMultipleCaches() throws Exception {
        IgniteEx srv = startGrid(0);
        srv.cluster().active(true);

        int cacheCnt = 2;

        // Create a new caches in the same group.
        // It is important that initially created cache CACHE_NAME remains empty.
        for (int i = 0; i < cacheCnt; ++i) {
            String cacheName = CACHE_NAME + "-" + i;

            srv.getOrCreateCache(getCacheConfiguration(cacheName));

            fillCache(srv.cache(cacheName));
        }

        waitAndCheckExpired(srv, srv.cache(CACHE_NAME + "-" + (cacheCnt - 1)));

        stopAllGrids();
    }

    /**
     * @throws Exception if failed.
     */
    public void testTtlIsAppliedAfterRestart() throws Exception {
        loadAndWaitForCleanup(true);
    }

    /**
     * @throws Exception if failed.
     */
    public void testPutOpsIntoCacheWithExpirationConcurrentlyWithCheckpointCompleteSuccessfully() throws Exception {
        IgniteEx ig0 = startGrid(0);

        ig0.cluster().active(true);

        IgniteCache<Object, Object> cache = ig0.getOrCreateCache(CACHE_NAME);

        AtomicBoolean timeoutReached = new AtomicBoolean(false);

        GridCacheDatabaseSharedManager db = (GridCacheDatabaseSharedManager)ig0.context().cache().context().database();

        IgniteInternalFuture ldrFut = runMultiThreadedAsync(() -> {
            while (!timeoutReached.get()) {
                Map map = new TreeMap();

                for (int i = 0; i < SMALL_ENTRIES; i++)
                    map.put(i, i);

                cache.putAll(map);
            }
        }, WORKLOAD_THREADS_CNT, "loader");

        IgniteInternalFuture updaterFut = runMultiThreadedAsync(() -> {
            while (!timeoutReached.get()) {
                for (int i = 0; i < SMALL_ENTRIES; i++)
                    cache.put(i, i * 10);
            }
        }, WORKLOAD_THREADS_CNT, "updater");

        IgniteInternalFuture cpWriteLockUnlockFut = runAsync(() -> {
            ReentrantReadWriteLock lock = U.field(db, "checkpointLock");

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
    public void testConcurrentPutOpsToCacheWithExpirationCompleteSuccesfully() throws Exception {
        final AtomicBoolean end = new AtomicBoolean();

        final IgniteEx srv = startGrid(0);

        srv.cluster().active(true);

        // Start high workload
        IgniteInternalFuture loadFut = runMultiThreadedAsync(() -> {
            while (!end.get() && !fail) {
                IgniteCache<Integer, byte[]> cache = srv.cache(CACHE_NAME);

                for (int i = 0; i < SMALL_ENTRIES; i++)
                    cache.put(i, new byte[1024]);

                //Touch entries.
                for (int i = 0; i < SMALL_ENTRIES; i++)
                    cache.get(i); // touch entries
            }
        }, WORKLOAD_THREADS_CNT, "high-workload");

        try {
            // Let's wait some time.
            loadFut.get(10, TimeUnit.SECONDS);
        }
        catch (Exception e) {
            assertFalse("Failure handler was called. See log above.", fail);

            assertTrue(X.hasCause(e, IgniteFutureTimeoutCheckedException.class));
        }
        finally {
            end.set(true);
        }

        assertFalse("Failure handler was called. See log above.", fail);
    }

    /**
     * @throws Exception if failed.
     */
    private void loadAndWaitForCleanup(boolean restartGrid) throws Exception {
        IgniteEx srv = startGrid(0);
        srv.cluster().active(true);

        fillCache(srv.cache(CACHE_NAME));

        if (restartGrid) {
            srv.context().cache().context().database().wakeupForCheckpoint("test-checkpoint");

            stopGrid(0);
            srv = startGrid(0);
            srv.cluster().active(true);
        }

        final IgniteCache<Integer, byte[]> cache = srv.cache(CACHE_NAME);

        printStatistics((IgniteCacheProxy)cache, "After restart from LFS");

        waitAndCheckExpired(srv, cache);

        stopAllGrids();
    }

    /**
     * @throws Exception if failed.
     */
    public void testRebalancingWithTtlExpirable() throws Exception {
        IgniteEx srv = startGrid(0);
        srv.cluster().active(true);

        fillCache(srv.cache(CACHE_NAME));

        srv = startGrid(1);

        //causes rebalancing start
        srv.cluster().setBaselineTopology(srv.cluster().topologyVersion());

        final IgniteCache<Integer, byte[]> cache = srv.cache(CACHE_NAME);

        printStatistics((IgniteCacheProxy)cache, "After rebalancing start");

        waitAndCheckExpired(srv, cache);

        stopAllGrids();
    }

    /**
     * @throws Exception if failed.
     */
    public void testStartStopAfterRebalanceWithTtlExpirable() throws Exception {
        try {
            IgniteEx srv = startGrid(0);
            startGrid(1);
            srv.cluster().active(true);

            ExpiryPolicy plc = CreatedExpiryPolicy.factoryOf(Duration.ONE_DAY).create();

            IgniteCache<Integer, byte[]> cache0 = srv.cache(CACHE_NAME);

            fillCache(cache0.withExpiryPolicy(plc));

            srv = startGrid(2);

            IgniteCache<Integer, byte[]> cache = srv.cache(CACHE_NAME);

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
        }
        finally {
            stopAllGrids();
        }
    }

    /** */
    protected void fillCache(IgniteCache<Integer, byte[]> cache) {
        cache.putAll(new TreeMap<Integer, byte[]>() {{
            for (int i = 0; i < ENTRIES; i++)
                put(i, new byte[1024]);
        }});

        //Touch entries.
        for (int i = 0; i < ENTRIES; i++)
            cache.get(i); // touch entries

        printStatistics((IgniteCacheProxy)cache, "After cache puts");
    }

    /** */
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
        GridCacheContext cctx = ctx.cacheContext(CU.cacheId(CACHE_NAME));

        // Check partitions through internal API.
        for (int partId = 0; partId < PART_SIZE; ++partId) {
            GridDhtLocalPartition locPart = cctx.dht().topology().localPartition(partId);

            if (locPart == null)
                continue;

            IgniteCacheOffheapManager.CacheDataStore dataStore =
                ctx.cache().cacheGroup(CU.cacheId(GROUP_NAME)).offheap().dataStore(locPart);

            GridCursor cur = dataStore.cursor();

            assertFalse(cur.next());
            assertEquals(0, locPart.fullSize());
        }

        for (int i = 0; i < ENTRIES; i++)
            assertNull(cache.get(i));
    }

    /** */
    private void printStatistics(IgniteCacheProxy cache, String msg) {
        System.out.println(msg + " {{");
        cache.context().printMemoryStats();
        System.out.println("}} " + msg);
    }
}
