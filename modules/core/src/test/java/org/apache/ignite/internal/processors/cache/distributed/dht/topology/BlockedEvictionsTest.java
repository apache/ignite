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

package org.apache.ignite.internal.processors.cache.distributed.dht.topology;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteFutureTimeoutCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.NodeStoppingException;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.CacheMetricsImpl;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.IgniteCacheOffheapManager;
import org.apache.ignite.internal.processors.resource.DependencyResolver;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import static org.apache.ignite.testframework.GridTestUtils.runAsync;

/**
 * Tests various scenarios while partition eviction is blocked.
 */
public class BlockedEvictionsTest extends GridCommonAbstractTest {
    /** */
    private boolean persistence;

    /** */
    private boolean stats;

    /** */
    private int sysPoolSize;

    /**
     * {@inheritDoc}
     */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setRebalanceThreadPoolSize(ThreadLocalRandom.current().nextInt(3) + 2);
        cfg.setSystemThreadPoolSize(sysPoolSize);
        cfg.setConsistentId(igniteInstanceName);

        if (persistence) {
            DataStorageConfiguration dsCfg = new DataStorageConfiguration().setWalSegmentSize(4 * 1024 * 1024);
            dsCfg.getDefaultDataRegionConfiguration().setPersistenceEnabled(persistence).setMaxSize(100 * 1024 * 1024);
            cfg.setDataStorageConfiguration(dsCfg);
        }

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();

        stats = false;
        sysPoolSize = IgniteConfiguration.DFLT_SYSTEM_CORE_THREAD_CNT;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     * Stopping the cache during clearing should be no-op, all partitions are expected to be cleared.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testStopCache_Volatile() throws Exception {
        testOperationDuringEviction(false, 1, new Runnable() {
            @Override public void run() {
                grid(0).cache(DEFAULT_CACHE_NAME).close();
            }
        });

        awaitPartitionMapExchange(true, true, null);
        assertPartitionsSame(idleVerify(grid(0), DEFAULT_CACHE_NAME));
    }

    /**
     * Stopping the cache during clearing should be no-op, all partitions are expected to be cleared.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testStopCache_Persistence() throws Exception {
        testOperationDuringEviction(true, 1, new Runnable() {
            @Override public void run() {
                grid(0).cache(DEFAULT_CACHE_NAME).close();
            }
        });

        awaitPartitionMapExchange(true, true, null);
        assertPartitionsSame(idleVerify(grid(0), DEFAULT_CACHE_NAME));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDeactivation_Volatile() throws Exception {
        AtomicReference<IgniteInternalFuture> ref = new AtomicReference<>();

        testOperationDuringEviction(false, 1, new Runnable() {
            @Override public void run() {
                IgniteInternalFuture fut = runAsync(() -> grid(0).cluster().state(ClusterState.INACTIVE));

                ref.set(fut);

                doSleep(1000);

                assertFalse(fut.isDone());
            }
        });

        ref.get().get();

        assertTrue(grid(0).cluster().state() == ClusterState.INACTIVE);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDeactivation_Persistence() throws Exception {
        AtomicReference<IgniteInternalFuture> ref = new AtomicReference<>();

        testOperationDuringEviction(true, 1, new Runnable() {
            @Override public void run() {
                IgniteInternalFuture fut = runAsync(() -> grid(0).cluster().state(ClusterState.INACTIVE));

                ref.set(fut);

                doSleep(1000);

                assertFalse(fut.isDone());
            }
        });

        ref.get().get();

        assertTrue(grid(0).cluster().state() == ClusterState.INACTIVE);
    }

    /**
     * Checks if a failure handler is called if a clearing throws unexpected exception.
     */
    @Test
    public void testFailureHandler() {

    }

    /** */
    @Test
    public void testEvictionMetrics() throws Exception {
        stats = true;

        testOperationDuringEviction(true, 1, new Runnable() {
            @Override public void run() {
                CacheMetricsImpl metrics = grid(0).cachex(DEFAULT_CACHE_NAME).context().cache().metrics0();

                assertTrue(metrics.evictingPartitionsLeft() > 0);
            }
        });

        awaitPartitionMapExchange(true, true, null);
        CacheMetricsImpl metrics = grid(0).cachex(DEFAULT_CACHE_NAME).context().cache().metrics0();
        assertTrue(metrics.evictingPartitionsLeft() == 0);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSysPoolStarvation() throws Exception {
        sysPoolSize = 1;

        testOperationDuringEviction(true, 1, new Runnable() {
            @Override public void run() {
                try {
                    grid(0).context().closure().runLocalSafe(new Runnable() {
                        @Override public void run() {
                            // No-op.
                        }
                    }, true).get(5_000);
                } catch (IgniteCheckedException e) {
                    fail(X.getFullStackTrace(e));
                }
            }
        });

        awaitPartitionMapExchange(true, true, null);
        assertPartitionsSame(idleVerify(grid(0), DEFAULT_CACHE_NAME));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCacheGroupDestroy_Volatile() throws Exception {
        AtomicReference<IgniteInternalFuture> ref = new AtomicReference<>();

        testOperationDuringEviction(false, 1, new Runnable() {
            @Override public void run() {
                IgniteInternalFuture fut = runAsync(new Runnable() {
                    @Override public void run() {
                        grid(0).destroyCache(DEFAULT_CACHE_NAME);
                    }
                });

                doSleep(500);

                assertFalse(fut.isDone()); // Cache stop should be blocked by concurrent unfinished eviction.

                ref.set(fut);
            }
        });

        try {
            ref.get().get(10_000);
        } catch (IgniteFutureTimeoutCheckedException e) {
            fail(X.getFullStackTrace(e));
        }

        PartitionsEvictManager mgr = grid(0).context().cache().context().evict();

        // Group eviction context should remain in map.
        Map evictionGroupsMap = U.field(mgr, "evictionGroupsMap");

        assertEquals("Group context must be cleaned up", 0, evictionGroupsMap.size());

        grid(0).getOrCreateCache(cacheConfiguration());

        assertEquals(0, evictionGroupsMap.size());

        assertPartitionsSame(idleVerify(grid(0), DEFAULT_CACHE_NAME));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testStopNodeDuringEviction() throws Exception {
        AtomicReference<IgniteInternalFuture> ref = new AtomicReference<>();

        testOperationDuringEviction(false, 1, new Runnable() {
            @Override public void run() {
                IgniteInternalFuture fut = runAsync(new Runnable() {
                    @Override public void run() {
                        grid(0).close();
                    }
                });

                doSleep(500);

                assertFalse(fut.isDone()); // Cache stop should be blocked by concurrent unfinished eviction.

                ref.set(fut);
            }
        });

        try {
            ref.get().get(10_000);
        } catch (IgniteFutureTimeoutCheckedException e) {
            fail(X.getFullStackTrace(e));
        }

        waitForTopology(2);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testStopNodeDuringEviction_2() throws Exception {
        AtomicInteger holder = new AtomicInteger();

        CountDownLatch l1 = new CountDownLatch(1);
        CountDownLatch l2 = new CountDownLatch(1);

        IgniteEx g0 = startGrid(0, new DependencyResolver() {
            @Override public <T> T resolve(T instance) {
                if (instance instanceof GridDhtPartitionTopologyImpl) {
                    GridDhtPartitionTopologyImpl top = (GridDhtPartitionTopologyImpl) instance;

                    top.partitionFactory(new GridDhtPartitionTopologyImpl.PartitionFactory() {
                        @Override public GridDhtLocalPartition create(
                            GridCacheSharedContext ctx,
                            CacheGroupContext grp,
                            int id,
                            boolean recovery
                        ) {
                            return new GridDhtLocalPartitionSyncEviction(ctx, grp, id, recovery, 3, l1, l2) {
                                /** */
                                @Override protected void sync() {
                                    if (holder.get() == id)
                                        super.sync();
                                }
                            };
                        }
                    });
                }
                else if (instance instanceof IgniteCacheOffheapManager) {
                    IgniteCacheOffheapManager mgr = (IgniteCacheOffheapManager) instance;

                    IgniteCacheOffheapManager spied = Mockito.spy(mgr);

                    Mockito.doAnswer(new Answer() {
                        @Override public Object answer(InvocationOnMock invocation) throws Throwable {
                            Object ret = invocation.callRealMethod();

                            // Wait is necessary here to guarantee test progress.
                            doSleep(2_000);

                            return ret;
                        }
                    }).when(spied).stop();

                    return (T) spied;
                }

                return instance;
            }
        });

        startGrid(1);

        awaitPartitionMapExchange();

        IgniteCache<Object, Object> cache = g0.getOrCreateCache(cacheConfiguration());

        int p0 = evictingPartitionsAfterJoin(g0, cache, 1).get(0);
        holder.set(p0);

        loadDataToPartition(p0, g0.name(), DEFAULT_CACHE_NAME, 5_000, 0, 3);

        startGrid(2);

        U.awaitQuiet(l1);

        GridDhtLocalPartition part = g0.cachex(DEFAULT_CACHE_NAME).context().topology().localPartition(p0);

        AtomicReference<GridFutureAdapter<?>> ref = U.field(part, "finishFutRef");

        GridFutureAdapter<?> finishFut = ref.get();

        IgniteInternalFuture fut = runAsync(g0::close);

        // Give some time to execute cache store destroy.
        doSleep(500);

        l2.countDown();

        fut.get();

        // Partition clearing future should be finished with NodeStoppingException.
        assertTrue(finishFut.error().getMessage(),
            finishFut.error() != null && X.hasCause(finishFut.error(), NodeStoppingException.class));
    }

    /**
     * @param persistence {@code True} to use persistence.
     * @param mode        Mode: <ul><li>0 - block before clearing start</li>
     *                    <li>1 - block in the middle of clearing</li></ul>
     * @param r           A runnable to run while eviction is blocked.
     * @throws Exception If failed.
     */
    protected void testOperationDuringEviction(boolean persistence, int mode, Runnable r) throws Exception {
        this.persistence = persistence;

        AtomicInteger holder = new AtomicInteger();
        CountDownLatch l1 = new CountDownLatch(1);
        CountDownLatch l2 = new CountDownLatch(1);

        IgniteEx g0 = startGrid(0, new DependencyResolver() {
            @Override public <T> T resolve(T instance) {
                if (instance instanceof GridDhtPartitionTopologyImpl) {
                    GridDhtPartitionTopologyImpl top = (GridDhtPartitionTopologyImpl) instance;

                    top.partitionFactory(new GridDhtPartitionTopologyImpl.PartitionFactory() {
                        @Override public GridDhtLocalPartition create(
                            GridCacheSharedContext ctx,
                            CacheGroupContext grp,
                            int id,
                            boolean recovery
                        ) {
                            return new GridDhtLocalPartitionSyncEviction(ctx, grp, id, recovery, mode, l1, l2) {
                                /** */
                                @Override protected void sync() {
                                    if (holder.get() == id)
                                        super.sync();
                                }
                            };
                        }
                    });
                }

                return instance;
            }
        });

        startGrid(1);

        if (persistence)
            g0.cluster().state(ClusterState.ACTIVE);

        awaitPartitionMapExchange(true, true, null);

        IgniteCache<Object, Object> cache = g0.getOrCreateCache(cacheConfiguration());

        List<Integer> allEvicting = evictingPartitionsAfterJoin(g0, cache, 1024);
        int p0 = allEvicting.get(0);
        holder.set(p0);

        final int cnt = 5_000;

        List<Integer> keys = partitionKeys(g0.cache(DEFAULT_CACHE_NAME), p0, cnt, 0);

        try (IgniteDataStreamer<Object, Object> ds = g0.dataStreamer(DEFAULT_CACHE_NAME)) {
            for (Integer key : keys)
                ds.addData(key, key);
        }

        IgniteEx joining = startGrid(2);

        if (persistence)
            resetBaselineTopology();

        assertTrue(U.await(l1, 30_000, TimeUnit.MILLISECONDS));

        r.run();

        l2.countDown();
    }

    /** */
    protected CacheConfiguration<Object, Object> cacheConfiguration() {
        return new CacheConfiguration<>(DEFAULT_CACHE_NAME).
            setCacheMode(CacheMode.PARTITIONED).
            setBackups(1).
            setStatisticsEnabled(stats).
            setAffinity(new RendezvousAffinityFunction(false, persistence ? 64 : 1024));
    }
}
