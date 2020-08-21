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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.IgniteCache;
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
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.resource.DependencyResolver;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.testframework.GridTestUtils.runAsync;

/**
 * Tests various scenarios while partition eviction is blocked.
 */
@WithSystemProperty(key = "IGNITE_PRELOAD_RESEND_TIMEOUT", value = "0")
public class BlockedEvictionsTest extends GridCommonAbstractTest {
    /** */
    private boolean persistence;

    /**
     * {@inheritDoc}
     */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

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
                doSleep(1000); // Wait a bit to give some time for partition state message to process on crd.

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
                doSleep(1000); // Wait a bit to give some time for partition state message to process on crd.

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
    public void testCacheGroupDestroy_Volatile() throws Exception {
        AtomicReference<IgniteInternalFuture> ref = new AtomicReference<>();

        testOperationDuringEviction(false, 1, new Runnable() {
            @Override public void run() {
                doSleep(1000); // Wait a bit to give some time for partition state message to process on crd.

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

        Map evictionGroupsMap = U.field(mgr, "evictionGroupsMap");

        assertEquals(1, evictionGroupsMap.size());

        IgniteCache<Object, Object> cache = grid(0).getOrCreateCache(cacheConfiguration());

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
                doSleep(1000); // Wait a bit to give some time for partition state message to process on crd.

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
                        @Override public GridDhtLocalPartition create(GridCacheSharedContext ctx, CacheGroupContext grp, int id, boolean recovery) {
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
            setAffinity(new RendezvousAffinityFunction(false, persistence ? 64 : 1024));
    }
}
