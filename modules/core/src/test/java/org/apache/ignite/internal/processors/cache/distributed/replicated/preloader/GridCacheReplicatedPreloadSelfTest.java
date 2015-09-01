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

package org.apache.ignite.internal.processors.cache.distributed.replicated.preloader;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.affinity.AffinityFunction;
import org.apache.ignite.cache.affinity.AffinityFunctionContext;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.cache.GridCacheEntryEx;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.spi.eventstorage.memory.MemoryEventStorageSpi;
import org.apache.ignite.testframework.GridTestClassLoader;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.CacheRebalanceMode.ASYNC;
import static org.apache.ignite.cache.CacheRebalanceMode.SYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.configuration.DeploymentMode.CONTINUOUS;
import static org.apache.ignite.events.EventType.EVT_CACHE_REBALANCE_STARTED;
import static org.apache.ignite.events.EventType.EVT_CACHE_REBALANCE_STOPPED;

/**
 * Tests for replicated cache preloader.
 */
public class GridCacheReplicatedPreloadSelfTest extends GridCommonAbstractTest {
    /** */
    private CacheRebalanceMode preloadMode = ASYNC;

    /** */
    private int batchSize = 4096;

    /** */
    private int poolSize = 2;

    /** */
    private TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(disco);

        cfg.setCacheConfiguration(cacheConfiguration(gridName));
        cfg.setPeerClassLoadingLocalClassPathExclude(GridCacheReplicatedPreloadSelfTest.class.getName(),
            TestValue.class.getName());

        cfg.setDeploymentMode(CONTINUOUS);

        cfg.setUserAttributes(F.asMap("EVEN", !gridName.endsWith("0") && !gridName.endsWith("2")));

        MemoryEventStorageSpi spi = new MemoryEventStorageSpi();

        spi.setExpireCount(50_000);

        cfg.setEventStorageSpi(spi);

        return cfg;
    }

    /**
     * Gets cache configuration for grid with specified name.
     *
     * @param gridName Grid name.
     * @return Cache configuration.
     */
    CacheConfiguration cacheConfiguration(String gridName) {
        CacheConfiguration cacheCfg = defaultCacheConfiguration();

        cacheCfg.setCacheMode(REPLICATED);
        cacheCfg.setWriteSynchronizationMode(FULL_SYNC);
        cacheCfg.setRebalanceMode(preloadMode);
        cacheCfg.setRebalanceBatchSize(batchSize);
        cacheCfg.setRebalanceThreadPoolSize(poolSize);

        return cacheCfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testSingleNode() throws Exception {
        preloadMode = SYNC;

        try {
            startGrid(1);
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testSingleZeroPoolSize() throws Exception {
        preloadMode = SYNC;
        poolSize = 0;

        try {
            startGrid(1);

            assert false : "Grid should have been failed to start.";
        }
        catch (IgniteCheckedException e) {
            info("Caught expected exception: " + e);
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If test failed.
     */
    public void testIntegrity() throws Exception {
        preloadMode = SYNC;

        try {
            Ignite g1 = startGrid(1);

            GridCacheAdapter<Integer, String> cache1 = ((IgniteKernal)g1).internalCache(null);

            cache1.getAndPut(1, "val1");
            cache1.getAndPut(2, "val2");

            GridCacheEntryEx e1 = cache1.peekEx(1);

            assert e1 != null;

            Ignite g2 = startGrid(2);

            Collection<Event> evts = null;

            for (int i = 0; i < 3; i++) {
                evts = g2.events().localQuery(F.<Event>alwaysTrue(),
                        EVT_CACHE_REBALANCE_STARTED, EVT_CACHE_REBALANCE_STOPPED);

                if (evts.size() != 2) {
                    info("Wrong events collection size (will retry in 1000 ms): " + evts.size());

                    Thread.sleep(1000);
                }
                else
                    break;
            }

            assert evts != null && evts.size() == 2 : "Wrong events received: " + evts;

            Iterator<Event> iter = evts.iterator();

            assertEquals(EVT_CACHE_REBALANCE_STARTED, iter.next().type());
            assertEquals(EVT_CACHE_REBALANCE_STOPPED, iter.next().type());

            IgniteCache<Integer, String> cache2 = g2.cache(null);

            assertEquals("val1", cache2.localPeek(1, CachePeekMode.ONHEAP));
            assertEquals("val2", cache2.localPeek(2, CachePeekMode.ONHEAP));

            GridCacheAdapter<Integer, String> cacheAdapter2 = ((IgniteKernal)g2).internalCache(null);

            GridCacheEntryEx e2 = cacheAdapter2.peekEx(1);

            assert e2 != null;
            assert e2 != e1;

            assert e2.version() != null;

            assertEquals(e1.version(), e2.version());
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If test failed.
     */
    public void testDeployment() throws Exception {
        preloadMode = SYNC;

        try {
            Ignite g1 = startGrid(1);
            Ignite g2 = startGrid(2);

            IgniteCache<Integer, Object> cache1 = g1.cache(null);
            IgniteCache<Integer, Object> cache2 = g2.cache(null);

            ClassLoader ldr = new GridTestClassLoader(
                GridCacheReplicatedPreloadSelfTest.class.getName(),
                TestValue.class.getName(),
                TestAffinityFunction.class.getName());

            Object v1 = ldr.loadClass(TestValue.class.getName()).newInstance();

            cache1.put(1, v1);

            info("Stored value in cache1 [v=" + v1 + ", ldr=" + v1.getClass().getClassLoader() + ']');

            Object v2 = cache2.get(1);

            info("Read value from cache2 [v=" + v2 + ", ldr=" + v2.getClass().getClassLoader() + ']');

            assert v2 != null;
            assert v2.toString().equals(v1.toString());
            assert !v2.getClass().getClassLoader().equals(getClass().getClassLoader());
            assert v2.getClass().getClassLoader().getClass().getName().contains("GridDeploymentClassLoader");

            stopGrid(1);

            Ignite g3 = startGrid(3);

            IgniteCache<Integer, Object> cache3 = g3.cache(null);

            Object v3 = cache3.localPeek(1, CachePeekMode.ONHEAP);

            assert v3 != null;

            info("Read value from cache3 [v=" + v3 + ", ldr=" + v3.getClass().getClassLoader() + ']');

            assert v3 != null;
            assert v3.toString().equals(v1.toString());
            assert !v3.getClass().getClassLoader().equals(getClass().getClassLoader());
            assert v3.getClass().getClassLoader().getClass().getName().contains("GridDeploymentClassLoader");
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If test failed.
     */
    public void testSync() throws Exception {
        preloadMode = SYNC;
        batchSize = 512;

        try {
            IgniteCache<Integer, String> cache1 = startGrid(1).cache(null);

            int keyCnt = 1000;

            for (int i = 0; i < keyCnt; i++)
                cache1.put(i, "val" + i);

            IgniteCache<Integer, String> cache2 = startGrid(2).cache(null);

            assertEquals(keyCnt, cache2.localSize(CachePeekMode.ALL));
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If test failed.
     */
    public void testAsync() throws Exception {
        preloadMode = ASYNC;
        batchSize = 256;

        try {
            IgniteCache<Integer, String> cache1 = startGrid(1).cache(null);

            int keyCnt = 2000;

            for (int i = 0; i < keyCnt; i++)
                cache1.put(i, "val" + i);

            IgniteCache<Integer, String> cache2 = startGrid(2).cache(null);

            int size = cache2.localSize(CachePeekMode.ALL);

            info("Size of cache2: " + size);

            assert waitCacheSize(cache2, keyCnt, getTestTimeout()) :
                "Actual cache size: " + cache2.localSize(CachePeekMode.ALL);
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @param cache Cache.
     * @param expSize Lower bound of expected size.
     * @param timeout Timeout.
     * @return {@code true} if success.
     * @throws InterruptedException If thread was interrupted.
     */
    @SuppressWarnings({"BusyWait"})
    private boolean waitCacheSize(IgniteCache<Integer, String> cache, int expSize, long timeout)
        throws InterruptedException {
        assert cache != null;
        assert expSize > 0;
        assert timeout >= 0;

        long end = System.currentTimeMillis() + timeout;

        while (cache.localSize(CachePeekMode.ALL) < expSize) {
            Thread.sleep(50);

            if (end - System.currentTimeMillis() <= 0)
                break;
        }

        return cache.localSize(CachePeekMode.ALL) >= expSize;
    }

    /**
     * @throws Exception If test failed.
     */
    public void testBatchSize1() throws Exception {
        preloadMode = SYNC;
        batchSize = 1; // 1 byte but one entry should be in batch anyway.

        try {
            IgniteCache<Integer, String> cache1 = startGrid(1).cache(null);

            int cnt = 100;

            for (int i = 0; i < cnt; i++)
                cache1.put(i, "val" + i);

            IgniteCache<Integer, String> cache2 = startGrid(2).cache(null);

            assertEquals(cnt, cache2.localSize(CachePeekMode.ALL));
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If test failed.
     */
    public void testBatchSize1000() throws Exception {
        preloadMode = SYNC;
        batchSize = 1000; // 1000 bytes.

        try {
            IgniteCache<Integer, String> cache1 = startGrid(1).cache(null);

            int cnt = 100;

            for (int i = 0; i < cnt; i++)
                cache1.put(i, "val" + i);

            IgniteCache<Integer, String> cache2 = startGrid(2).cache(null);

            assertEquals(cnt, cache2.localSize(CachePeekMode.ALL));
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If test failed.
     */
    public void testBatchSize10000() throws Exception {
        preloadMode = SYNC;
        batchSize = 10000; // 10000 bytes.

        try {
            IgniteCache<Integer, String> cache1 = startGrid(1).cache(null);

            int cnt = 100;

            for (int i = 0; i < cnt; i++)
                cache1.put(i, "val" + i);

            IgniteCache<Integer, String> cache2 = startGrid(2).cache(null);

            assertEquals(cnt, cache2.localSize(CachePeekMode.ALL));
        }
        finally {
            stopGrid(1);
            stopGrid(2);
        }
    }

    /**
     * @throws Exception If test failed.
     */
    public void testMultipleNodes() throws Exception {
        preloadMode = ASYNC;
        batchSize = 256;

        try {
            int gridCnt = 4;

            startGridsMultiThreaded(gridCnt);

            info("Beginning data population...");

            int cnt = 2500;

            Map<Integer, String> map = null;

            for (int i = 0; i < cnt; i++) {
                if (i % 100 == 0) {
                    if (map != null && !map.isEmpty()) {
                        grid(0).cache(null).putAll(map);

                        info("Put entries count: " + i);
                    }

                    map = new HashMap<>();
                }

                map.put(i, "val" + i);
            }

            if (map != null && !map.isEmpty())
                grid(0).cache(null).putAll(map);

            for (int gridIdx = 0; gridIdx < gridCnt; gridIdx++) {
                assert grid(gridIdx).cache(null).localSize(CachePeekMode.ALL) == cnt :
                    "Actual size: " + grid(gridIdx).cache(null).localSize(CachePeekMode.ALL);

                info("Cache size is OK for grid index: " + gridIdx);
            }

            IgniteCache<Integer, String> lastCache = startGrid(gridCnt).cache(null);

            // Let preloading start.
            Thread.sleep(1000);

            // Stop random initial node while preloading is in progress.
            int idx = new Random().nextInt(gridCnt);

            info("Stopping node with index: " + idx);

            stopGrid(idx);

            assert waitCacheSize(lastCache, cnt, 20 * 1000) :
                "Actual cache size: " + lastCache.localSize(CachePeekMode.ALL);
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If test failed.
     */
    public void testConcurrentStartSync() throws Exception {
        preloadMode = SYNC;
        batchSize = 10000;

        try {
            startGridsMultiThreaded(4);
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If test failed.
     */
    public void testConcurrentStartAsync() throws Exception {
        preloadMode = ASYNC;
        batchSize = 10000;

        try {
            startGridsMultiThreaded(4);
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     *
     */
    @SuppressWarnings({"PublicInnerClass"})
    public static class TestValue implements Serializable {
        /** */
        private String val = "test-" + System.currentTimeMillis();

        /**
         * @return Value
         */
        public String getValue() {
            return val;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(TestValue.class, this);
        }
    }

    /**
     * Test affinity.
     */
    @SuppressWarnings({"PublicInnerClass"})
    private static class TestAffinityFunction implements AffinityFunction {
        /** {@inheritDoc} */
        @Override public int partitions() {
            return 2;
        }

        /** {@inheritDoc} */
        @Override public int partition(Object key) {
            if (key instanceof Number)
                return ((Number)key).intValue() % 2;

            return key == null ? 0 : U.safeAbs(key.hashCode() % 2);
        }

        /** {@inheritDoc} */
        @Override public List<List<ClusterNode>> assignPartitions(AffinityFunctionContext affCtx) {
            List<List<ClusterNode>> res = new ArrayList<>(partitions());

            for (int part = 0; part < partitions(); part++)
                res.add(nodes(part, affCtx.currentTopologySnapshot()));

            return res;
        }

        /** {@inheritDoc} */
        @SuppressWarnings({"RedundantTypeArguments"})
        public List<ClusterNode> nodes(int part, Collection<ClusterNode> nodes) {
            Collection<ClusterNode> col = new HashSet<>(nodes);

            if (col.size() <= 1)
                return new ArrayList<>(col);

            for (Iterator<ClusterNode> iter = col.iterator(); iter.hasNext(); ) {
                ClusterNode node = iter.next();

                boolean even = node.<Boolean>attribute("EVEN");

                if ((even && part != 0) || (!even && part != 1))
                    iter.remove();
            }

            return new ArrayList<>(col);
        }

        /** {@inheritDoc} */
        @Override public void reset() {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void removeNode(UUID nodeId) {
            // No-op.
        }
    }
}