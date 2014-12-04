/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.distributed.replicated.preloader;

import org.apache.ignite.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.affinity.*;
import org.gridgain.grid.events.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.grid.spi.eventstorage.memory.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.testframework.*;
import org.gridgain.testframework.junits.common.*;

import java.io.*;
import java.util.*;

import static org.gridgain.grid.GridDeploymentMode.*;
import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.gridgain.grid.cache.GridCachePreloadMode.*;
import static org.gridgain.grid.cache.GridCacheWriteSynchronizationMode.*;
import static org.gridgain.grid.events.GridEventType.*;

/**
 * Tests for replicated cache preloader.
 */
public class GridCacheReplicatedPreloadSelfTest extends GridCommonAbstractTest {
    /** */
    private GridCachePreloadMode preloadMode = ASYNC;

    /** */
    private int batchSize = 4096;

    /** */
    private int poolSize = 2;

    /** */
    private GridTcpDiscoveryIpFinder ipFinder = new GridTcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected GridConfiguration getConfiguration(String gridName) throws Exception {
        GridConfiguration cfg = super.getConfiguration(gridName);

        GridTcpDiscoverySpi disco = new GridTcpDiscoverySpi();

        disco.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(disco);

        cfg.setCacheConfiguration(cacheConfiguration(gridName));
        cfg.setPeerClassLoadingLocalClassPathExclude(GridCacheReplicatedPreloadSelfTest.class.getName(),
            TestValue.class.getName());

        cfg.setDeploymentMode(CONTINUOUS);

        cfg.setUserAttributes(F.asMap("EVEN", !gridName.endsWith("0") && !gridName.endsWith("2")));

        GridMemoryEventStorageSpi spi = new GridMemoryEventStorageSpi();

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
    GridCacheConfiguration cacheConfiguration(String gridName) {
        GridCacheConfiguration cacheCfg = defaultCacheConfiguration();

        cacheCfg.setCacheMode(REPLICATED);
        cacheCfg.setWriteSynchronizationMode(FULL_SYNC);
        cacheCfg.setPreloadMode(preloadMode);
        cacheCfg.setPreloadBatchSize(batchSize);
        cacheCfg.setPreloadThreadPoolSize(poolSize);

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
        catch (GridException e) {
            info("Caught expected exception: " + e);
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If test failed.
     */
    @SuppressWarnings({"BusyWait"})
    public void _testIntegrity() throws Exception { // TODO GG-9141
        preloadMode = SYNC;

        try {
            Ignite g1 = startGrid(1);

            GridCache<Integer, String> cache1 = g1.cache(null);

            cache1.put(1, "val1");
            cache1.put(2, "val2");

            GridCacheEntry<Integer, String> e1 = cache1.entry(1);

            assert e1 != null;

            Ignite g2 = startGrid(2);

            Collection<GridEvent> evts = null;

            for (int i = 0; i < 3; i++) {
                evts = g2.events().localQuery(F.<GridEvent>alwaysTrue(),
                    EVT_CACHE_PRELOAD_STARTED, EVT_CACHE_PRELOAD_STOPPED);

                if (evts.size() != 2) {
                    info("Wrong events collection size (will retry in 1000 ms).");

                    Thread.sleep(1000);
                }
                else
                    break;
            }

            assert evts != null && evts.size() == 2 : "Wrong events received: " + evts;

            Iterator<GridEvent> iter = evts.iterator();

            assertEquals(EVT_CACHE_PRELOAD_STARTED, iter.next().type());
            assertEquals(EVT_CACHE_PRELOAD_STOPPED, iter.next().type());

            GridCache<Integer, String> cache2 = g2.cache(null);

            assertEquals("val1", cache2.peek(1));
            assertEquals("val2", cache2.peek(2));

            GridCacheEntry<Integer, String> e2 = cache2.entry(1);

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

            GridCache<Integer, Object> cache1 = g1.cache(null);
            GridCache<Integer, Object> cache2 = g2.cache(null);

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

            GridCache<Integer, Object> cache3 = g3.cache(null);

            Object v3 = cache3.peek(1);

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
            GridCache<Integer, String> cache1 = startGrid(1).cache(null);

            int keyCnt = 1000;

            for (int i = 0; i < keyCnt; i++)
                cache1.put(i, "val" + i);

            GridCache<Integer, String> cache2 = startGrid(2).cache(null);

            assertEquals(keyCnt, cache2.size());
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
            GridCache<Integer, String> cache1 = startGrid(1).cache(null);

            int keyCnt = 2000;

            for (int i = 0; i < keyCnt; i++)
                cache1.put(i, "val" + i);

            GridCache<Integer, String> cache2 = startGrid(2).cache(null);

            int size = cache2.size();

            info("Size of cache2: " + size);

            assert waitCacheSize(cache2, keyCnt, getTestTimeout()) : "Actual cache size: " + cache2.size();
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
    private boolean waitCacheSize(GridCacheProjection<Integer, String> cache, int expSize, long timeout)
        throws InterruptedException {
        assert cache != null;
        assert expSize > 0;
        assert timeout >= 0;

        long end = System.currentTimeMillis() + timeout;

        while (cache.size() < expSize) {
            Thread.sleep(50);

            if (end - System.currentTimeMillis() <= 0)
                break;
        }

        return cache.size() >= expSize;
    }

    /**
     * @throws Exception If test failed.
     */
    public void testBatchSize1() throws Exception {
        preloadMode = SYNC;
        batchSize = 1; // 1 byte but one entry should be in batch anyway.

        try {
            GridCache<Integer, String> cache1 = startGrid(1).cache(null);

            int cnt = 100;

            for (int i = 0; i < cnt; i++)
                cache1.put(i, "val" + i);

            GridCache<Integer, String> cache2 = startGrid(2).cache(null);

            assertEquals(cnt, cache2.size());
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
            GridCache<Integer, String> cache1 = startGrid(1).cache(null);

            int cnt = 100;

            for (int i = 0; i < cnt; i++)
                cache1.put(i, "val" + i);

            GridCache<Integer, String> cache2 = startGrid(2).cache(null);

            assertEquals(cnt, cache2.size());
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
            GridCache<Integer, String> cache1 = startGrid(1).cache(null);

            int cnt = 100;

            for (int i = 0; i < cnt; i++)
                cache1.put(i, "val" + i);

            GridCache<Integer, String> cache2 = startGrid(2).cache(null);

            assertEquals(cnt, cache2.size());
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
                assert grid(gridIdx).cache(null).size() == cnt : "Actual size: " + grid(gridIdx).cache(null).size();

                info("Cache size is OK for grid index: " + gridIdx);
            }

            GridCache<Integer, String> lastCache = startGrid(gridCnt).cache(null);

            // Let preloading start.
            Thread.sleep(1000);

            // Stop random initial node while preloading is in progress.
            int idx = new Random().nextInt(gridCnt);

            info("Stopping node with index: " + idx);

            stopGrid(idx);

            assert waitCacheSize(lastCache, cnt, 20 * 1000) : "Actual cache size: " + lastCache.size();
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
    private static class TestAffinityFunction implements GridCacheAffinityFunction {
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
        @Override public List<List<ClusterNode>> assignPartitions(GridCacheAffinityFunctionContext affCtx) {
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
