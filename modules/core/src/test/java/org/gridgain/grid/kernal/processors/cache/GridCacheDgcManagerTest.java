/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.affinity.consistenthash.*;
import org.gridgain.grid.cache.query.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.resources.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.testframework.junits.common.*;

import java.util.*;

import static org.gridgain.grid.GridLifecycleEventType.*;
import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.gridgain.grid.cache.GridCacheDistributionMode.*;
import static org.gridgain.grid.cache.GridCachePreloadMode.*;
import static org.gridgain.grid.cache.GridCacheTxConcurrency.*;
import static org.gridgain.grid.cache.GridCacheTxIsolation.*;
import static org.gridgain.grid.cache.GridCacheWriteSynchronizationMode.*;

/**
 * Cache DGC test.
 */
public class GridCacheDgcManagerTest extends GridCommonAbstractTest {
    /** */
    private static final int BATCH_SIZE  = 500;

    /** */
    private static final int ENTRY_COUNT = 1000;

    /** */
    private static final String TEST_STRING = repeat("ABC,", 6250); // 25K chars

    /** */
    private static final int NODES_CNT = 0;

    /** */
    private static final GridTcpDiscoveryIpFinder ipFinder = new GridTcpDiscoveryVmIpFinder(true);

    /** */
    @SuppressWarnings( {"NonConstantFieldWithUpperCaseName"})
    private static boolean TEST_INDEXED;

    /** {@inheritDoc} */
    @SuppressWarnings({"ConstantConditions"})
    @Override protected void beforeTest() throws Exception {
        if (NODES_CNT > 0)
            startGridsMultiThreaded(NODES_CNT);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected GridConfiguration getConfiguration(String gridName) throws Exception {
        GridConfiguration cfg = super.getConfiguration(gridName);

        GridCacheConfiguration partCacheCfg = defaultCacheConfiguration();

        partCacheCfg.setCacheMode(PARTITIONED);
        partCacheCfg.setBackups(1);
        partCacheCfg.setPreloadMode(SYNC);
        partCacheCfg.setDefaultTxConcurrency(PESSIMISTIC);
        partCacheCfg.setDefaultTxIsolation(READ_COMMITTED);
        partCacheCfg.setWriteSynchronizationMode(FULL_SYNC);
        partCacheCfg.setDistributionMode(PARTITIONED_ONLY);
        partCacheCfg.setStore(null);
        partCacheCfg.setSwapEnabled(false);
        partCacheCfg.setPreloadBatchSize(1000000);
        partCacheCfg.setDgcSuspectLockTimeout(5000);

        cfg.setCacheConfiguration(partCacheCfg);

        GridTcpDiscoverySpi discoSpi = new GridTcpDiscoverySpi();

        discoSpi.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(discoSpi);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testDgcNonIndexed() throws Exception {
        GridConfiguration cfg = getConfiguration("test");

        cfg.setLifecycleBeans(new GridCacheLoaderLifeCycleBean());

        final Grid g = G.start(cfg);

        Integer res = g.compute().call(new GridCallable<Integer>() {
            @Override public Integer call() throws GridException {
                GridCache<MyKey, MyValue> cache = g.cache(null);

                GridCacheQuery<Map.Entry<MyKey, MyValue>> q = cache.queries().createScanQuery(null);

                return F.sumInt(q.execute(new GridReducer<Map.Entry<MyKey, MyValue>, Integer>() {
                    /** */
                    private int cnt_;

                    @Override public boolean collect(Map.Entry<MyKey, MyValue> e) {
                        cnt_++;
                        return true;
                    }

                    @Override public Integer reduce() {
                        return cnt_;
                    }
                }).get());
            }
        });

        assertEquals(1000, res.intValue());
    }

    /**
     * @throws Exception If failed.
     */
    public void testDgcIndexed() throws Exception {
        GridConfiguration cfg = getConfiguration("test");

        cfg.setLifecycleBeans(new GridCacheLoaderLifeCycleBean());

        TEST_INDEXED = true;

        final Grid g = G.start(cfg);

        Integer res = g.compute().call(new GridCallable<Integer>() {
            @Override public Integer call() throws GridException {
                GridCache<MyKey, MyValueIndexed> cache = g.cache(null);

                GridCacheQuery<Map.Entry<MyKey, MyValueIndexed>> q = cache.queries().createScanQuery(null);

                return F.sumInt(q.execute(new GridReducer<Map.Entry<MyKey, MyValueIndexed>, Integer>() {
                    /** */
                    private int cnt_;

                    @Override public boolean collect(Map.Entry<MyKey, MyValueIndexed> e) {
                        cnt_++;

                        return true;
                    }

                    @Override public Integer reduce() {
                        return cnt_;
                    }
                }).get());
            }
        });

        assertEquals(1000, res.intValue());
    }

    /**
     * @param str String to repeat.
     * @param cnt Count.
     * @return Long string.
     */
    public static String repeat(String str, int cnt) {
        // If this multiplication overflows, a NegativeArraySizeException or
        // OutOfMemoryError is not far behind
        StringBuilder builder = new StringBuilder(str.length() * cnt);

        for (int i = 0; i < cnt; i++)
            builder.append(str);

        return builder.toString();
    }


    /**
     * Cache DGC test.
     */
    private static class GridCacheLoaderLifeCycleBean implements GridLifecycleBean {
        /** */
        @GridLoggerResource
        private GridLogger log;

        /** */
        @GridInstanceResource
        private Grid grid;

        /** {@inheritDoc} */
        @Override public void onLifecycleEvent(GridLifecycleEventType evt) throws GridException {
            if (grid.configuration().isDaemon())
                return;

            try {
                if (evt == AFTER_GRID_START)
                    populateCache();
            }
            catch (Exception e) {
                throw new GridException(e);
            }
        }

        /**
         * @throws Exception If failed.
         */
        private void populateCache() throws Exception {
            log.info("Populating cache data...");

            // These are two slightly different use cases:
            // - in the first one, we populate the cache with values that have no indexed fields
            // - in the second one, we populate the cache with values with indexed fields
            //
            // You can run either one or another to see the problem. The second case is especially
            // broken.

            if (TEST_INDEXED)
                // populates the cache with values for which indexing is required
                populateCacheIndexed(grid);
            else
                // populates the cache with non-indexed values
                populateCache(grid);

            // this sleep() simulates some additional long running work that takes place after the
            // cache has been populated
            Thread.sleep(20000);

            // Observe that at this point the cache size is different from the one reported by
            // populateCache() above.
            // The effect is even more pronounced with populateCacheIndexed(...)
            log.info("Current cache size " + grid.cache(null).size());
        }

        /**
         * @param grid Grid.
         * @throws Exception If failed.
         */
        @SuppressWarnings("unchecked")
        private void populateCache(Grid grid) throws Exception {
            GridCache<MyKey, MyValue> myCache = grid.cache(null);
            // not sure if this is required:
            myCache.flagsOff(GridCacheFlag.SKIP_STORE, GridCacheFlag.SKIP_SWAP);

            Map<MyKey, MyValue> batch = new HashMap<>(BATCH_SIZE);

            GridFuture<?> fut = null;

            for (int i = 0; i < ENTRY_COUNT; i++) {
                batch.put(new MyKey(i), new MyValue(TEST_STRING + i));

                if (i % BATCH_SIZE == 0) {
                    if (fut != null)
                        fut.get();

                    fut = myCache.putAllAsync(batch);

                    batch = new HashMap<>(BATCH_SIZE);
                }
            }

            if (!batch.isEmpty())
                fut = myCache.putAllAsync(batch);

            assert fut != null;

            fut.get();

            batch.clear();

            log.info("Loaded " + grid.cache(null).size() + " entries into the cache.");
        }

        /**
         * @param grid Grid.
         * @throws Exception If failed.
         */
        @SuppressWarnings("unchecked")
        private void populateCacheIndexed(Grid grid) throws Exception {
            GridCache<MyKey, MyValueIndexed> myCache = grid.cache(null);
            // not sure if this is required:
            myCache.flagsOff(GridCacheFlag.SKIP_STORE, GridCacheFlag.SKIP_SWAP);

            Map<MyKey, MyValueIndexed> batch = new HashMap<>(BATCH_SIZE);

            GridFuture<?> fut = null;

            for (int i = 0; i < ENTRY_COUNT; i++) {
                batch.put(new MyKey(i), new MyValueIndexed(TEST_STRING + i));

                if (i % BATCH_SIZE == 0) {
                    if (fut != null)
                        fut.get();

                    fut = myCache.putAllAsync(batch);

                    batch = new HashMap<>(BATCH_SIZE);
                }
            }

            if (!batch.isEmpty())
                fut = myCache.putAllAsync(batch);

            assert fut != null;

            fut.get();

            batch.clear();

            // Depending on the computer speed the cache size may differ from the number of elements
            // actually inserted
            log.info("Loaded " + ENTRY_COUNT + " entries, but the cache size is " +
                grid.cache(null).size());
        }
    }

    /**
     *
     */
    private static class MyKey {
        /** */
        private final int id;

        /**
         * @param id ID.
         */
        MyKey(int id) {
            this.id = id;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return id;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object obj) {
            if (this == obj)
                return true;

            if (obj == null)
                return false;

            if (getClass() != obj.getClass())
                return false;

            MyKey other = (MyKey) obj;

            return id == other.id;
        }
    }

    /**
     *
     */
    private static class MyValueIndexed {
        /** */
        @GridCacheQueryTextField
        @SuppressWarnings( {"UnusedDeclaration"})
        private final String data;

        /**
         * @param data Data.
         */
        MyValueIndexed(String data) {
            this.data = data;
        }
    }

    /**
     *
     */
    private static class MyValue {
        /** */
        @SuppressWarnings( {"UnusedDeclaration"})
        private final String data;

        /**
         * @param data Data.
         */
        MyValue(String data) {
            this.data = data;
        }
    }
}
