/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache;

import org.apache.ignite.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.lang.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.query.*;
import org.gridgain.grid.events.*;
import org.gridgain.grid.marshaller.optimized.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.grid.spi.swapspace.*;
import org.gridgain.grid.spi.swapspace.noop.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.testframework.junits.common.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static java.util.concurrent.TimeUnit.*;
import static org.gridgain.grid.GridDeploymentMode.*;
import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.gridgain.grid.cache.GridCachePeekMode.*;
import static org.gridgain.grid.cache.GridCacheWriteSynchronizationMode.*;
import static org.gridgain.grid.events.GridEventType.*;

/**
 * Test for cache swap.
 */
public class GridCacheSwapSelfTest extends GridCommonAbstractTest {
    /** Entry count. */
    private static final int ENTRY_CNT = 1000;

    /** Swap count. */
    private final AtomicInteger swapCnt = new AtomicInteger();

    /** Unswap count. */
    private final AtomicInteger unswapCnt = new AtomicInteger();

    /** Saved versions. */
    private final Map<Integer, Object> versions = new HashMap<>();

    /** */
    private final GridTcpDiscoveryIpFinder ipFinder = new GridTcpDiscoveryVmIpFinder(true);

    /** */
    private boolean swapEnabled = true;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        GridTcpDiscoverySpi disco = new GridTcpDiscoverySpi();

        disco.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(disco);

        cfg.setNetworkTimeout(2000);

        GridCacheConfiguration cacheCfg = defaultCacheConfiguration();

        cacheCfg.setWriteSynchronizationMode(FULL_SYNC);
        cacheCfg.setCacheMode(REPLICATED);
        cacheCfg.setSwapEnabled(swapEnabled);

        cfg.setCacheConfiguration(cacheCfg);

        cfg.setDeploymentMode(SHARED);
        cfg.setPeerClassLoadingLocalClassPathExclude(GridCacheSwapSelfTest.class.getName(),
            CacheValue.class.getName());

        cfg.setMarshaller(new GridOptimizedMarshaller(false));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        versions.clear();

        super.afterTestsStopped();
    }

    /**
     * @throws Exception If failed.
     */
    public void testDisabledSwap() throws Exception {
        swapEnabled = false;

        try {
            Ignite ignite = startGrids(1);

            GridSwapSpaceSpi swapSpi = ignite.configuration().getSwapSpaceSpi();

            assertNotNull(swapSpi);

            assertTrue(swapSpi instanceof GridNoopSwapSpaceSpi);
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testEnabledSwap() throws Exception {
        try {
            Ignite ignite = startGrids(1);

            GridSwapSpaceSpi swapSpi = ignite.configuration().getSwapSpaceSpi();

            assertNotNull(swapSpi);

            assertFalse(swapSpi instanceof GridNoopSwapSpaceSpi);
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings("BusyWait")
    public void testSwapDeployment() throws Exception {
        try {
            Ignite ignite1 = startGrid(1);
            Ignite ignite2 = startGrid(2);

            GridCache<Integer, Object> cache1 = ignite1.cache(null);
            GridCache<Integer, Object> cache2 = ignite2.cache(null);

            Object v1 = new CacheValue(1);

            cache1.put(1, v1);

            info("Stored value in cache1 [v=" + v1 + ", ldr=" + v1.getClass().getClassLoader() + ']');

            Object v2 = cache2.get(1);

            assert v2 != null;

            info("Read value from cache2 [v=" + v2 + ", ldr=" + v2.getClass().getClassLoader() + ']');

            assert v2 != null;
            assert !v2.getClass().getClassLoader().equals(getClass().getClassLoader());
            assert v2.getClass().getClassLoader().getClass().getName().contains("GridDeploymentClassLoader");

            SwapListener lsnr = new SwapListener();

            ignite2.events().localListen(lsnr, EVT_CACHE_OBJECT_SWAPPED, EVT_CACHE_OBJECT_UNSWAPPED);

            cache2.evictAll();

            assert lsnr.awaitSwap();

            assert cache2.get(1) != null;

            assert lsnr.awaitUnswap();

            ignite2.events().stopLocalListen(lsnr);

            lsnr = new SwapListener();

            ignite2.events().localListen(lsnr, EVT_CACHE_OBJECT_SWAPPED, EVT_CACHE_OBJECT_UNSWAPPED);

            cache2.evictAll();

            assert lsnr.awaitSwap();

            stopGrid(1);

            boolean success = false;

            for (int i = 0; i < 6; i++) {
                success = cache2.get(1) == null;

                if (success)
                    break;
                else if (i < 2) {
                    info("Sleeping to wait for cache clear.");

                    Thread.sleep(500);
                }
            }

            assert success;
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @param cache Cache.
     * @param timeout Timeout.
     * @return {@code True} if success.
     * @throws InterruptedException If thread was interrupted.
     */
    @SuppressWarnings({"BusyWait"})
    private boolean waitCacheEmpty(GridCacheProjection<Integer,Object> cache, long timeout)
        throws InterruptedException {
        assert cache != null;
        assert timeout >= 0;

        long end = System.currentTimeMillis() + timeout;

        while (end - System.currentTimeMillis() >= 0) {
            if (cache.isEmpty())
                return true;

            Thread.sleep(500);
        }

        return cache.isEmpty();
    }

    /**
     * @throws Exception If failed.
     */
    // TODO: enable when GG-7341 is fixed.
    public void _testSwapEviction() throws Exception {
        try {
            final CountDownLatch evicted = new CountDownLatch(10);

            startGrids(1);

            grid(0).events().localListen(new IgnitePredicate<GridEvent>() {
                @Override public boolean apply(GridEvent evt) {
                    info("Received event: " + evt);

                    switch (evt.type()) {
                        case EVT_SWAP_SPACE_DATA_EVICTED:
                            assert evicted.getCount() > 0;

                            evicted.countDown();

                            break;
                    }

                    return true;
                }
            }, EVT_CACHE_OBJECT_SWAPPED, EVT_CACHE_OBJECT_UNSWAPPED, EVT_SWAP_SPACE_DATA_EVICTED);

            GridCache<Integer, CacheValue> cache = grid(0).cache(null);

            for (int i = 0; i< 20; i++) {
                cache.put(i, new CacheValue(i));

                cache.evict(i);
            }

            assert evicted.await(4, SECONDS) : "Entries were not evicted from swap: " + evicted.getCount();

            Collection<Map.Entry<Integer, CacheValue>> res = cache.queries().
                createSqlQuery(CacheValue.class, "val >= ? and val < ?").
                execute(0, 20).
                get();

            int size = res.size();

            assert size == 10 : size;

            for (Map.Entry<Integer, CacheValue> entry : res) {
                info("Entry: " + entry);

                assert entry != null;
                assert entry.getKey() != null;
                assert entry.getValue() != null;
                assert entry.getKey() == entry.getValue().value();
                assert entry.getKey() >= 10;
            }
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testSwap() throws Exception {
        try {
            startGrids(1);

            grid(0).events().localListen(new IgnitePredicate<GridEvent>() {
                @Override public boolean apply(GridEvent evt) {
                    assert evt != null;

                    switch (evt.type()) {
                        case EVT_CACHE_OBJECT_SWAPPED:
                            swapCnt.incrementAndGet();

                            break;
                        case EVT_CACHE_OBJECT_UNSWAPPED:
                            unswapCnt.incrementAndGet();

                            break;
                    }

                    return true;
                }
            }, EVT_CACHE_OBJECT_SWAPPED, EVT_CACHE_OBJECT_UNSWAPPED);

            GridCache<Integer, CacheValue> cache = grid(0).cache(null);

            populate(cache);
            evictAll(cache);

            query(cache, 0, 200);        // Query swapped entries.
            unswap(cache, 200, 400);     // Check 'promote' method.
            unswapAll(cache, 400, 600);  // Check 'promoteAll' method.
            get(cache, 600, 800);        // Check 'get' method.
            peek(cache, 800, ENTRY_CNT); // Check 'peek' method in 'SWAP' mode.

            // Check that all entries were unswapped.
            for (int i = 0; i < ENTRY_CNT; i++) {
                CacheValue val = cache.peek(i);

                assert val != null;
                assert val.value() == i;
            }

            // Query unswapped entries.
            Collection<Map.Entry<Integer, CacheValue>> res = cache.queries().
                createSqlQuery(CacheValue.class, "val >= ? and val < ?").
                execute(0, ENTRY_CNT).
                get();

            assert res.size() == ENTRY_CNT;

            for (Map.Entry<Integer, CacheValue> entry : res) {
                assert entry != null;
                assert entry.getKey() != null;
                assert entry.getValue() != null;
                assert entry.getKey() == entry.getValue().value();
            }
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testSwapIterator() throws Exception {
        try {
            startGrids(1);

            grid(0);

            GridCache<Integer, Integer> cache = grid(0).cache(null);

            for (int i = 0; i < 100; i++) {
                info("Putting: " + i);

                cache.put(i, i);

                assert cache.evict(i);
            }

            Iterator<Map.Entry<Integer, Integer>> iter = cache.swapIterator();

            assert iter != null;

            int i = 0;

            while (iter.hasNext()) {
                Map.Entry<Integer, Integer> e = iter.next();

                Integer key = e.getKey();

                info("Key: " + key);

                i++;

                iter.remove();

                assertNull(cache.get(key));
            }

            assertEquals(100, i);

            assert cache.isEmpty();
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * Populates cache.
     *
     * @param cache Cache.
     * @throws Exception In case of error.
     */
    private void populate(GridCacheProjection<Integer, CacheValue> cache) throws Exception {
        resetCounters();

        for (int i = 0; i < ENTRY_CNT; i++) {
            cache.put(i, new CacheValue(i));

            CacheValue val = cache.peek(i);

            assert val != null;
            assert val.value() == i;

            GridCacheEntry<Integer, CacheValue> entry = cache.entry(i);

            assert entry != null;

            versions.put(i, entry.version());
        }

        assert swapCnt.get() == 0;
        assert unswapCnt.get() == 0;
    }

    /**
     * Evicts all entries in cache.
     *
     * @param cache Cache.
     * @throws Exception In case of error.
     */
    private void evictAll(GridCacheProjection<Integer, CacheValue> cache) throws Exception {
        resetCounters();

        cache.evictAll();

        for (int i = 0; i < ENTRY_CNT; i++)
            assert cache.peek(i) == null;

        assert swapCnt.get() == ENTRY_CNT;
        assert unswapCnt.get() == 0;
    }

    /**
     * Runs SQL query and checks result.
     *
     * @param cache Cache.
     * @param lowerBound Lower key bound.
     * @param upperBound Upper key bound.
     * @throws Exception In case of error.
     */
    private void query(GridCacheProjection<Integer, CacheValue> cache,
        int lowerBound, int upperBound) throws Exception {
        resetCounters();

        Collection<Map.Entry<Integer, CacheValue>> res = cache.queries().
            createSqlQuery(CacheValue.class, "val >= ? and val < ?").
            execute(lowerBound, upperBound).
            get();

        assert res.size() == upperBound - lowerBound;

        for (Map.Entry<Integer, CacheValue> entry : res) {
            assert entry != null;
            assert entry.getKey() != null;
            assert entry.getValue() != null;
            assert entry.getKey() == entry.getValue().value();
        }

        assert swapCnt.get() == 0;
        assert unswapCnt.get() == 0;

        checkEntries(cache, lowerBound, upperBound);

        assert swapCnt.get() == 0;
        assert unswapCnt.get() == upperBound - lowerBound;
    }

    /**
     * Unswaps entries and checks result.
     *
     * @param cache Cache.
     * @param lowerBound Lower key bound.
     * @param upperBound Upper key bound.
     * @throws Exception In case of error.
     */
    private void unswap(GridCacheProjection<Integer, CacheValue> cache,
        int lowerBound, int upperBound) throws Exception {
        resetCounters();

        assertEquals(0, swapCnt.get());
        assertEquals(0, unswapCnt.get());

        for (int i = lowerBound; i < upperBound; i++) {
            assert cache.peek(i) == null;

            CacheValue val = cache.promote(i);

            assertNotNull(val);
            assertEquals(i, val.value());

            assertEquals(i - lowerBound + 1, unswapCnt.get());
        }

        assertEquals(0, swapCnt.get());
        assertEquals(unswapCnt.get(), upperBound - lowerBound);

        checkEntries(cache, lowerBound, upperBound);

        assertEquals(0, swapCnt.get());
        assertEquals(unswapCnt.get(), upperBound - lowerBound);
    }

    /**
     * Unswaps entries and checks result.
     *
     * @param cache Cache.
     * @param lowerBound Lower key bound.
     * @param upperBound Upper key bound.
     * @throws Exception In case of error.
     */
    private void unswapAll(GridCacheProjection<Integer, CacheValue> cache,
        int lowerBound, int upperBound) throws Exception {
        resetCounters();

        Collection<Integer> keys = new HashSet<>();

        for (int i = lowerBound; i < upperBound; i++) {
            assert cache.peek(i) == null;

            keys.add(i);
        }

        cache.promoteAll(keys);

        assert swapCnt.get() == 0;
        assert unswapCnt.get() == upperBound - lowerBound;

        checkEntries(cache, lowerBound, upperBound);

        assert swapCnt.get() == 0;
        assert unswapCnt.get() == upperBound - lowerBound;
    }

    /**
     * Unswaps entries via {@code get} method and checks result.
     *
     * @param cache Cache.
     * @param lowerBound Lower key bound.
     * @param upperBound Upper key bound.
     * @throws Exception In case of error.
     */
    private void get(GridCacheProjection<Integer, CacheValue> cache,
        int lowerBound, int upperBound) throws Exception {
        resetCounters();

        for (int i = lowerBound; i < upperBound; i++) {
            assert cache.peek(i) == null;

            CacheValue val = cache.get(i);

            assert val != null;
            assert val.value() == i;
        }

        assert swapCnt.get() == 0;
        assert unswapCnt.get() == upperBound - lowerBound;

        checkEntries(cache, lowerBound, upperBound);

        assert swapCnt.get() == 0;
        assert unswapCnt.get() == upperBound - lowerBound;
    }

    /**
     * Peeks entries in {@code SWAP} mode and checks result.
     *
     * @param cache Cache.
     * @param lowerBound Lower key bound.
     * @param upperBound Upper key bound.
     * @throws Exception In case of error.
     */
    private void peek(GridCacheProjection<Integer, CacheValue> cache,
        int lowerBound, int upperBound) throws Exception {
        resetCounters();

        for (int i = lowerBound; i < upperBound; i++) {
            assert cache.peek(i) == null;

            CacheValue val = cache.peek(i, F.asList(SWAP));

            assert val != null;
            assert val.value() == i;
        }

        assert swapCnt.get() == 0;
        assert unswapCnt.get() == 0;

        checkEntries(cache, lowerBound, upperBound);

        assert swapCnt.get() == 0;
        assert unswapCnt.get() == upperBound - lowerBound;
    }

    /**
     * Resets event counters.
     */
    private void resetCounters() {
        swapCnt.set(0);
        unswapCnt.set(0);
    }

    /**
     * Checks that entries in cache are correct after being unswapped.
     * If entry is still swapped, it will be unswapped in this method.
     *
     * @param cache Cache.
     * @param lowerBound Lower key bound.
     * @param upperBound Upper key bound.
     * @throws Exception In case of error.
     */
    private void checkEntries(GridCacheProjection<Integer, CacheValue> cache,
        int lowerBound, int upperBound) throws Exception {
        for (int i = lowerBound; i < upperBound; i++) {
            GridCacheEntry<Integer, CacheValue> entry = cache.entry(i);

            assert entry != null;
            assert entry.getKey() != null;

            CacheValue val = entry.getValue();

            assert val != null;
            assert entry.getKey() == val.value();
            assert entry.version().equals(versions.get(i));
        }
    }

    /**
     *
     */
    private static class CacheValue {
        /** Value. */
        @GridCacheQuerySqlField
        private final int val;

        /**
         * @param val Value.
         */
        private CacheValue(int val) {
            this.val = val;
        }

        /**
         * @return Value.
         */
        public int value() {
            return val;
        }
    }

    /**
     *
     */
    private class SwapListener implements IgnitePredicate<GridEvent> {
        /** */
        private final CountDownLatch swapLatch = new CountDownLatch(1);

        /** */
        private final CountDownLatch unswapLatch = new CountDownLatch(1);

        /** {@inheritDoc} */
        @Override public boolean apply(GridEvent evt) {
            assert evt != null;

            info("Received event: " + evt);

            switch (evt.type()) {
                case EVT_CACHE_OBJECT_SWAPPED:
                    swapLatch.countDown();

                    break;
                case EVT_CACHE_OBJECT_UNSWAPPED:
                    unswapLatch.countDown();

                    break;
            }

            return true;
        }

        /**
         * @return {@code True} if await succeeded.
         * @throws InterruptedException If interrupted.
         */
        boolean awaitSwap() throws InterruptedException {
            return swapLatch.await(5000, MILLISECONDS);
        }

        /**
         * @return {@code True} if await succeeded.
         * @throws InterruptedException If interrupted.
         */
        boolean awaitUnswap() throws InterruptedException {
            return unswapLatch.await(5000, MILLISECONDS);
        }
    }
}
