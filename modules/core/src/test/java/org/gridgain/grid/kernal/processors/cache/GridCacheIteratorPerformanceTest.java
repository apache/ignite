/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache;

import org.apache.ignite.configuration.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.testframework.junits.common.*;

import static org.gridgain.grid.cache.GridCacheMode.*;

/**
 * Cache iterator performance test.
 */
public class GridCacheIteratorPerformanceTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final GridTcpDiscoveryIpFinder IP_FINDER = new GridTcpDiscoveryVmIpFinder(true);

    /** Large entry count. */
    private static final int LARGE_ENTRY_CNT = 100000;

    /** Small entry count. */
    private static final int SMALL_ENTRY_CNT = 10000;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        GridTcpDiscoverySpi disco = new GridTcpDiscoverySpi();

        disco.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(disco);

        cfg.setCacheConfiguration(cacheConfiguration());

        return cfg;
    }

    /**
     * @return Cache configuration.
     */
    private GridCacheConfiguration cacheConfiguration() {
        GridCacheConfiguration cfg = defaultCacheConfiguration();

        cfg.setCacheMode(PARTITIONED);
        cfg.setBackups(1);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        startGrid();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopGrid();
    }

    /**
     * Iterates over cache.
     *
     * @param prj Projection.
     * @param c Visitor closure.
     */
    private void iterate(GridCacheProjection<Integer, Integer> prj, IgniteInClosure<GridCacheEntry<Integer, Integer>> c) {
        prj.forEach(c);
    }

    /**
     * @return Empty filter.
     */
    private IgniteInClosure<GridCacheEntry<Integer, Integer>> emptyFilter() {
        return new CI1<GridCacheEntry<Integer, Integer>>() {
            @Override public void apply(GridCacheEntry<Integer, Integer> e) {
                // No-op
            }
        };
    }

    /**
     * @throws Exception If failed.
     */
    public void testSmall() throws Exception {
        GridCacheProjection<Integer, Integer> cache = grid().cache(null);

        for (int i = 0; i < SMALL_ENTRY_CNT; i++)
            assert cache.putx(i, i);

        assert cache.size() == SMALL_ENTRY_CNT;

        IgniteInClosure<GridCacheEntry<Integer, Integer>> c = emptyFilter();

        // Warmup.
        for (int i = 0; i < 10; i ++)
            iterate(cache, c);

        long start = System.currentTimeMillis();

        iterate(cache, c);

        long time = System.currentTimeMillis() - start;

        X.println(">>>");
        X.println(">>> Iterated over " + cache.size() + " entries.");
        X.println(">>> Iteration time: " + time + "ms.");
        X.println(">>>");
    }

    /**
     * @throws Exception If failed.
     */
    public void testLarge() throws Exception {
        GridCacheProjection<Integer, Integer> cache = grid().cache(null);

        for (int i = 0; i < LARGE_ENTRY_CNT; i++)
            assert cache.putx(i, i);

        assert cache.size() == LARGE_ENTRY_CNT;

        IgniteInClosure<GridCacheEntry<Integer, Integer>> c = emptyFilter();

        // Warmup.
        for (int i = 0; i < 3; i++)
            iterate(cache, c);

        long start = System.currentTimeMillis();

        iterate(cache, c);

        long time = System.currentTimeMillis() - start;

        X.println(">>>");
        X.println(">>> Iterated over " + cache.size() + " entries.");
        X.println(">>> Iteration time: " + time + "ms.");
        X.println(">>>");
    }

    /**
     * @throws Exception If failed.
     */
    public void testProjectionFiltered() throws Exception {
        GridCache<Integer, Integer> cache = grid().cache(null);

        for (int i = 0; i < LARGE_ENTRY_CNT; i++)
            assert cache.putx(i, i);

        assert cache.size() == LARGE_ENTRY_CNT;

        IgniteInClosure<GridCacheEntry<Integer, Integer>> c = emptyFilter();

        GridCacheProjection<Integer, Integer> prj = cache.projection(new P2<Integer, Integer>() {
            @Override public boolean apply(Integer key, Integer val) {
                return val < SMALL_ENTRY_CNT;
            }
        });

        assert prj.size() == SMALL_ENTRY_CNT;

        // Warmup.
        for (int i = 0; i < 3; i++)
            iterate(prj, c);

        long start = System.currentTimeMillis();

        iterate(prj, c);

        long time = System.currentTimeMillis() - start;

        X.println(">>>");
        X.println(">>> Iterated over " + prj.size() + " entries.");
        X.println(">>> Iteration time: " + time + "ms.");
        X.println(">>>");
    }


    /**
     * @throws Exception If failed.
     */
    public void testFiltered() throws Exception {
        GridCache<Integer, Integer> cache = grid().cache(null);

        for (int i = 0; i < LARGE_ENTRY_CNT; i++)
            assert cache.putx(i, i);

        assert cache.size() == LARGE_ENTRY_CNT;

        final BoxedInt cnt = new BoxedInt();

        IgniteInClosure<GridCacheEntry<Integer, Integer>> c = new CI1<GridCacheEntry<Integer, Integer>>() {
            @Override public void apply(GridCacheEntry<Integer, Integer> t) {
                if (t.peek() < SMALL_ENTRY_CNT)
                    cnt.increment();
            }
        };

        assert cache.size() == LARGE_ENTRY_CNT;

        // Warmup.
        for (int i = 0; i < 3; i++)
            iterate(cache, c);

        cnt.reset();

        long start = System.currentTimeMillis();

        iterate(cache, c);

        long time = System.currentTimeMillis() - start;

        X.println(">>>");
        X.println(">>> Iterated over " + cache.size() + " entries, accepted " + cnt.get() + " entries.");
        X.println(">>> Iteration time: " + time + "ms.");
        X.println(">>>");
    }

    /**
     * Boxed integer.
     */
    private static class BoxedInt {
        /** */
        private int i;

        /**
         * @param i Integer.
         */
        BoxedInt(int i) {
            this.i = i;
        }

        BoxedInt() {
            // No-op.
        }

        /**
         * @return Integer.
         */
        int increment() {
            return ++i;
        }

        /**
         * @return Integer.
         */
        int get() {
            return i;
        }

        /**
         * Resets integer.
         */
        void reset() {
            i = 0;
        }
    }
}
