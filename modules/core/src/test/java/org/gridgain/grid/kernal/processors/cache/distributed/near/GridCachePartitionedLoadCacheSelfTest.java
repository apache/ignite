/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.distributed.near;

import org.apache.ignite.configuration.*;
import org.apache.ignite.lang.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.store.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.testframework.junits.common.*;
import org.jetbrains.annotations.*;

import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.gridgain.grid.cache.GridCacheWriteSynchronizationMode.*;

/**
 * Load cache test.
 */
public class GridCachePartitionedLoadCacheSelfTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Grids count. */
    private static final int GRID_CNT = 3;

    /** Puts count. */
    private static final int PUT_CNT = 100;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        GridCacheConfiguration cache = defaultCacheConfiguration();

        cache.setCacheMode(PARTITIONED);
        cache.setBackups(1);
        cache.setStore(new TestStore());
        cache.setWriteSynchronizationMode(FULL_SYNC);

        cfg.setCacheConfiguration(cache);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(disco);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testLoadCache() throws Exception {
        try {
            startGridsMultiThreaded(GRID_CNT);

            GridCache<Integer, String> cache = cache(0);

            cache.loadCache(null, 0, PUT_CNT);

            int[] parts = cache.affinity().allPartitions(grid(0).localNode());

            int cnt1 = 0;

            for (int i = 0; i < PUT_CNT; i++)
                if (U.containsIntArray(parts,  cache.affinity().partition(i)))
                    cnt1++;

            info("Number of keys to load: " + cnt1);

            int cnt2 = 0;

            for (GridCacheEntry<Integer, String> e : cache.entrySet()) {
                assert e.primary() || e.backup();

                cnt2++;
            }

            info("Number of keys loaded: " + cnt2);

            assertEquals(cnt1, cnt2);
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * Test store.
     */
    private static class TestStore extends GridCacheStoreAdapter<Integer, String> {
        /** {@inheritDoc} */
        @Override public void loadCache(IgniteBiInClosure<Integer, String> clo,
            @Nullable Object... args) throws GridException {
            assert clo != null;
            assert args != null;

            Integer cnt = (Integer)args[0];

            assert cnt != null;

            for (int i = 0; i < cnt; i++)
                clo.apply(i, "val" + i);
        }

        /** {@inheritDoc} */
        @Override public String load(GridCacheTx tx, Integer key) throws GridException {
            // No-op.

            return null;
        }

        /** {@inheritDoc} */
        @Override public void put(GridCacheTx tx, Integer key, String val) throws GridException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void remove(GridCacheTx tx, Integer key) throws GridException {
            // No-op.
        }
    }
}
