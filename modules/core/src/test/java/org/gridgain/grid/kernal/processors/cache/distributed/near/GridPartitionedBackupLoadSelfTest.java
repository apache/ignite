/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.distributed.near;

import org.apache.ignite.configuration.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.store.*;
import org.gridgain.grid.spi.discovery.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.testframework.junits.common.*;
import org.jetbrains.annotations.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.gridgain.grid.cache.GridCacheWriteSynchronizationMode.*;

/**
 * Test that persistent store is not used when loading invalidated entry from backup node.
 */
public class GridPartitionedBackupLoadSelfTest extends GridCommonAbstractTest {
    /** */
    private static final GridTcpDiscoveryIpFinder IP_FINDER = new GridTcpDiscoveryVmIpFinder(true);

    /** */
    private static final int GRID_CNT = 3;

    /** */
    private final TestStore store = new TestStore();

    /** */
    private final AtomicInteger cnt = new AtomicInteger();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setDiscoverySpi(discoverySpi());
        cfg.setCacheConfiguration(cacheConfiguration());

        return cfg;
    }

    /**
     * @return Discovery SPI.
     */
    private GridDiscoverySpi discoverySpi() {
        GridTcpDiscoverySpi spi = new GridTcpDiscoverySpi();

        spi.setIpFinder(IP_FINDER);

        return spi;
    }

    /**
     * @return Cache configuration.
     */
    private GridCacheConfiguration cacheConfiguration() {
        GridCacheConfiguration cfg = defaultCacheConfiguration();

        cfg.setCacheMode(PARTITIONED);
        cfg.setBackups(1);
        cfg.setStore(store);
        cfg.setWriteSynchronizationMode(FULL_SYNC);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        startGridsMultiThreaded(GRID_CNT);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    public void testBackupLoad() throws Exception {
        assert grid(0).cache(null).putx(1, 1);

        assert store.get(1) == 1;

        for (int i = 0; i < GRID_CNT; i++) {
            GridCache<Integer, Integer> cache = cache(i);

            GridCacheEntry<Integer, Integer> entry = cache.entry(1);

            if (entry.backup()) {
                assert entry.peek() == 1;

                assert entry.clear();

                assert entry.peek() == null;

                // Store is called in putx method, so we reset counter here.
                cnt.set(0);

                assert entry.get() == 1;

                assert cnt.get() == 0;
            }
        }
    }

    /**
     * Test store.
     */
    private class TestStore extends GridCacheStoreAdapter<Integer, Integer> {
        /** */
        private Map<Integer, Integer> map = new ConcurrentHashMap<>();

        /** {@inheritDoc} */
        @Override public Integer load(@Nullable GridCacheTx tx, Integer key)
            throws GridException {
            cnt.incrementAndGet();

            return null;
        }

        /** {@inheritDoc} */
        @Override public void put(GridCacheTx tx, Integer key, @Nullable Integer val)
            throws GridException {
            map.put(key, val);
        }

        /** {@inheritDoc} */
        @Override public void remove(GridCacheTx tx, Integer key) throws GridException {
            // No-op
        }

        /**
         * @param key Key.
         * @return Value.
         */
        public Integer get(Integer key) {
            return map.get(key);
        }
    }
}
