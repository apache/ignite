/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.distributed.near;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.store.*;
import org.gridgain.grid.kernal.processors.cache.distributed.*;
import org.gridgain.grid.spi.discovery.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.testframework.junits.common.*;
import org.jetbrains.annotations.*;

import java.util.concurrent.atomic.*;

import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.gridgain.grid.cache.GridCacheWriteSynchronizationMode.*;
import static org.gridgain.grid.kernal.processors.cache.distributed.GridCacheModuloAffinityFunction.*;

/**
 * Test that store is called correctly on puts.
 */
public class GridCachePartitionedStorePutSelfTest extends GridCommonAbstractTest {
    /** */
    private static final GridTcpDiscoveryIpFinder IP_FINDER = new GridTcpDiscoveryVmIpFinder(true);

    /** */
    private static final AtomicInteger CNT = new AtomicInteger(0);

    /** */
    private GridCache<Integer, Integer> cache1;

    /** */
    private GridCache<Integer, Integer> cache2;

    /** */
    private GridCache<Integer, Integer> cache3;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setDiscoverySpi(discoverySpi());
        cfg.setCacheConfiguration(cacheConfiguration());
        cfg.setUserAttributes(F.asMap(IDX_ATTR, CNT.getAndIncrement()));

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
        cfg.setStore(new TestStore());
        cfg.setAffinity(new GridCacheModuloAffinityFunction(3, 1));
        cfg.setWriteSynchronizationMode(FULL_SYNC);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        cache1 = startGrid(1).cache(null);
        cache2 = startGrid(2).cache(null);
        cache3 = startGrid(3).cache(null);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutx() throws Throwable {
        info("Putting to the first node.");

        cache1.putx(0, 1);

        info("Putting to the second node.");

        cache2.putx(0, 2);

        info("Putting to the third node.");

        cache3.putx(0, 3);
    }

    /**
     * Test store.
     */
    private static class TestStore extends GridCacheStoreAdapter<Object, Object> {
        /** {@inheritDoc} */
        @Override public Object load(@Nullable GridCacheTx tx, Object key)
            throws GridException {
            assert false;

            return null;
        }

        /** {@inheritDoc} */
        @Override public void put(GridCacheTx tx, Object key, @Nullable Object val)
            throws GridException {
            // No-op
        }

        /** {@inheritDoc} */
        @Override public void remove(GridCacheTx tx, Object key) throws GridException {
            // No-op
        }
    }
}
