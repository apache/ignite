/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.eviction.lru;

import org.apache.ignite.*;
import org.apache.ignite.configuration.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.eviction.lru.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.testframework.junits.common.*;

import static org.gridgain.grid.cache.GridCacheAtomicityMode.*;
import static org.gridgain.grid.cache.GridCacheDistributionMode.*;
import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.gridgain.grid.cache.GridCachePreloadMode.*;
import static org.gridgain.grid.cache.GridCacheWriteSynchronizationMode.*;

/**
 * LRU near eviction tests for NEAR_ONLY distribution mode (GG-8884).
 */
public class GridCacheNearOnlyLruNearEvictionPolicySelfTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** Grid count. */
    private static final int GRID_COUNT = 2;

    /** Maximum size for near eviction policy. */
    private static final int EVICTION_MAX_SIZE = 10;

    /** Node count. */
    private int cnt;

    /** Caching mode specified by test. */
    private GridCacheMode cacheMode;

    /** Cache atomicity mode specified by test. */
    private GridCacheAtomicityMode atomicityMode;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cnt = 0;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(gridName);

        GridCacheConfiguration cc = new GridCacheConfiguration();

        cc.setAtomicityMode(atomicityMode);
        cc.setCacheMode(cacheMode);
        cc.setWriteSynchronizationMode(PRIMARY_SYNC);
        cc.setDistributionMode(cnt == 0 ? NEAR_ONLY : PARTITIONED_ONLY);
        cc.setPreloadMode(SYNC);
        cc.setNearEvictionPolicy(new GridCacheLruEvictionPolicy(EVICTION_MAX_SIZE));
        cc.setStartSize(100);
        cc.setQueryIndexEnabled(true);
        cc.setBackups(0);

        c.setCacheConfiguration(cc);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(ipFinder);

        c.setDiscoverySpi(disco);

        cnt++;

        return c;
    }

    /**
     * @throws Exception If failed.
     */
    public void testPartitionedAtomicNearEvictionMaxSize() throws Exception {
        atomicityMode = ATOMIC;
        cacheMode = PARTITIONED;

        checkNearEvictionMaxSize();
    }

    /**
     * @throws Exception If failed.
     */
    public void testPartitionedTransactionalNearEvictionMaxSize() throws Exception {
        atomicityMode = TRANSACTIONAL;
        cacheMode = PARTITIONED;

        checkNearEvictionMaxSize();
    }

    /**
     * @throws Exception If failed.
     */
    public void testReplicatedAtomicNearEvictionMaxSize() throws Exception {
        atomicityMode = ATOMIC;
        cacheMode = REPLICATED;

        checkNearEvictionMaxSize();
    }

    /**
     * @throws Exception If failed.
     */
    public void testReplicatedTransactionalNearEvictionMaxSize() throws Exception {
        atomicityMode = TRANSACTIONAL;
        cacheMode = REPLICATED;

        checkNearEvictionMaxSize();
    }

    /**
     * @throws Exception If failed.
     */
    private void checkNearEvictionMaxSize() throws Exception {
        startGrids(GRID_COUNT);

        try {
            int cnt = 1000;

            info("Inserting " + cnt + " keys to cache.");

            try (IgniteDataLoader<Integer, String> ldr = grid(1).dataLoader(null)) {
                for (int i = 0; i < cnt; i++)
                    ldr.addData(i, Integer.toString(i));
            }

            assertTrue("Near cache size " + near(0).nearSize() + ", but eviction maximum size " + EVICTION_MAX_SIZE,
                near(0).nearSize() <= EVICTION_MAX_SIZE);

            info("Getting " + cnt + " keys from cache.");

            for (int i = 0; i < cnt; i++) {
                GridCache<Integer, String> cache = grid(0).cache(null);

                assertTrue(cache.get(i).equals(Integer.toString(i)));
            }

            assertTrue("Near cache size " + near(0).nearSize() + ", but eviction maximum size " + EVICTION_MAX_SIZE,
                near(0).nearSize() <= EVICTION_MAX_SIZE);
        }
        finally {
            stopAllGrids();
        }
    }
}
