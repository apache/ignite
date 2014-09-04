/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.eviction.lru;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.eviction.lru.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.testframework.junits.common.*;

import java.util.*;

import static org.gridgain.grid.cache.GridCacheAtomicityMode.*;
import static org.gridgain.grid.cache.GridCacheDistributionMode.*;
import static org.gridgain.grid.cache.GridCachePreloadMode.*;
import static org.gridgain.grid.cache.GridCacheWriteSynchronizationMode.*;

/**
 * LRU near eviction test (GG-8884).
 */
public class GridCacheLruNearEvictionPolicySelfTest extends GridCommonAbstractTest {
    /** */
    private static final GridTcpDiscoveryIpFinder ipFinder = new GridTcpDiscoveryVmIpFinder(true);

    /** Maximum size for near eviction policy. */
    private static final int EVICTION_MAX_SIZE = 10;

    /** Grid count. */
    private static final int GRID_COUNT = 3;

    /** Cache atomicity mode specified by test. */
    private GridCacheAtomicityMode atomicityMode;

    /** {@inheritDoc} */
    @Override protected GridConfiguration getConfiguration(String gridName) throws Exception {
        GridConfiguration c = super.getConfiguration(gridName);

        GridCacheConfiguration cc = new GridCacheConfiguration();

        cc.setAtomicityMode(atomicityMode);
        cc.setCacheMode(GridCacheMode.PARTITIONED);
        cc.setWriteSynchronizationMode(PRIMARY_SYNC);
        cc.setDistributionMode(NEAR_PARTITIONED);
        cc.setPreloadMode(ASYNC);
        cc.setNearEvictionPolicy(new GridCacheLruEvictionPolicy(EVICTION_MAX_SIZE));
        cc.setStartSize(100);
        cc.setQueryIndexEnabled(true);
        cc.setBackups(1);

        c.setCacheConfiguration(cc);

        GridTcpDiscoverySpi disco = new GridTcpDiscoverySpi();

        disco.setIpFinder(ipFinder);

        c.setDiscoverySpi(disco);

        return c;
    }

    /**
     * @throws Exception If failed.
     */
    public void testAtomicNearEvictionMaxSize() throws Exception {
        atomicityMode = ATOMIC;

        checkNearEvictionMaxSize();
    }

    /**
     * @throws Exception If failed.
     */
    public void testTransactionalNearEvictionMaxSize() throws Exception {
        atomicityMode = TRANSACTIONAL;

        checkNearEvictionMaxSize();
    }

    /**
     * @throws Exception If failed.
     */
    private void checkNearEvictionMaxSize() throws Exception {
        startGridsMultiThreaded(GRID_COUNT);

        try {
            Random rand = new Random(0);

            int cnt = 1000;

            info("Inserting " + cnt + " keys to cache.");

            for (int i = 0; i < cnt; i++) {
                GridCache<Integer, String> cache = grid(rand.nextInt(GRID_COUNT)).cache(null);

                int key = i % 500;
                String val = Integer.toString(key);

                cache.put(key, val);
            }

            for (int i = 0; i < GRID_COUNT; i++)
                assertTrue("Near cache size " + near(i).nearSize() + ", but eviction maximum size " + EVICTION_MAX_SIZE,
                    near(i).nearSize() <= EVICTION_MAX_SIZE);

            info("Getting " + cnt + " keys from cache.");

            for (int i = 0; i < cnt; i++) {
                GridCache<Integer, String> cache = grid(rand.nextInt(GRID_COUNT)).cache(null);

                int key = i % 500;

                String val = cache.get(key);

                assertTrue(val == null || val.equals(Integer.toString(key)));
            }

            for (int i = 0; i < GRID_COUNT; i++)
                assertTrue("Near cache size " + near(i).nearSize() + ", but eviction maximum size " + EVICTION_MAX_SIZE,
                    near(i).nearSize() <= EVICTION_MAX_SIZE);
        }
        finally {
            stopAllGrids();
        }
    }
}
