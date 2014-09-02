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
import org.gridgain.grid.util.typedef.*;
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

    private static final int EVICTION_MAX_SIZE = 100;

    /** {@inheritDoc} */
    @Override protected GridConfiguration getConfiguration(String gridName) throws Exception {
        GridConfiguration c = super.getConfiguration(gridName);

        GridCacheConfiguration cc = new GridCacheConfiguration();

        cc.setAtomicityMode(ATOMIC);
        cc.setCacheMode(GridCacheMode.PARTITIONED);
        cc.setWriteSynchronizationMode(PRIMARY_SYNC);
        cc.setDistributionMode(NEAR_PARTITIONED);
        cc.setPreloadMode(ASYNC);
        cc.setNearEvictionPolicy(new GridCacheLruEvictionPolicy(EVICTION_MAX_SIZE));
        cc.setStartSize(500);
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
    public void testXxx() throws Exception {
        final int gridCnt = 3;

        startGridsMultiThreaded(gridCnt);

        try {
            Random rand = new Random();

            int cnt = 10000;

            for (int i = 0; i < cnt; i++) {
                GridCache<Integer, String> cache = grid(rand.nextInt(gridCnt)).cache(null);

                int key = rand.nextInt(5000);
                String val = Integer.toString(key);

                cache.put(key, val);

                if (i % 1000 == 0)
                    info("Stored cache object for key [key=" + key + ", idx=" + i + ']');
            }

            for (int i = 0; i < cnt; i++) {
                GridCache<Integer, String> cache = grid(rand.nextInt(gridCnt)).cache(null);

                int key = rand.nextInt(5000);

                String val = cache.get(key);

                assertTrue(val == null || val.equals(Integer.toString(key)));

                if (i % 1000 == 0)
                    info("Got cache object for key [key=" + key + ", idx=" + i + ']');
            }

            for (int i = 0; i < gridCnt; i++)
                X.println(" --> Near cache size for grid #" + i + ": " + near(i).nearSize());

            for (int i = 0; i < gridCnt; i++)
                assertTrue("Near cache size " + near(i).nearSize() + ", but eviction maximum size " + EVICTION_MAX_SIZE,
                    near(i).nearSize() <= EVICTION_MAX_SIZE);
        }
        finally {
            stopAllGrids();
        }
    }
}
