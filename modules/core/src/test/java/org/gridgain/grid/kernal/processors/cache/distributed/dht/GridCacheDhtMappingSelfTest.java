/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.distributed.dht;

import org.apache.ignite.*;
import org.apache.ignite.configuration.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.processors.cache.distributed.near.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.testframework.junits.common.*;

import static org.gridgain.grid.cache.GridCacheAtomicityMode.*;
import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.gridgain.grid.cache.GridCacheDistributionMode.*;
import static org.gridgain.grid.cache.GridCachePreloadMode.*;

/**
 * Tests dht mapping.
 */
public class GridCacheDhtMappingSelfTest extends GridCommonAbstractTest {
    /** Number of key backups. */
    private static final int BACKUPS = 1;

    /** IP finder. */
    private GridTcpDiscoveryIpFinder ipFinder = new GridTcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        GridCacheConfiguration cacheCfg = defaultCacheConfiguration();

        cacheCfg.setCacheMode(PARTITIONED);
        cacheCfg.setWriteSynchronizationMode(GridCacheWriteSynchronizationMode.FULL_SYNC);
        cacheCfg.setPreloadMode(SYNC);
        cacheCfg.setBackups(BACKUPS);
        cacheCfg.setAtomicityMode(TRANSACTIONAL);
        cacheCfg.setDistributionMode(NEAR_PARTITIONED);

        GridTcpDiscoverySpi disco = new GridTcpDiscoverySpi();

        disco.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(disco);
        cfg.setCacheConfiguration(cacheCfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        stopAllGrids();
    }

    /** @throws Exception If failed. */
    public void testMapping() throws Exception {
        int nodeCnt = 5;

        startGridsMultiThreaded(nodeCnt);

        GridCache<Integer, Integer> cache = grid(nodeCnt - 1).cache(null);

        int kv = 1;

        cache.put(kv, kv);

        int cnt = 0;

        for (int i = 0; i < nodeCnt; i++) {
            Ignite g = grid(i);

            GridDhtCacheAdapter<Integer, Integer> dht = ((GridNearCacheAdapter<Integer, Integer>)
                ((GridKernal)g).<Integer, Integer>internalCache()).dht();

            if (dht.peek(kv) != null) {
                info("Key found on node: " + g.cluster().localNode().id());

                cnt++;
            }
        }

        // Test key should be on primary and backup node only.
        assertEquals(1 + BACKUPS, cnt);
    }
}
