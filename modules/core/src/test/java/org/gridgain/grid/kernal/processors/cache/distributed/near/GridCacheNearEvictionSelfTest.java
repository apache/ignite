/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.distributed.near;

import org.apache.ignite.*;
import org.apache.ignite.configuration.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.marshaller.optimized.*;
import org.gridgain.grid.resources.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.testframework.*;
import org.gridgain.testframework.junits.common.*;

import java.util.concurrent.*;

import static org.gridgain.grid.cache.GridCacheAtomicWriteOrderMode.*;
import static org.gridgain.grid.cache.GridCacheAtomicityMode.*;
import static org.gridgain.grid.cache.GridCacheDistributionMode.*;
import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.gridgain.grid.cache.GridCachePreloadMode.*;
import static org.gridgain.grid.cache.GridCacheWriteSynchronizationMode.*;

/**
 * Tests entries distribution between primary-backup-near caches according to nodes count in grid.
 */
public class GridCacheNearEvictionSelfTest extends GridCommonAbstractTest {
    /** Grid count. */
    private int gridCnt;

    /** */
    private static GridTcpDiscoveryIpFinder ipFinder = new GridTcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(gridName);

        GridCacheConfiguration cc = defaultCacheConfiguration();

        cc.setCacheMode(PARTITIONED);
        cc.setDistributionMode(NEAR_PARTITIONED);
        cc.setWriteSynchronizationMode(FULL_SYNC);
        cc.setBackups(1);
        cc.setPreloadMode(SYNC);
        cc.setNearEvictionPolicy(null);
        cc.setAtomicityMode(atomicityMode());
        cc.setAtomicWriteOrderMode(PRIMARY);

        c.setCacheConfiguration(cc);

        GridTcpDiscoverySpi disco = new GridTcpDiscoverySpi();

        disco.setIpFinder(ipFinder);

        c.setDiscoverySpi(disco);

        c.setMarshaller(new GridOptimizedMarshaller(false));

        return c;
    }

    /**
     * @return Atomicity mode.
     */
    protected GridCacheAtomicityMode atomicityMode() {
        return TRANSACTIONAL;
    }

    /** @throws Exception If failed. */
    public void testNearEnabledOneNode() throws Exception {
        gridCnt = 1;

        startGridsMultiThreaded(gridCnt);

        try {
            GridCache<Integer, String> c = grid(0).cache(null);

            int cnt = 100;

            for (int i = 0; i < cnt; i++)
                assertTrue(c.putx(i, Integer.toString(i)));

            assertEquals(cnt, c.size());
            assertEquals(cnt, c.size());
            assertEquals(0, near(0).nearSize());
        }
        finally {
            stopAllGrids();
        }
    }

    /** @throws Exception If failed. */
    public void testNearEnabledTwoNodes() throws Exception {
        gridCnt = 2;

        startGridsMultiThreaded(gridCnt);

        try {
            final int cnt = 100;

            grid(0).compute().broadcast(new Callable<Object>() {
                @GridInstanceResource
                private Ignite ignite;

                @Override public Object call() throws Exception {
                    GridCache<Integer, String> c = ignite.cache(null);

                    for (int i = 0; i < cnt; i++)
                        c.putx(i, Integer.toString(i));

                    return true;
                }
            });

            for (int i = 0; i < gridCnt; i++) {
                assertEquals(cnt, grid(i).cache(null).size());
                assertEquals(cnt, grid(i).cache(null).size());
                assertEquals(0, near(i).nearSize());
            }
        }
        finally {
            stopAllGrids();
        }
    }

    /** @throws Exception If failed. */
    public void testNearEnabledThreeNodes() throws Exception {
        gridCnt = 3;

        startGridsMultiThreaded(gridCnt);

        try {
            final int cnt = 100;

            grid(0).compute().broadcast(new Callable<Object>() {
                @GridInstanceResource
                private Ignite ignite;

                @Override public Object call() throws Exception {
                    GridCache<Integer, String> c = ignite.cache(null);

                    for (int i = 0; i < cnt; i++)
                        c.putx(i, Integer.toString(i));

                    return true;
                }
            });

            for (int i = 0; i < gridCnt; i++) {
                final Ignite g = grid(i);

                // Repeatedly check cache sizes because of concurrent cache updates.
                assertTrue(GridTestUtils.waitForCondition(new PA() {
                    @Override public boolean apply() {
                        // Every node contains either near, backup, or primary.
                        return cnt == g.cache(null).size();
                    }
                }, getTestTimeout()));

                int keySize = near(i).nearSize();

                assert keySize < cnt : "Key size is not less than count [cnt=" + cnt + ", size=" + keySize + ']';
            }
        }
        finally {
            stopAllGrids();
        }
    }
}
