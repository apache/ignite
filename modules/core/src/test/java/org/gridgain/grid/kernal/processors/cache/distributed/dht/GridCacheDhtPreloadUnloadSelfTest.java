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
import org.apache.ignite.lifecycle.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.affinity.consistenthash.*;
import org.gridgain.grid.resources.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.testframework.junits.common.*;

import static org.gridgain.grid.GridDeploymentMode.*;
import static org.gridgain.grid.cache.GridCacheAtomicityMode.*;
import static org.gridgain.grid.cache.GridCacheConfiguration.*;
import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.gridgain.grid.cache.GridCachePreloadMode.*;

/**
 * Test large cache counts.
 */
@SuppressWarnings({"BusyWait"})
public class GridCacheDhtPreloadUnloadSelfTest extends GridCommonAbstractTest {
    /** Default backups. */
    private static final int DFLT_BACKUPS = 1;

    /** Partitions. */
    private static final int DFLT_PARTITIONS = 521;

    /** Preload batch size. */
    private static final int DFLT_BATCH_SIZE = DFLT_PRELOAD_BATCH_SIZE;

    /** Number of key backups. Each test method can set this value as required. */
    private int backups = DFLT_BACKUPS;

    /** Preload mode. */
    private GridCachePreloadMode preloadMode = ASYNC;

    /** */
    private int preloadBatchSize = DFLT_BATCH_SIZE;

    /** Number of partitions. */
    private int partitions = DFLT_PARTITIONS;

    /** */
    private LifecycleBean lbean;

    /** IP finder. */
    private GridTcpDiscoveryIpFinder ipFinder = new GridTcpDiscoveryVmIpFinder(true);

    /** Network timeout. */
    private long netTimeout = 1000;

    /**
     *
     */
    public GridCacheDhtPreloadUnloadSelfTest() {
        super(false /*start grid. */);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(gridName);

        GridCacheConfiguration cc = defaultCacheConfiguration();

        cc.setCacheMode(PARTITIONED);
        cc.setPreloadBatchSize(preloadBatchSize);
        cc.setWriteSynchronizationMode(GridCacheWriteSynchronizationMode.FULL_SYNC);
        cc.setPreloadMode(preloadMode);
        cc.setAffinity(new GridCacheConsistentHashAffinityFunction(false, partitions));
        cc.setBackups(backups);
        cc.setAtomicityMode(TRANSACTIONAL);

        GridTcpDiscoverySpi disco = new GridTcpDiscoverySpi();

        disco.setIpFinder(ipFinder);

        if (lbean != null)
            c.setLifecycleBeans(lbean);

        c.setDiscoverySpi(disco);
        c.setCacheConfiguration(cc);
        c.setDeploymentMode(CONTINUOUS);
        c.setNetworkTimeout(netTimeout);

        return c;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        backups = DFLT_BACKUPS;
        partitions = DFLT_PARTITIONS;
        preloadMode = ASYNC;
        preloadBatchSize = DFLT_BATCH_SIZE;
        netTimeout = 1000;
    }

    /** @throws Exception If failed. */
    public void testUnloadZeroBackupsTwoNodes() throws Exception {
        preloadMode = SYNC;
        backups = 0;
        netTimeout = 500;

        try {
            startGrid(0);

            int cnt = 1000;

            populate(grid(0).<Integer, String>cache(null), cnt);

            int gridCnt = 2;

            for (int i = 1; i < gridCnt; i++)
                startGrid(i);

            long wait = 3000;

            waitForUnload(gridCnt, cnt, wait);
        }
        finally {
            stopAllGrids();
        }
    }

    /** @throws Exception If failed. */
    public void testUnloadOneBackupTwoNodes() throws Exception {
        preloadMode = SYNC;
        backups = 1;
        netTimeout = 500;

        try {
            startGrid(0);

            int cnt = 1000;

            populate(grid(0).<Integer, String>cache(null), cnt);

            int gridCnt = 2;

            for (int i = 1; i < gridCnt; i++)
                startGrid(i);

            long wait = 2000;

            info("Sleeping for " + wait + "ms");

            // Unfortunately there is no other way but sleep.
            Thread.sleep(wait);

            for (int i = 0; i < gridCnt; i++)
                info("Grid size [i=" + i + ", size=" + grid(i).cache(null).size() + ']');

            for (int i = 0; i < gridCnt; i++) {
                GridCache<Integer, String> c = grid(i).cache(null);

                // Nothing should be unloaded since nodes are backing up each other.
                assert c.size() == cnt;
            }
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     *
     * @param gridCnt Grid count.
     * @param cnt Count.
     * @param wait Wait.
     * @throws InterruptedException If interrupted.
     */
    private void waitForUnload(long gridCnt, long cnt, long wait) throws InterruptedException {
        info("Waiting for preloading to complete for " + wait + "ms...");

        long endTime = System.currentTimeMillis() + wait;

        while (System.currentTimeMillis() < endTime) {
            boolean err = false;

            for (int i = 0; i < gridCnt; i++) {
                GridCache<Integer, String> c = grid(i).cache(null);

                if (c.size() >= cnt)
                    err = true;
            }

            if (!err)
                break;
            else
                Thread.sleep(500);
        }

        for (int i = 0; i < gridCnt; i++)
            info("Grid size [i=" + i + ", size=" + grid(i).cache(null).size() + ']');

        for (int i = 0; i < gridCnt; i++) {
            GridCache<Integer, String> c = grid(i).cache(null);

            assert c.size() < cnt;
        }
    }

    /** @throws Exception If failed. */
    public void testUnloadOneBackupThreeNodes() throws Exception {
        preloadMode = SYNC;
        backups = 1;
        netTimeout = 500;
        partitions = 23;

        try {
            startGrid(0);

            int cnt = 1000;

            populate(grid(0).<Integer, String>cache(null), cnt);

            int gridCnt = 3;

            for (int i = 1; i < gridCnt; i++) {
                startGrid(i);

                for (int j = 0; j <= i; j++)
                    info("Grid size [i=" + i + ", size=" + grid(j).cache(null).size() + ']');
            }

            long wait = 3000;

            waitForUnload(gridCnt, cnt, wait);
        }
        finally {
            stopAllGrids();
        }
    }

    /** @throws Exception If failed. */
    public void testUnloadOneBackThreeNodesWithLifeCycleBean() throws Exception {
        preloadMode = SYNC;
        backups = 1;

        try {
            final int cnt = 1000;

            lbean = new LifecycleBean() {
                @GridInstanceResource
                private Ignite ignite;

                @Override public void onLifecycleEvent(GridLifecycleEventType evt) throws GridException {
                    if (evt == GridLifecycleEventType.AFTER_GRID_START) {
                        GridCache<Integer, String> c = ignite.cache(null);

                        if (c.putxIfAbsent(-1, "true")) {
                            populate(ignite.<Integer, String>cache(null), cnt);

                            info(">>> POPULATED GRID <<<");
                        }
                    }
                }
            };

            int gridCnt = 3;

            for (int i = 0; i < gridCnt; i++) {
                startGrid(i);

                for (int j = 0; j < i; j++)
                    info("Grid size [i=" + i + ", size=" + grid(j).cache(null).size() + ']');
            }

            long wait = 3000;

            waitForUnload(gridCnt, cnt, wait);
        }
        finally {
            lbean = null;

            stopAllGrids();
        }
    }

    /**
     * @param c Cache.
     * @param cnt Key count.
     * @throws GridException If failed.
     */
    private void populate(GridCache<Integer, String> c, int cnt) throws GridException {
        for (int i = 0; i < cnt; i++)
            c.put(i, value(1024));
    }

    /**
     * @param size Size.
     * @return Value.
     */
    private String value(int size) {
        StringBuilder b = new StringBuilder(size / 2 + 1);

        for (int i = 0; i < size / 3; i++)
            b.append('a' + (i % 26));

        return b.toString();
    }
}
