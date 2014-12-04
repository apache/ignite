/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.distributed.dht;

import org.apache.ignite.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.affinity.consistenthash.*;
import org.gridgain.grid.resources.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.testframework.junits.common.*;

import static org.gridgain.grid.GridDeploymentMode.*;
import static org.gridgain.grid.cache.GridCacheConfiguration.*;
import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.gridgain.grid.cache.GridCachePreloadMode.*;
import static org.gridgain.grid.cache.GridCacheWriteSynchronizationMode.*;

/**
 * Test large cache counts.
 */
public class GridCacheDhtPreloadBigDataSelfTest extends GridCommonAbstractTest {
    /** Size of values in KB. */
    private static final int KBSIZE = 10 * 1024;

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
    private GridLifecycleBean lbean;

    /** IP finder. */
    private GridTcpDiscoveryIpFinder ipFinder = new GridTcpDiscoveryVmIpFinder(true);

    /**
     *
     */
    public GridCacheDhtPreloadBigDataSelfTest() {
        super(false /*start grid. */);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(gridName);

        GridCacheConfiguration cc = defaultCacheConfiguration();

        cc.setCacheMode(PARTITIONED);
        cc.setPreloadBatchSize(preloadBatchSize);
        cc.setWriteSynchronizationMode(FULL_SYNC);
        cc.setPreloadMode(preloadMode);
        cc.setAffinity(new GridCacheConsistentHashAffinityFunction(false, partitions));
        cc.setBackups(backups);

        GridTcpDiscoverySpi disco = new GridTcpDiscoverySpi();

        disco.setIpFinder(ipFinder);

        if (lbean != null)
            c.setLifecycleBeans(lbean);

        c.setDiscoverySpi(disco);
        c.setCacheConfiguration(cc);
        c.setDeploymentMode(CONTINUOUS);
        c.setNetworkTimeout(1000);

        return c;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        backups = DFLT_BACKUPS;
        partitions = DFLT_PARTITIONS;
        preloadMode = ASYNC;
        preloadBatchSize = DFLT_BATCH_SIZE;
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        // Clean up memory for test suite.
        lbean = null;
    }

    /**
     * @throws Exception If failed.
     */
    public void testLargeObjects() throws Exception {
        preloadMode = SYNC;

        try {
            startGrid(0);

            int cnt = 10000;

            populate(grid(0).<Integer, byte[]>cache(null), cnt, KBSIZE);

            int gridCnt = 3;

            for (int i = 1; i < gridCnt; i++)
                startGrid(i);

            Thread.sleep(10000);

            for (int i = 0; i < gridCnt; i++) {
                GridCache<Integer, String> c = grid(i).cache(null);

                if (backups + 1 <= gridCnt)
                    assert c.size() < cnt : "Cache size: " + c.size();
                else
                    assert c.size() == cnt;
            }
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testLargeObjectsWithLifeCycleBean() throws Exception {
        preloadMode = SYNC;
        partitions = 23;

        try {
            final int cnt = 10000;

            lbean = new GridLifecycleBean() {
                @GridInstanceResource
                private Ignite ignite;

                @Override public void onLifecycleEvent(GridLifecycleEventType evt) throws GridException {
                    if (evt == GridLifecycleEventType.AFTER_GRID_START) {
                        GridCache<Integer, byte[]> c = ignite.cache(null);

                        if (c.putxIfAbsent(-1, new byte[1])) {
                            populate(c, cnt, KBSIZE);

                            info(">>> POPULATED GRID <<<");
                        }
                    }
                }
            };

            int gridCnt = 3;

            for (int i = 0; i < gridCnt; i++)
                startGrid(i);

            for (int i = 0; i < gridCnt; i++)
                info("Grid size [i=" + i + ", size=" + grid(i).cache(null).size() + ']');

            Thread.sleep(10000);

            for (int i = 0; i < gridCnt; i++) {
                GridCache<Integer, String> c = grid(i).cache(null);

                if (backups + 1 <= gridCnt)
                    assert c.size() < cnt;
                else
                    assert c.size() == cnt;
            }
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @param c Cache.
     * @param cnt Key count.
     * @param kbSize Size in KB.
     * @throws GridException If failed.
     */
    private void populate(GridCache<Integer, byte[]> c, int cnt, int kbSize) throws GridException {
        for (int i = 0; i < cnt; i++)
            c.put(i, value(kbSize));
    }

    /**
     * @param size Size.
     * @return Value.
     */
    private byte[] value(int size) {
        return new byte[size];
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 6 * 60 * 1000; // 6 min.
    }
}
