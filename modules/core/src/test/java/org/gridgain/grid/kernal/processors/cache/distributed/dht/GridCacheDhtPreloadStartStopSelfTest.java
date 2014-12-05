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
import org.apache.ignite.lang.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.affinity.*;
import org.gridgain.grid.cache.affinity.consistenthash.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.kernal.processors.cache.distributed.dht.preloader.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.testframework.junits.common.*;

import java.util.*;

import static org.apache.ignite.configuration.GridDeploymentMode.*;
import static org.gridgain.grid.cache.GridCacheAtomicityMode.*;
import static org.gridgain.grid.cache.GridCacheConfiguration.*;
import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.gridgain.grid.cache.GridCachePreloadMode.*;
import static org.gridgain.grid.kernal.processors.cache.distributed.dht.GridDhtPartitionState.*;

/**
 * Test cases for partitioned cache {@link GridDhtPreloader preloader}.
 */
public class GridCacheDhtPreloadStartStopSelfTest extends GridCommonAbstractTest {
    /** */
    private static final long TEST_TIMEOUT = 5 * 60 * 1000;

    /** Default backups. */
    private static final int DFLT_BACKUPS = 1;

    /** Partitions. */
    private static final int DFLT_PARTITIONS = 521;

    /** Preload batch size. */
    private static final int DFLT_BATCH_SIZE = DFLT_PRELOAD_BATCH_SIZE;

    /** Default cache count. */
    private static final int DFLT_CACHE_CNT = 10;

    /** Number of key backups. Each test method can set this value as required. */
    private int backups = DFLT_BACKUPS;

    /** Preload mode. */
    private GridCachePreloadMode preloadMode = ASYNC;

    /** */
    private int preloadBatchSize = DFLT_BATCH_SIZE;

    /** Number of partitions. */
    private int partitions = DFLT_PARTITIONS;

    /** */
    private int cacheCnt = DFLT_CACHE_CNT;

    /** IP finder. */
    private GridTcpDiscoveryIpFinder ipFinder = new GridTcpDiscoveryVmIpFinder(true);

    /**
     *
     */
    public GridCacheDhtPreloadStartStopSelfTest() {
        super(false /*start grid. */);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        GridCacheConfiguration[] cacheCfgs = new GridCacheConfiguration[cacheCnt];

        for (int i = 0; i < cacheCnt; i++) {
            GridCacheConfiguration cacheCfg = defaultCacheConfiguration();

            cacheCfg.setName("partitioned-" + i);

            cacheCfg.setCacheMode(PARTITIONED);
            cacheCfg.setPreloadBatchSize(preloadBatchSize);
            cacheCfg.setWriteSynchronizationMode(GridCacheWriteSynchronizationMode.FULL_SYNC);
            cacheCfg.setPreloadMode(preloadMode);
            cacheCfg.setAffinity(new GridCacheConsistentHashAffinityFunction(false, partitions));
            cacheCfg.setBackups(backups);
            cacheCfg.setAtomicityMode(TRANSACTIONAL);

            cacheCfgs[i] = cacheCfg;
        }

        GridTcpDiscoverySpi disco = new GridTcpDiscoverySpi();

        disco.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(disco);
        cfg.setCacheConfiguration(cacheCfgs);
        cfg.setDeploymentMode(CONTINUOUS);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        backups = DFLT_BACKUPS;
        partitions = DFLT_PARTITIONS;
        preloadMode = ASYNC;
        preloadBatchSize = DFLT_BATCH_SIZE;
        cacheCnt = DFLT_CACHE_CNT;
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return TEST_TIMEOUT;
    }

    /**
     * @param cache Cache.
     * @return Affinity.
     */
    private GridCacheAffinity<Integer> affinity(GridCache<Integer, ?> cache) {
        return cache.affinity();
    }

    /**
     * @param c Cache.
     * @return {@code True} if synchronoous preloading.
     */
    private boolean isSync(GridCache<?, ?> c) {
        return c.configuration().getPreloadMode() == SYNC;
    }

    /**
     * @param cnt Number of grids.
     * @param startIdx Start node index.
     * @param list List of started grids.
     * @throws Exception If failed.
     */
    private void startGrids(int cnt, int startIdx, Collection<Ignite> list) throws Exception {
        for (int i = 0; i < cnt; i++)
            list.add(startGrid(startIdx++));
    }

    /** @param grids Grids to stop. */
    private void stopGrids(Iterable<Ignite> grids) {
        for (Ignite g : grids)
            stopGrid(g.name());
    }

    /** @throws Exception If failed. */
    public void testDeadlock() throws Exception {
        info("Testing deadlock...");

        Collection<Ignite> ignites = new LinkedList<>();

        int gridCnt = 3;

        startGrids(gridCnt, 1, ignites);

        info("Grids started: " + gridCnt);

        stopGrids(ignites);
    }

    /**
     * @param keyCnt Key count.
     * @param nodeCnt Node count.
     * @throws Exception If failed.
     */
    private void checkNodes(int keyCnt, int nodeCnt) throws Exception {
        try {
            Ignite g1 = startGrid(0);

            GridCache<Integer, String> c1 = g1.cache(null);

            putKeys(c1, keyCnt);
            checkKeys(c1, keyCnt);

            Collection<Ignite> ignites = new LinkedList<>();

            startGrids(nodeCnt, 1, ignites);

            // Check all nodes.
            for (Ignite g : ignites) {
                GridCache<Integer, String> c = g.cache(null);

                checkKeys(c, keyCnt);
            }

            info(">>> Finished checking nodes [keyCnt=" + keyCnt + ", nodeCnt=" + nodeCnt + ']');

            stopGrids(ignites);

            GridDhtCacheAdapter<Integer, String> dht = dht(c1);

            info(">>> Waiting for preload futures...");

            GridCachePartitionExchangeManager<Object, Object> exchMgr
                = ((GridKernal)g1).context().cache().context().exchange();

            // Wait for exchanges to complete.
            for (IgniteFuture<?> fut : exchMgr.exchangeFutures())
                fut.get();

            GridCacheAffinity<Integer> aff = affinity(c1);

            for (int i = 0; i < keyCnt; i++) {
                if (aff.mapPartitionToPrimaryAndBackups(aff.partition(i)).contains(g1.cluster().localNode())) {
                    GridDhtPartitionTopology<Integer, String> top = dht.topology();

                    for (GridDhtLocalPartition<Integer, String> p : top.localPartitions())
                        assertEquals("Invalid partition state for partition: " + p, OWNING, p.state());
                }
            }
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @param c Cache.
     * @param cnt Key count.
     * @throws GridException If failed.
     */
    private void putKeys(GridCache<Integer, String> c, int cnt) throws GridException {
        for (int i = 0; i < cnt; i++)
            c.put(i, Integer.toString(i));
    }

    /**
     * @param c Cache.
     * @param cnt Key count.
     * @throws GridException If failed.
     */
    private void checkKeys(GridCache<Integer, String> c, int cnt) throws GridException {
        GridCacheAffinity<Integer> aff = affinity(c);

        boolean sync = isSync(c);

        Ignite ignite = c.gridProjection().grid();

        for (int i = 0; i < cnt; i++) {
            if (aff.mapPartitionToPrimaryAndBackups(aff.partition(i)).contains(ignite.cluster().localNode())) {
                String val = sync ? c.peek(i) : c.get(i);

                assertEquals("Key check failed [grid=" + ignite.name() + ", cache=" + c.name() + ", key=" + i + ']',
                    Integer.toString(i), val);
            }
        }
    }
}
