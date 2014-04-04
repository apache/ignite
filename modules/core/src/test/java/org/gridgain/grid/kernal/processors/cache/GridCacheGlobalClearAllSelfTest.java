/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.affinity.consistenthash.*;
import org.gridgain.grid.events.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.testframework.junits.common.*;

import java.util.*;
import java.util.concurrent.atomic.*;

import static org.gridgain.grid.cache.GridCacheAtomicityMode.*;
import static org.gridgain.grid.cache.GridCacheDistributionMode.*;
import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.gridgain.grid.events.GridEventType.*;

/**
 * Test {@link GridCache#globalClearAll()} operation in multinode environment with nodes
 * having caches with different names.
 */
public class GridCacheGlobalClearAllSelfTest extends GridCommonAbstractTest {
    /** Grid nodes count. */
    private static final int GRID_CNT = 3;

    /** Amount of keys stored in the default cache. */
    private static final int KEY_CNT = 20;

    /** Amount of keys stored in cache other than default. */
    private static final int KEY_CNT_OTHER = 10;

    /** Default cache name. */
    private static final String CACHE_NAME = "cache_name";

    /** Cache name which differs from the default one. */
    private static final String CACHE_NAME_OTHER = "cache_name_other";

    /** VM IP finder for TCP discovery SPI. */
    private static final GridTcpDiscoveryIpFinder IP_FINDER = new GridTcpDiscoveryVmIpFinder(true);

    /** Cache name which will be passed to grid configuration. */
    private GridCacheMode cacheMode = PARTITIONED;

    /** Cache mode which will be passed to grid configuration. */
    private String cacheName = CACHE_NAME;

    /** {@inheritDoc} */
    @Override protected GridConfiguration getConfiguration(String gridName) throws Exception {
        GridConfiguration cfg = super.getConfiguration(gridName);

        GridCacheConfiguration ccfg = defaultCacheConfiguration();

        ccfg.setName(cacheName);
        ccfg.setCacheMode(cacheMode);
        ccfg.setAtomicityMode(TRANSACTIONAL);
        ccfg.setDistributionMode(NEAR_PARTITIONED);

        if (cacheMode == PARTITIONED)
            ccfg.setBackups(1);

        cfg.setCacheConfiguration(ccfg);

        GridTcpDiscoverySpi discoSpi = new GridTcpDiscoverySpi();

        discoSpi.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(discoSpi);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * Start GRID_CNT nodes. All nodes except the last one will have one cache with particular name, while the last
     * one will have one cache of the same type, but with different name.
     *
     * @throws Exception In case of exception.
     */
    private void startNodes() throws Exception {
        cacheName = CACHE_NAME;

        for (int i = 0; i < GRID_CNT - 1; i++)
            startGrid(i);

        cacheName = CACHE_NAME_OTHER;

        startGrid(GRID_CNT - 1);
    }

    /**
     * Test for partitioned cache.
     *
     * @throws Exception In case of exception.
     */
    public void testGlobalClearAllPartitioned() throws Exception {
        cacheMode = PARTITIONED;

        startNodes();

        performTest();
    }

    /**
     * Test for replicated cache.
     *
     * @throws Exception In case of exception.
     */
    public void testGlobalClearAllReplicated() throws Exception {
        cacheMode = REPLICATED;

        startNodes();

        performTest();
    }

    /**
     * Ensure that globalClearAll() clears correct cache and is only executed on nodes with the cache excluding
     * master-node where it is executed locally.
     *
     * @throws Exception If failed.
     */
    public void performTest() throws Exception {
        List<AtomicInteger> ctrs = new ArrayList<>(GRID_CNT);

        // Add task start event listeners to all grids.
        for (int i = 0; i < GRID_CNT; i++)
            ctrs.add(addTaskStartedEvtListener(grid(i)));

        // Put values into normal replicated cache.
        for (int i = 0; i < KEY_CNT; i++)
            grid(0).cache(CACHE_NAME).put(i, "val" + i);

        // Put values into a cache with another name.
        for (int i = 0; i < KEY_CNT_OTHER; i++)
            grid(GRID_CNT - 1).cache(CACHE_NAME_OTHER).put(i, "val" + i);

        // Check cache sizes.
        for (int i = 0; i < GRID_CNT - 1; i++) {
            GridCache<Object, Object> cache = grid(i).cache(CACHE_NAME);

            assertEquals("Key set [i=" + i + ", keys=" + cache.keySet() + ']', KEY_CNT, cache.size());
        }

        assert grid(GRID_CNT - 1).cache(CACHE_NAME_OTHER).size() == KEY_CNT_OTHER;

        // Perform clear.
        grid(0).cache(CACHE_NAME).globalClearAll();

        // Expect caches with the given name to be clear on all nodes.
        for (int i = 0; i < GRID_CNT - 1; i++)
            assert grid(i).cache(CACHE_NAME).isEmpty();

        // ... but cache with another name should remain untouched.
        assert grid(GRID_CNT - 1).cache(CACHE_NAME_OTHER).size() == KEY_CNT_OTHER;

        // All nodes listeners should have noticed globalClearAll() task execution except of grid(0)
        // which is the master node for this task and grid(GRID_CNT-1) which doesn't have affected cache.
        assert ctrs.get(0).get() == 0;

        for (int i = 1; i < GRID_CNT - 1; i++)
            assert ctrs.get(i).get() == 1;

        assert ctrs.get(GRID_CNT - 1).get() == 0;
    }

    /**
     * Attach {@link GlobalClearAllJobStartEventListener} to the grid.
     *
     * @param grid Grid listener to be attached to.
     * @return Counter passed to listener.
     */
    private AtomicInteger addTaskStartedEvtListener(GridProjection grid) {
        AtomicInteger ctr = new AtomicInteger();

        grid.events().localListen(new GlobalClearAllJobStartEventListener(ctr), EVT_JOB_STARTED);

        return ctr;
    }

    /**
     * Job start event listener which increments passed counter whenever globalClearAll-related job is executed.
     */
    private static class GlobalClearAllJobStartEventListener implements GridPredicate<GridEvent> {
        /** Counter to be incremented. */
        private final AtomicInteger ctr;

        /**
         * Standard constructor.
         *
         * @param ctr Counter.
         */
        private GlobalClearAllJobStartEventListener(AtomicInteger ctr) {
            this.ctr = ctr;
        }

        /** {@inheritDoc} */
        @Override public boolean apply(GridEvent evt) {
            assert evt.type() == EVT_JOB_STARTED;

            GridJobEvent jobEvt = (GridJobEvent)evt;

            if (jobEvt.taskClassName().contains("GlobalClearAllCallable"))
                ctr.incrementAndGet();

            return true;
        }
    }
}
