/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.affinity;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.affinity.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.testframework.*;
import org.gridgain.testframework.junits.common.*;

import java.util.*;
import java.util.concurrent.*;

import static org.gridgain.grid.cache.GridCacheMode.*;

/**
 * Tests for {@link GridAffinityProcessor}.
 */
@GridCommonTest(group = "Affinity Processor")
public abstract class GridAffinityProcessorAbstractSelfTest extends GridCommonAbstractTest {
    /** Number of grids started for tests. Should not be less than 2. */
    private static final int NODES_CNT = 3;

    /** Cache name. */
    private static final String CACHE_NAME = "cache";

    /** IP finder. */
    private static final GridTcpDiscoveryIpFinder ipFinder = new GridTcpDiscoveryVmIpFinder(true);

    /** Flag to start grid with cache. */
    private boolean withCache;

    /** {@inheritDoc} */
    @Override protected GridConfiguration getConfiguration(String gridName) throws Exception {
        GridConfiguration cfg = super.getConfiguration(gridName);

        GridTcpDiscoverySpi discoSpi = new GridTcpDiscoverySpi();

        discoSpi.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(discoSpi);

        if (withCache) {
            GridCacheConfiguration cacheCfg = defaultCacheConfiguration();

            cacheCfg.setName(CACHE_NAME);
            cacheCfg.setCacheMode(PARTITIONED);
            cacheCfg.setBackups(1);
            cacheCfg.setAffinity(affinityFunction());

            cfg.setCacheConfiguration(cacheCfg);
        }

        return cfg;
    }

    /**
     * Creates affinity function for test.
     *
     * @return Affinity function.
     */
    protected abstract GridCacheAffinityFunction affinityFunction();

    /** {@inheritDoc} */
    @SuppressWarnings({"ConstantConditions"})
    @Override protected void beforeTestsStarted() throws Exception {
        assert NODES_CNT >= 1;

        withCache = false;

        for (int i = 0; i < NODES_CNT; i++)
            startGrid(i);

        withCache = true;

        for (int i = NODES_CNT; i < 2 * NODES_CNT; i++)
            startGrid(i);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /**
     * Test affinity functions caching and clean up.
     *
     * @throws Exception In case of any exception.
     */
    @SuppressWarnings("AssertEqualsBetweenInconvertibleTypes")
    public void testAffinityProcessor() throws Exception {
        Random rnd = new Random();

        GridKernal grid1 = (GridKernal)grid(rnd.nextInt(NODES_CNT)); // With cache.
        GridKernal grid2 = (GridKernal)grid(NODES_CNT + rnd.nextInt(NODES_CNT)); // Without cache.

        assertEquals(NODES_CNT * 2, grid1.nodes().size());
        assertEquals(NODES_CNT * 2, grid2.nodes().size());

        assertNull(grid1.cache(CACHE_NAME));
        assertNotNull(grid2.cache(CACHE_NAME));

        GridAffinityProcessor affPrc1 = grid1.context().affinity();
        GridAffinityProcessor affPrc2 = grid2.context().affinity();

        ConcurrentMap<String, GridFuture<GridAffinityCache>> affMap1 =
            GridTestUtils.getFieldValue(affPrc1, "affMap");

        ConcurrentMap<String, GridFuture<GridAffinityCache>> affMap2 =
            GridTestUtils.getFieldValue(affPrc2, "affMap");

        assertEquals("Validate initial state of affinity cache.", Collections.emptyMap(), affMap1);
        assertEquals("Validate initial state of affinity cache.", Collections.emptyMap(), affMap1);

        // Create keys collection.
        Collection<Integer> keys = new ArrayList<>(1000);

        for (int i = 0; i < 1000; i++)
            keys.add(i);

        //
        // Validate affinity functions collection updated on first call.
        //

        affPrc1.mapKeysToNodes(CACHE_NAME, keys);

        assertEquals(1, affMap1.size());
        assertEquals(0, affMap2.size());

        GridFuture<GridAffinityCache> t1 = affMap1.get(CACHE_NAME);

        assertNotNull(t1);
        assertNotNull(t1.get());
        assertTrue(t1.isDone());

        affPrc2.mapKeysToNodes(CACHE_NAME, keys);

        assertEquals(1, affMap1.size());
        assertEquals(1, affMap2.size());

        GridFuture<GridAffinityCache> t2 = affMap2.get(CACHE_NAME);

        assertNotNull(t2);
        assertNotNull(t2.get());
        assertTrue(t2.isDone());

        //
        // Validate affinity caches are clearen on topology update.
        //

        ConcurrentMap<Long, Object> affCache1 = GridTestUtils.getFieldValue(t1.get(), "affCache");
        ConcurrentMap<Long, Object> affCache2 = GridTestUtils.getFieldValue(t2.get(), "affCache");

        long topVer = grid1.context().discovery().topologyVersion();

        assertNotNull(affCache1.get(topVer));
        assertNotNull(affCache2.get(topVer));

        withCache = true;

        Grid last = startGrid(NODES_CNT * 2); // Add node with cache.

        assertEquals(NODES_CNT * 2 + 1, last.nodes().size());

        assertNull("Expect cache cleaned up on topology update: " + affCache1, affCache1.get(topVer));
        assertNull("Expect cache cleaned up on topology update: " + affCache2, affCache2.get(topVer));

        //
        // Validate affinity function is cleaned from processor collection,
        // when all cache nodes with such name leave topology.
        //

        for (int i = NODES_CNT; i < NODES_CNT * 2 + 1; i++)
            stopGrid(i); // Stop all cache nodes.

        long cleanUpDelay = GridTestUtils.<Long>getFieldValue(
            GridAffinityProcessor.class, "AFFINITY_MAP_CLEAN_UP_DELAY");

        Thread.sleep(cleanUpDelay * 6 / 5); // +20% time.

        assertEquals("All cache nodes leave topology.", Collections.emptyMap(), affMap1);
    }

    /**
     * Test performance of affinity processor.
     *
     * @throws Exception In case of any exception.
     */
    public void testPerformance() throws Exception {
        GridKernal grid = (GridKernal)grid(0);
        GridAffinityProcessor aff = grid.context().affinity();

        int keysSize = 1000000;

        Collection<Integer> keys = new ArrayList<>(keysSize);

        for (int i = 0; i < keysSize; i++)
            keys.add(i);

        long start = System.currentTimeMillis();

        int iterations = 10000000;

        for (int i = 0; i < iterations; i++)
            aff.mapKeyToNode(keys);

        long diff = System.currentTimeMillis() - start;

        info(">>> Map " + keysSize + " keys to " + grid.nodes().size() + " nodes " + iterations + " times in " + diff + "ms.");

        assertTrue(diff < 25000);
    }
}
