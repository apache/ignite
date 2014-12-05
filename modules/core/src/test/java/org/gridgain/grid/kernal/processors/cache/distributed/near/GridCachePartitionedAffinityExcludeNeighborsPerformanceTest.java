/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.distributed.near;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.configuration.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.affinity.*;
import org.gridgain.grid.cache.affinity.consistenthash.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.grid.util.*;
import org.gridgain.testframework.junits.common.*;

import java.util.*;

import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.gridgain.grid.cache.GridCachePreloadMode.*;

/**
 * Partitioned affinity test.
 */
@SuppressWarnings({"PointlessArithmeticExpression"})
public class GridCachePartitionedAffinityExcludeNeighborsPerformanceTest extends GridCommonAbstractTest {
    /** Grid count. */
    private static final int GRIDS = 3;

    /** Random number generator. */
    private static final Random RAND = new Random();

    /** */
    private boolean excNeighbores;

    /** */
    private GridTcpDiscoveryIpFinder ipFinder = new GridTcpDiscoveryVmIpFinder(true);

    /** */
    private static Collection<String> msgs = new ArrayList<>();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(gridName);

        TcpDiscoverySpi spi = new TcpDiscoverySpi();

        spi.setIpFinder(ipFinder);

        c.setDiscoverySpi(spi);

        GridCacheConfiguration cc = defaultCacheConfiguration();

        cc.setCacheMode(PARTITIONED);

        cc.setBackups(2);

        GridCacheAffinityFunction aff = new GridCacheConsistentHashAffinityFunction(excNeighbores);

        cc.setAffinity(aff);

        cc.setPreloadMode(NONE);

        c.setCacheConfiguration(cc);

        return c;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        msgs.clear();
    }

    /**
     * @param ignite Grid.
     * @return Affinity.
     */
    static GridCacheAffinity<Object> affinity(Ignite ignite) {
        return ignite.cache(null).affinity();
    }

    /**
     * @param aff Affinity.
     * @param key Key.
     * @return Nodes.
     */
    private static Collection<? extends ClusterNode> nodes(GridCacheAffinity<Object> aff, Object key) {
        return aff.mapKeyToPrimaryAndBackups(key);
    }

    /**
     * @throws Exception If failed.
     */
    public void testCountPerformance() throws Exception {
        excNeighbores = false;

        int cnt = 1000000;

        long t1 = checkCountPerformance(cnt, "includeNeighbors");

        System.gc();

        excNeighbores = true;

        long t2 = checkCountPerformance(cnt, "excludeNeighbors");

        for (String msg : msgs)
            info(msg);

        info(">>> t2/t1: " + (t2/t1));
    }

    /**
     * @param cnt Count.
     * @param testName Test name.
     * @return Duration.
     * @throws Exception If failed.
     */
    private long checkCountPerformance(int cnt, String testName) throws Exception {
        startGridsMultiThreaded(GRIDS);

        try {
            Ignite g = grid(0);

            // Warmup.
            checkCountPerformance0(g, 10000);

            info(">>> Starting count based test [testName=" + testName + ", cnt=" + cnt + ']');

            long dur = checkCountPerformance0(g, cnt);

            String msg = ">>> Performance [testName=" + testName + ", cnt=" + cnt + ", duration=" + dur + "ms]";

            info(">>> ");
            info(msg);
            info(">>> ");

            msgs.add(msg);

            return dur;
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     *
     * @param g Grid.
     * @param cnt Count.
     * @return Result.
     * @throws Exception If failed.
     */
    private long checkCountPerformance0(Ignite g, int cnt) throws Exception {
        GridCacheAffinity<Object> aff = affinity(g);

        GridTimer timer = new GridTimer("test");

        for (int i = 0; i < cnt; i++) {
            Object key = RAND.nextInt(Integer.MAX_VALUE);

            Collection<? extends ClusterNode> affNodes = nodes(aff, key);

            assert excNeighbores ? affNodes.size() == 1 : affNodes.size() == GRIDS;
        }

        timer.stop();

        return timer.duration();
    }

    /**
     * @throws Exception If failed.
     */
    public void testTimedPerformance() throws Exception {
        excNeighbores = false;

        long dur = 15000;

        int cnt1 = checkTimedPerformance(dur, "includeNeighbors");

        System.gc();

        excNeighbores = true;

        int cnt2 = checkTimedPerformance(dur, "excludeNeighbors");

        for (String msg : msgs)
            info(msg);

        info(">>> cnt1/cnt2=" + (cnt1/cnt2));
    }

    /**
     * @param dur Duration.
     * @param testName Test name.
     * @return Number of operations.
     * @throws Exception If failed.
     */
    private int checkTimedPerformance(long dur, String testName) throws Exception {
        startGridsMultiThreaded(GRIDS);

        try {
            Ignite g = grid(0);

            GridCacheAffinity<Object> aff = affinity(g);

            // Warmup.
            checkCountPerformance0(g, 10000);

            info(">>> Starting timed based test [testName=" + testName + ", duration=" + dur + ']');

            int cnt = 0;

            for (long t = System.currentTimeMillis(); cnt % 1000 != 0 || System.currentTimeMillis() - t < dur;) {
                Object key = RAND.nextInt(Integer.MAX_VALUE);

                Collection<? extends ClusterNode> affNodes = nodes(aff, key);

                assert excNeighbores ? affNodes.size() == 1 : affNodes.size() == GRIDS;

                cnt++;
            }

            String msg = ">>> Performance [testName=" + testName + ", duration=" + dur + "ms, cnt=" + cnt + ']';

            info(">>> ");
            info(msg);
            info(">>> ");

            msgs.add(msg);

            return cnt;
        }
        finally {
            stopAllGrids();
        }
    }
}
