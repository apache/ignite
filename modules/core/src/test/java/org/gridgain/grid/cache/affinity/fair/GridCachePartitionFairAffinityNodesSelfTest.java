/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.cache.affinity.fair;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.testframework.junits.common.*;

import java.util.*;

/**
 * Tests partition fair affinity in real grid.
 */
public class GridCachePartitionFairAffinityNodesSelfTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final GridTcpDiscoveryIpFinder IP_FINDER = new GridTcpDiscoveryVmIpFinder(true);

    /** Number of backups. */
    private int backups;

    /** Number of partitions. */
    private int parts = 512;

    /** Add nodes test. */
    private static final boolean[] ADD_ONLY = new boolean[] {true, true, true, true, true, true};

    /** Add nodes test. */
    private static final boolean[] ADD_REMOVE = new boolean[]
        {
            true,  true,  true,  true,  true, true,
            false, false, false, false, false
        };

    /** */
    private static final boolean[] MIXED1 = new boolean[]
        {
            // 1     2     3      2     3     4      3     4     5      4      3      2
            true, true, true, false, true, true, false, true, true, false, false, false
        };

    /** */
    private static final boolean[] MIXED2 = new boolean[]
        {
            // 1     2     3      2      1     2      1     2     3      2      1     2
            true, true, true, false, false, true, false, true, true, false, false, true
        };

    /** */
    private static final boolean[] MIXED3 = new boolean[]
        {
            // 1     2     3     4     5     6      5     6     7     8     9      8      7     8     9
            true, true, true, true, true, true, false, true, true, true, true, false, false, true, true,
            //  8      7      6
            false, false, false
        };

    /** {@inheritDoc} */
    @Override protected GridConfiguration getConfiguration(String gridName) throws Exception {
        GridConfiguration cfg = super.getConfiguration(gridName);

        GridCacheConfiguration ccfg = cacheConfiguration();

        cfg.setCacheConfiguration(ccfg);

        GridTcpDiscoverySpi discoSpi = new GridTcpDiscoverySpi();

        discoSpi.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(discoSpi);

        return cfg;
    }

    /**
     * @return Cache configuration.
     */
    private GridCacheConfiguration cacheConfiguration() {
        GridCacheConfiguration cfg = new GridCacheConfiguration();

        cfg.setBackups(backups);

        cfg.setCacheMode(GridCacheMode.PARTITIONED);

        cfg.setDistributionMode(GridCacheDistributionMode.PARTITIONED_ONLY);

        cfg.setAffinity(new GridCachePartitionFairAffinity(parts));

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testAdd() throws Exception {
        checkSequence(ADD_ONLY);
    }

    /**
     * @throws Exception If failed.
     */
    public void testAddRemove() throws Exception {
        checkSequence(ADD_REMOVE);
    }

    /**
     * @throws Exception If failed.
     */
    public void testMixed1() throws Exception {
        checkSequence(MIXED1);
    }

    /**
     * @throws Exception If failed.
     */
    public void testMixed2() throws Exception {
        checkSequence(MIXED2);
    }

    /**
     * @throws Exception If failed.
     */
    public void testMixed3() throws Exception {
        checkSequence(MIXED3);
    }

    /**
     * @throws Exception If failed.
     */
    private void checkSequence(boolean[] seq) throws Exception {
        for (int b = 0; b < 3; b++) {
            backups = b;

            info(">>>>>>>>>>>>>>>> Checking backups: " + backups);

            checkSequence0(seq);

            info(">>>>>>>>>>>>>>>> Finished check: " + backups);
        }
    }

    /**
     * @param seq Start/stop sequence.
     * @throws Exception If failed.
     */
    private void checkSequence0(boolean[] seq) throws Exception {
        try {
            startGrid(0);

            TreeSet<Integer> started = new TreeSet<>();

            started.add(0);

            int topVer = 1;

            for (boolean start : seq) {
                if (start) {
                    int nextIdx = nextIndex(started);

                    startGrid(nextIdx);

                    started.add(nextIdx);
                }
                else {
                    int idx = started.last();

                    stopGrid(idx);

                    started.remove(idx);
                }

                topVer++;

                info("Grid 0: " + grid(0).localNode().id());

                ((GridKernal)grid(0)).internalCache().context().affinity().affinityReadyFuture(topVer).get();

                for (int i : started) {
                    if (i != 0) {
                        GridEx grid = grid(i);

                        ((GridKernal)grid).internalCache().context().affinity().affinityReadyFuture(topVer).get();

                        info("Grid " + i + ": " + grid.localNode().id());

                        for (int part = 0; part < parts; part++) {
                            List<ClusterNode> firstNodes = (List<ClusterNode>)grid(0).cache(null).affinity()
                                .mapPartitionToPrimaryAndBackups(part);

                            List<ClusterNode> secondNodes = (List<ClusterNode>)grid.cache(null).affinity()
                                .mapPartitionToPrimaryAndBackups(part);

                            assertEquals(firstNodes.size(), secondNodes.size());

                            for (int n = 0; n < firstNodes.size(); n++)
                                assertEquals(firstNodes.get(n), secondNodes.get(n));
                        }
                    }
                }
            }
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * First positive integer that is not present in started set.
     *
     * @param started Already started indices.
     * @return First positive integer that is not present in started set.
     */
    private int nextIndex(Collection<Integer> started) {
        assert started.contains(0);

        for (int i = 1; i < 10000; i++) {
            if (!started.contains(i))
                return i;
        }

        throw new IllegalStateException();
    }
}
