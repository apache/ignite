/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.cache.affinity.fair;

import java.util.Collection;
import java.util.List;
import java.util.TreeSet;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Tests partition fair affinity in real grid.
 */
public class GridFairAffinityFunctionNodesSelfTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

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
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        CacheConfiguration ccfg = cacheConfiguration();

        cfg.setCacheConfiguration(ccfg);

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

        discoSpi.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(discoSpi);

        return cfg;
    }

    /**
     * @return Cache configuration.
     */
    private CacheConfiguration cacheConfiguration() {
        CacheConfiguration cfg = new CacheConfiguration();

        cfg.setBackups(backups);

        cfg.setCacheMode(CacheMode.PARTITIONED);

        cfg.setNearConfiguration(null);

        cfg.setAffinity(new FairAffinityFunction(parts));

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

                ((IgniteKernal)grid(0)).internalCache().context().affinity().affinityReadyFuture(topVer).get();

                for (int i : started) {
                    if (i != 0) {
                        IgniteEx grid = grid(i);

                        ((IgniteKernal)grid).internalCache().context().affinity().affinityReadyFuture(topVer).get();

                        info("Grid " + i + ": " + grid.localNode().id());

                        for (int part = 0; part < parts; part++) {
                            List<ClusterNode> firstNodes = (List<ClusterNode>)grid(0).affinity(null)
                                .mapPartitionToPrimaryAndBackups(part);

                            List<ClusterNode> secondNodes = (List<ClusterNode>)grid.affinity(null)
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