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

package org.apache.ignite.internal.processors.cache.distributed.near;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.Ignite;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.affinity.AffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteNodeAttributes;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteProductVersion;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.internal.TcpDiscoveryNode;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheRebalanceMode.NONE;

/**
 * Partitioned affinity test.
 */
@SuppressWarnings({"PointlessArithmeticExpression", "FieldCanBeLocal"})
public abstract class GridCacheAffinityFunctionExcludeNeighborsAbstractSelfTest extends GridCommonAbstractTest {
    /** Number of backups. */
    private int backups = 2;

    /** */
    private int gridInstanceNum;

    /** */
    private TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(final String gridName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(gridName);

        // Override node attributes in discovery spi.
        TcpDiscoverySpi spi = new TcpDiscoverySpi() {
            @Override public void setNodeAttributes(Map<String, Object> attrs,
                IgniteProductVersion ver) {
                super.setNodeAttributes(attrs, ver);

                // Set unique mac addresses for every group of three nodes.
                String macAddrs = "MOCK_MACS_" + (gridInstanceNum / 3);

                attrs.put(IgniteNodeAttributes.ATTR_MACS, macAddrs);

                gridInstanceNum++;
            }
        };

        spi.setIpFinder(ipFinder);

        c.setDiscoverySpi(spi);

        CacheConfiguration cc = defaultCacheConfiguration();

        cc.setCacheMode(PARTITIONED);

        cc.setBackups(backups);

        cc.setAffinity(affinityFunction());

        cc.setRebalanceMode(NONE);

        c.setCacheConfiguration(cc);

        return c;
    }

    /**
     * @return Affinity function for test.
     */
    protected abstract AffinityFunction affinityFunction();

    /**
     * @param aff Affinity.
     * @param key Key.
     * @return Nodes.
     */
    private static Collection<? extends ClusterNode> nodes(Affinity<Object> aff, Object key) {
        return aff.mapKeyToPrimaryAndBackups(key);
    }

    /**
     * @throws Exception If failed.
     */
    public void testAffinityMultiNode() throws Exception {
        int grids = 9;

        startGrids(grids);

        try {
            Object key = 12345;

            int copies = backups + 1;

            for (int i = 0; i < grids; i++) {
                final Ignite g = grid(i);

                Affinity<Object> aff = g.affinity(null);

                List<TcpDiscoveryNode> top = new ArrayList<>();

                for (ClusterNode node : g.cluster().nodes())
                    top.add((TcpDiscoveryNode) node);

                Collections.sort(top);

                assertEquals(grids, top.size());

                int idx = 1;

                for (ClusterNode n : top) {
                    assertEquals(idx, n.order());

                    idx++;
                }

                Collection<? extends ClusterNode> affNodes = nodes(aff, key);

                info("Affinity picture for grid [i=" + i + ", aff=" + U.toShortString(affNodes));

                assertEquals(copies, affNodes.size());

                Set<String> macs = new HashSet<>();

                for (ClusterNode node : affNodes)
                    macs.add((String)node.attribute(IgniteNodeAttributes.ATTR_MACS));

                assertEquals(copies, macs.size());
            }
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testAffinitySingleNode() throws Exception {
        Ignite g = startGrid();

        try {
            Object key = 12345;

            Collection<? extends ClusterNode> affNodes = nodes(g.affinity(null), key);

            info("Affinity picture for grid: " + U.toShortString(affNodes));

            assertEquals(1, affNodes.size());
        }
        finally {
            stopAllGrids();
        }
    }
}