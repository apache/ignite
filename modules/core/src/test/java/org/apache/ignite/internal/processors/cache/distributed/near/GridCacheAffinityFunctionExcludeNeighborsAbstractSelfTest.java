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

import org.apache.ignite.*;
import org.apache.ignite.cache.affinity.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.internal.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.apache.ignite.testframework.junits.common.*;

import java.util.*;

import static org.apache.ignite.cache.CacheMode.*;
import static org.apache.ignite.cache.CacheRebalanceMode.*;

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
            @Override public void setNodeAttributes(Map<String, Object> attrs, IgniteProductVersion ver) {
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
    protected abstract CacheAffinityFunction affinityFunction();

    /**
     * @param aff Affinity.
     * @param key Key.
     * @return Nodes.
     */
    private static Collection<? extends ClusterNode> nodes(CacheAffinity<Object> aff, Object key) {
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

                CacheAffinity<Object> aff = g.affinity(null);

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
