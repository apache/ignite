/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.cache.affinity;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.Ignite;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteNodeAttributes;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteProductVersion;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheRebalanceMode.NONE;

/**
 * Partitioned affinity test.
 */
public abstract class AffinityFunctionExcludeNeighborsAbstractSelfTest extends GridCommonAbstractTest {
    /** Number of backups. */
    private int backups = 2;

    /** Number of girds. */
    private int gridInstanceNum;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(final String igniteInstanceName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(igniteInstanceName);

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

        spi.setIpFinder(sharedStaticIpFinder);

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
    @Test
    public void testAffinityMultiNode() throws Exception {
        int grids = 9;

        startGrids(grids);

        try {
            Object key = 12345;

            int copies = backups + 1;

            for (int i = 0; i < grids; i++) {
                final Ignite g = grid(i);

                Affinity<Object> aff = g.affinity(DEFAULT_CACHE_NAME);

                List<ClusterNode> top = new ArrayList<>(g.cluster().nodes());

                Collections.sort((List)top);

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
    @Test
    public void testAffinitySingleNode() throws Exception {
        Ignite g = startGrid();

        try {
            Object key = 12345;

            Collection<? extends ClusterNode> affNodes = nodes(g.affinity(DEFAULT_CACHE_NAME), key);

            info("Affinity picture for grid: " + U.toShortString(affNodes));

            assertEquals(1, affNodes.size());
        }
        finally {
            stopAllGrids();
        }
    }
}
