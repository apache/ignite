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

package org.apache.ignite.spi.discovery.tcp;

import java.util.Collection;
import java.util.Optional;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManager;
import org.apache.ignite.spi.discovery.tcp.internal.TcpDiscoveryNode;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Tests that {@link TcpDiscoveryNode} that has already left cluster,
 * doesn't have resolved socket addresses in toplogy history that a new joining node receives.
 */
public class TcpDiscoveryDeadNodeAddressResolvingTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration configuration = super.getConfiguration(igniteInstanceName);

        configuration.setConsistentId(igniteInstanceName);

        return configuration;
    }

    /**
     * Tests that a new node that joins the topology and receives topology history,
     * doesn't resolve addresses of nodes that already left the topology.
     *
     * @throws Exception If failed.
     */
    @Test
    public void test() throws Exception {
        startGrid(0);

        IgniteEx cli = startClientGrid(1);
        String consistentId = (String)cli.configuration().getConsistentId();

        startGrid(2);

        cli.close();

        IgniteEx grid3 = startGrid(3);

        GridDiscoveryManager discovery = grid3.context().discovery();
        // Only grid0.
        checkNoNode(discovery, 1, consistentId);

        // grid1 must be in topology snapshots but as it has already left the cluster, its address should not be resolved.
        checkSockAddrsNull(discovery, 2, consistentId);
        checkSockAddrsNull(discovery, 3, consistentId);

        // grid1 will not be present in these topology snapshots.
        checkNoNode(discovery, 4, consistentId);
        checkNoNode(discovery, 5, consistentId);
    }

    /**
     * Checks that there is no node by consistent id in the topology history snapshot.
     *
     * @param disco Discovery manager.
     * @param topology Topology version.
     * @param consistentId Node consistent id.
     */
    private void checkNoNode(GridDiscoveryManager disco, int topology, String consistentId) {
        assertFalse(findNode(disco, topology, consistentId).isPresent());
    }

    /**
     * Checks that {@link TcpDiscoveryNode} from the topology snapshot doesn't have resolved socket addresses.
     *
     * @param disco Discovery manager.
     * @param topology Topology version.
     * @param consistentId Node consistent id.
     * @throws Exception If failed.
     */
    private void checkSockAddrsNull(GridDiscoveryManager disco, int topology, String consistentId) throws Exception {
        Optional<ClusterNode> node = findNode(disco, topology, consistentId);

        assertTrue(node.isPresent());

        ClusterNode clusterNode = node.get();

        Object sockAddrs = GridTestUtils.getFieldValue(clusterNode, "sockAddrs");
        assertNull(sockAddrs);
    }

    /**
     * Finds node by consistent id in the topology history snapshot.
     *
     * @param disco Discovery manager.
     * @param topology Topology version.
     * @param consistentId Node consistent id.
     * @return Node optional.
     */
    private Optional<ClusterNode> findNode(GridDiscoveryManager disco, int topology, String consistentId) {
        Collection<ClusterNode> nodes = disco.topology(topology);

        return nodes.stream().filter(node -> node.consistentId().equals(consistentId)).findFirst();
    }
}
