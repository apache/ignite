/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.spi.loadbalancing.roundrobin;

import java.util.Collections;
import org.apache.ignite.GridTestJob;
import org.apache.ignite.GridTestTaskSession;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.testframework.junits.spi.GridSpiAbstractTest;
import org.apache.ignite.testframework.junits.spi.GridSpiTest;
import org.junit.Test;

/**
 * Tests Round Robin load balancing for single node.
 */
@GridSpiTest(spi = RoundRobinLoadBalancingSpi.class, group = "Load Balancing SPI", triggerDiscovery = true)
public class GridRoundRobinLoadBalancingSpiLocalNodeSelfTest extends
    GridSpiAbstractTest<RoundRobinLoadBalancingSpi> {
    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings({"ObjectEquality"})
    @Test
    public void testLocalNode() throws Exception {
        assert getDiscoverySpi().getRemoteNodes().isEmpty();

        ClusterNode locNode = getDiscoverySpi().getLocalNode();

        ClusterNode node = getSpi().getBalancedNode(new GridTestTaskSession(IgniteUuid.randomUuid()),
            Collections.singletonList(locNode), new GridTestJob());

        assert  node == locNode;

        // Double check.
        node = getSpi().getBalancedNode(new GridTestTaskSession(IgniteUuid.randomUuid()),
            Collections.singletonList(locNode), new GridTestJob());

        assert node == locNode;
    }
}
