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

package org.apache.ignite.spi.loadbalancing.roundrobin;

import java.util.Collections;
import org.apache.ignite.GridTestJob;
import org.apache.ignite.GridTestTaskSession;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.testframework.junits.spi.GridSpiAbstractTest;
import org.apache.ignite.testframework.junits.spi.GridSpiTest;

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