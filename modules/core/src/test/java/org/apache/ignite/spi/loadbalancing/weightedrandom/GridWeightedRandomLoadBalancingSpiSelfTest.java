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

package org.apache.ignite.spi.loadbalancing.weightedrandom;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import org.apache.ignite.GridTestJob;
import org.apache.ignite.GridTestTaskSession;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.testframework.GridTestNode;
import org.apache.ignite.testframework.junits.spi.GridSpiAbstractTest;
import org.apache.ignite.testframework.junits.spi.GridSpiTest;
import org.junit.Test;

/**
 * Weighted random load balancing SPI.
 */
@GridSpiTest(spi = WeightedRandomLoadBalancingSpi.class, group = "Load Balancing SPI")
public class GridWeightedRandomLoadBalancingSpiSelfTest extends
    GridSpiAbstractTest<WeightedRandomLoadBalancingSpi> {
    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings({"ObjectEquality"})
    @Test
    public void testSingleNode() throws Exception {
        List<ClusterNode> nodes = Collections.singletonList((ClusterNode)new GridTestNode(UUID.randomUUID()));

        ClusterNode node = getSpi().getBalancedNode(new GridTestTaskSession(), nodes, new GridTestJob());

        assert nodes.contains(node);

        // Verify that same instance is returned every time.
        ClusterNode balancedNode = getSpi().getBalancedNode(new GridTestTaskSession(), nodes, new GridTestJob());

        assert node == balancedNode;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testMultipleNodes() throws Exception {
        List<ClusterNode> nodes = new ArrayList<>();

        for (int i = 0; i < 10; i++)
            nodes.add(new GridTestNode(UUID.randomUUID()));

        // Seal it.
        nodes = Collections.unmodifiableList(nodes);

        ClusterNode node = getSpi().getBalancedNode(new GridTestTaskSession(), nodes, new GridTestJob());

        assert node != null;
        assert nodes.contains(node);
    }
}
