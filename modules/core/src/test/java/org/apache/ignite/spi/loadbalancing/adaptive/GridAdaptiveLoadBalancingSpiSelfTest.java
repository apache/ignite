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

package org.apache.ignite.spi.loadbalancing.adaptive;

import java.util.Collections;
import java.util.List;
import java.util.UUID;
import org.apache.ignite.GridTestJob;
import org.apache.ignite.GridTestTaskSession;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeTaskSession;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.testframework.GridSpiTestContext;
import org.apache.ignite.testframework.GridTestNode;
import org.apache.ignite.testframework.junits.spi.GridSpiAbstractTest;
import org.apache.ignite.testframework.junits.spi.GridSpiTest;
import org.apache.ignite.testframework.junits.spi.GridSpiTestConfig;

/**
 * Tests adaptive load balancing SPI.
 */
@GridSpiTest(spi = AdaptiveLoadBalancingSpi.class, group = "Load Balancing SPI")
public class GridAdaptiveLoadBalancingSpiSelfTest extends GridSpiAbstractTest<AdaptiveLoadBalancingSpi> {
    /** {@inheritDoc} */
    @Override protected GridSpiTestContext initSpiContext() throws Exception {
        GridSpiTestContext ctx = super.initSpiContext();

        ctx.setLocalNode(new GridTestNode(UUID.randomUUID()));

        return ctx;
    }

    /**
     * @return {@code True} if node weights should be considered.
     */
    @GridSpiTestConfig
    public AdaptiveLoadProbe getLoadProbe() {
        return new AdaptiveLoadProbe() {
            @Override public double getLoad(ClusterNode node, int jobsSentSinceLastUpdate) {
                boolean isFirstTime = node.attribute("used") == null;

                assert isFirstTime ? jobsSentSinceLastUpdate == 0 : jobsSentSinceLastUpdate > 0;

                return (Double)node.attribute("load");
            }
        };
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings({"ObjectEquality"})
    public void testSingleNodeZeroWeight() throws Exception {
        GridTestNode node = (GridTestNode)getSpiContext().nodes().iterator().next();

        node.addAttribute("load", 0d);

        List<ClusterNode> nodes = Collections.singletonList((ClusterNode)node);

        ComputeTaskSession ses = new GridTestTaskSession(IgniteUuid.randomUuid());

        GridTestNode pick1 = (GridTestNode)getSpi().getBalancedNode(ses, nodes, new GridTestJob());

        pick1.setAttribute("used", true);

        assert nodes.contains(pick1);

        // Verify that same instance is returned every time.
        ClusterNode pick2 = getSpi().getBalancedNode(ses, nodes, new GridTestJob());

        assert pick1 == pick2;
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings({"ObjectEquality"})
    public void testSingleNodeSameSession() throws Exception {
        GridTestNode node = (GridTestNode)getSpiContext().nodes().iterator().next();

        node.addAttribute("load", 1d);

        List<ClusterNode> nodes = Collections.singletonList((ClusterNode)node);

        ComputeTaskSession ses = new GridTestTaskSession(IgniteUuid.randomUuid());

        GridTestNode pick1 = (GridTestNode)getSpi().getBalancedNode(ses, nodes, new GridTestJob());

        pick1.setAttribute("used", true);

        assert nodes.contains(pick1);

        // Verify that same instance is returned every time.
        ClusterNode pick2 = getSpi().getBalancedNode(ses, nodes, new GridTestJob());

        assert pick1 == pick2;
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings({"ObjectEquality"})
    public void testSingleNodeDifferentSession() throws Exception {
        GridTestNode node = (GridTestNode)getSpiContext().nodes().iterator().next();

        node.addAttribute("load", 2d);

        List<ClusterNode> nodes = Collections.singletonList((ClusterNode)node);

        GridTestNode pick1 = (GridTestNode)getSpi().getBalancedNode(new GridTestTaskSession(IgniteUuid.randomUuid()),
            nodes, new GridTestJob());

        pick1.setAttribute("used", true);

        assert nodes.contains(pick1);

        // Verify that same instance is returned every time.
        ClusterNode pick2 = getSpi().getBalancedNode(new GridTestTaskSession(IgniteUuid.randomUuid()), nodes,
            new GridTestJob());

        assert pick1 == pick2;
    }
}