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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import org.apache.ignite.GridTestJob;
import org.apache.ignite.GridTestTaskSession;
import org.apache.ignite.cluster.ClusterNode;
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
public class GridAdaptiveLoadBalancingSpiMultipleNodeSelfTest extends GridSpiAbstractTest<AdaptiveLoadBalancingSpi> {
    /** */
    private static final int RMT_NODE_CNT = 10;

    /** {@inheritDoc} */
    @Override protected GridSpiTestContext initSpiContext() throws Exception {
        GridSpiTestContext ctx = super.initSpiContext();

        for (int i = 0; i < RMT_NODE_CNT; i++) {
            GridTestNode node = new GridTestNode(UUID.randomUUID());

            node.setAttribute("load", (double)(i + 1));

            ctx.addNode(node);
        }

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
    public void testWeights() throws Exception {
        // Seal it.
        List<ClusterNode> nodes = new ArrayList<>(getSpiContext().remoteNodes());

        int[] cnts = new int[RMT_NODE_CNT];

        // Invoke load balancer a large number of times, so statistics won't lie.
        for (int i = 0; i < 50000; i++) {
            GridTestNode node = (GridTestNode)getSpi().getBalancedNode(new GridTestTaskSession(IgniteUuid.randomUuid()),
                nodes, new GridTestJob());

            int idx = ((Double)node.attribute("load")).intValue() - 1;

            if (cnts[idx] == 0)
                node.setAttribute("used", true);

            // Increment number of times a node was picked.
            cnts[idx]++;
        }

        info("Node counts: " + Arrays.toString(cnts));

        for (int i = 0; i < cnts.length - 1; i++) {
            assert cnts[i] > cnts[i + 1] : "Invalid node counts for index [idx=" + i + ", cnts[i]=" + cnts[i] +
                ", cnts[i+1]=" + cnts[i + 1] + ']';
        }
    }
}