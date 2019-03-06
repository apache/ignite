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
import org.junit.Test;

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
    @Test
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
