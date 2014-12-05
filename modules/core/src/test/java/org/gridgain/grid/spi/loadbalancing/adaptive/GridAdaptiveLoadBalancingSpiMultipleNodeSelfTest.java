/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.loadbalancing.adaptive;

import org.apache.ignite.cluster.*;
import org.apache.ignite.lang.*;
import org.gridgain.grid.*;
import org.gridgain.testframework.*;
import org.gridgain.testframework.junits.spi.*;
import java.util.*;

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
