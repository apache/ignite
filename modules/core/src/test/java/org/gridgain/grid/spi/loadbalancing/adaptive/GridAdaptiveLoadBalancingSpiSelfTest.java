/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.loadbalancing.adaptive;

import org.apache.ignite.cluster.*;
import org.apache.ignite.compute.*;
import org.apache.ignite.lang.*;
import org.gridgain.grid.*;
import org.gridgain.testframework.*;
import org.gridgain.testframework.junits.spi.*;

import java.util.*;

/**
 * Tests adaptive load balancing SPI.
 */
@GridSpiTest(spi = GridAdaptiveLoadBalancingSpi.class, group = "Load Balancing SPI")
public class GridAdaptiveLoadBalancingSpiSelfTest extends GridSpiAbstractTest<GridAdaptiveLoadBalancingSpi> {
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
    public GridAdaptiveLoadProbe getLoadProbe() {
        return new GridAdaptiveLoadProbe() {
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
