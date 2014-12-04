/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.loadbalancing.roundrobin;

import org.apache.ignite.cluster.*;
import org.apache.ignite.compute.*;
import org.apache.ignite.events.*;
import org.apache.ignite.lang.*;
import org.gridgain.grid.*;
import org.gridgain.testframework.*;
import org.gridgain.testframework.junits.spi.*;

import java.util.*;

import static org.apache.ignite.events.IgniteEventType.*;

/**
 * Tests round robin load balancing SPI.
 */
@GridSpiTest(spi = GridRoundRobinLoadBalancingSpi.class, group = "Load Balancing SPI")
public class GridRoundRobinLoadBalancingSpiMultipleNodesSelfTest
    extends GridSpiAbstractTest<GridRoundRobinLoadBalancingSpi> {
    /** {@inheritDoc} */
    @Override protected void spiConfigure(GridRoundRobinLoadBalancingSpi spi) throws Exception {
        super.spiConfigure(spi);

        spi.setPerTask(true);
    }

    /** {@inheritDoc} */
    @Override protected GridSpiTestContext initSpiContext() throws Exception {
        GridSpiTestContext spiCtx = super.initSpiContext();

        spiCtx.createLocalNode();
        spiCtx.createRemoteNodes(10);

        return spiCtx;
    }

    /**
     * @throws Exception If test failed.
     */
    @SuppressWarnings({"ObjectEquality"})
    public void testMultipleNodes() throws Exception {
        List<ClusterNode> allNodes = (List<ClusterNode>)getSpiContext().nodes();

        ComputeTaskSession ses = new GridTestTaskSession(IgniteUuid.randomUuid());

        // Initialize.
        getSpi().getBalancedNode(ses, allNodes, new GridTestJob());

        List<UUID> orderedNodes = new ArrayList<>(getSpi().getNodeIds(ses));

        // Check the round-robin actually did circle.
        for (int i = 0; i < allNodes.size(); i++) {
            ClusterNode node = getSpi().getBalancedNode(ses, allNodes, new GridTestJob());

            assert orderedNodes.get(i) == node.id();
        }

        // Double-check.
        for (int i = 0; i < allNodes.size(); i++) {
            ClusterNode node = getSpi().getBalancedNode(ses, allNodes, new GridTestJob());

            assert orderedNodes.get(i) == node.id();
        }
    }

    /**
     * @throws Exception If test failed.
     */
    @SuppressWarnings({"ObjectEquality"})
    public void testMultipleTasks() throws Exception {
        ComputeTaskSession ses1 = new GridTestTaskSession(IgniteUuid.randomUuid());
        ComputeTaskSession ses2 = new GridTestTaskSession(IgniteUuid.randomUuid());

        List<ClusterNode> allNodes = (List<ClusterNode>)getSpiContext().nodes();

        // Initialize.
        getSpi().getBalancedNode(ses1, allNodes, new GridTestJob());
        getSpi().getBalancedNode(ses2, allNodes, new GridTestJob());

        List<UUID> orderedNodes1 = getSpi().getNodeIds(ses1);
        List<UUID> orderedNodes2 = getSpi().getNodeIds(ses2);

        assert orderedNodes1 != orderedNodes2;

        // Check the round-robin actually did circle.
        for (int i = 0; i < allNodes.size(); i++) {
            ClusterNode node1 = getSpi().getBalancedNode(ses1, allNodes, new GridTestJob());

            assert orderedNodes1.get(i) == node1.id();

            ClusterNode node2 = getSpi().getBalancedNode(ses2, allNodes, new GridTestJob());

            assert orderedNodes2.get(i) == node2.id();

            assert orderedNodes1.get(i) == orderedNodes2.get(i);
        }

        // Double-check.
        for (int i = 0; i < allNodes.size(); i++) {
            ClusterNode node1 = getSpi().getBalancedNode(ses1, allNodes, new GridTestJob());

            assert orderedNodes1.get(i) == node1.id();

            ClusterNode node2 = getSpi().getBalancedNode(ses2, allNodes, new GridTestJob());

            assert orderedNodes2.get(i) == node2.id();

            assert orderedNodes1.get(i) == orderedNodes2.get(i);
        }

        getSpiContext().triggerEvent(new GridTaskEvent(
            null, null, EVT_TASK_FINISHED, ses1.getId(), null, null, false, null));
        getSpiContext().triggerEvent(new GridTaskEvent(
            null, null, EVT_TASK_FAILED, ses2.getId(), null, null, false, null));
    }
}
