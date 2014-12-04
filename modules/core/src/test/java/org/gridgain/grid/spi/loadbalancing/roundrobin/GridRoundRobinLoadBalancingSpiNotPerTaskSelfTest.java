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
import static org.gridgain.grid.spi.loadbalancing.roundrobin.GridRoundRobinTestUtils.*;

/**
 * Tests round robin load balancing.
 */
@GridSpiTest(spi = GridRoundRobinLoadBalancingSpi.class, group = "Load Balancing SPI")
public class GridRoundRobinLoadBalancingSpiNotPerTaskSelfTest
    extends GridSpiAbstractTest<GridRoundRobinLoadBalancingSpi> {
    /**
     * @return Per-task configuration parameter.
     */
    @GridSpiTestConfig
    public boolean getPerTask() {
        return false;
    }

    /** {@inheritDoc} */
    @Override protected GridSpiTestContext initSpiContext() throws Exception {
        GridSpiTestContext spiCtx = super.initSpiContext();

        spiCtx.createLocalNode();
        spiCtx.createRemoteNodes(10);

        return spiCtx;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        assert !getSpi().isPerTask() : "Invalid SPI configuration.";
    }

    /**
     * @throws Exception If test failed.
     */
    public void testMultipleNodes() throws Exception {
        List<ClusterNode> allNodes = (List<ClusterNode>)getSpiContext().nodes();

        ComputeTaskSession ses = new GridTestTaskSession();

        List<UUID> orderedNodes = new ArrayList<>(getSpi().getNodeIds(ses));

        assertEquals("Balancer doesn't use all available nodes", orderedNodes.size(), allNodes.size());

        checkCyclicBalancing(getSpi(), allNodes, orderedNodes, ses);
    }

    /**
     * @throws Exception If test failed.
     */
    public void testMultipleTaskSessions() throws Exception {
        ComputeTaskSession ses1 = new GridTestTaskSession(IgniteUuid.randomUuid());
        ComputeTaskSession ses2 = new GridTestTaskSession(IgniteUuid.randomUuid());

        List<ClusterNode> allNodes = (List<ClusterNode>)getSpiContext().nodes();

        List<UUID> orderedNodes = getSpi().getNodeIds(ses1);

        assertEquals("Balancer doesn't use all available nodes", orderedNodes.size(), allNodes.size());

        checkCyclicBalancing(getSpi(), allNodes, orderedNodes, ses1, ses2);

        getSpiContext().triggerEvent(new GridTaskEvent(
            null, null, EVT_TASK_FINISHED, ses1.getId(), null, null, false, null));
        getSpiContext().triggerEvent(new GridTaskEvent(
            null, null, EVT_TASK_FAILED, ses2.getId(), null, null, false, null));
    }

    /**
     * @throws Exception If test failed.
     */
    public void testBalancingOneNode() throws Exception {
        ComputeTaskSession ses = new GridTestTaskSession();

        List<ClusterNode> allNodes = (List<ClusterNode>)getSpiContext().nodes();

        List<ClusterNode> balancedNode = Arrays.asList(allNodes.get(0));

        ClusterNode firstNode = getSpi().getBalancedNode(ses, balancedNode, new GridTestJob());
        ClusterNode secondNode = getSpi().getBalancedNode(ses, balancedNode, new GridTestJob());

        assertEquals(firstNode, secondNode);
    }

    /** */
    public void testNodeNotInTopology() {
        ComputeTaskSession ses = new GridTestTaskSession();

        ClusterNode node = new GridTestNode(UUID.randomUUID());

        List<ClusterNode> notInTop = Arrays.asList(node);

        try {
            getSpi().getBalancedNode(ses, notInTop, new GridTestJob());
        }
        catch (GridException e) {
            assertTrue(e.getMessage().contains("Task topology does not have alive nodes"));
        }
    }
}
