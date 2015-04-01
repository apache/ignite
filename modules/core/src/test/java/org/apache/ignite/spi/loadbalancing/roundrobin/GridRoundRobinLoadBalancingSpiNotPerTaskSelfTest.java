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

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.compute.*;
import org.apache.ignite.events.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.testframework.*;
import org.apache.ignite.testframework.junits.spi.*;

import java.util.*;

import static org.apache.ignite.events.EventType.*;
import static org.apache.ignite.spi.loadbalancing.roundrobin.GridRoundRobinTestUtils.*;

/**
 * Tests round robin load balancing.
 */
@GridSpiTest(spi = RoundRobinLoadBalancingSpi.class, group = "Load Balancing SPI")
public class GridRoundRobinLoadBalancingSpiNotPerTaskSelfTest
    extends GridSpiAbstractTest<RoundRobinLoadBalancingSpi> {
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

        getSpiContext().triggerEvent(new TaskEvent(
            null, null, EVT_TASK_FINISHED, ses1.getId(), null, null, false, null));
        getSpiContext().triggerEvent(new TaskEvent(
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
    public void testNodeNotInTopology() throws Exception {
        ComputeTaskSession ses = new GridTestTaskSession();

        ClusterNode node = new GridTestNode(UUID.randomUUID());

        List<ClusterNode> notInTop = Arrays.asList(node);

        try {
            getSpi().getBalancedNode(ses, notInTop, new GridTestJob());
        }
        catch (IgniteException e) {
            assertTrue(e.getMessage().contains("Task topology does not have alive nodes"));
        }
    }
}
