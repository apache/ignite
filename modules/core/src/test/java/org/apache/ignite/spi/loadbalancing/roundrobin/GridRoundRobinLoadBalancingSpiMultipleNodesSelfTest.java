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

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.ignite.GridTestJob;
import org.apache.ignite.GridTestTaskSession;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeTaskSession;
import org.apache.ignite.events.TaskEvent;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.testframework.GridSpiTestContext;
import org.apache.ignite.testframework.junits.spi.GridSpiAbstractTest;
import org.apache.ignite.testframework.junits.spi.GridSpiTest;

import static org.apache.ignite.events.EventType.EVT_TASK_FAILED;
import static org.apache.ignite.events.EventType.EVT_TASK_FINISHED;

/**
 * Tests round robin load balancing SPI.
 */
@GridSpiTest(spi = RoundRobinLoadBalancingSpi.class, group = "Load Balancing SPI")
public class GridRoundRobinLoadBalancingSpiMultipleNodesSelfTest
    extends GridSpiAbstractTest<RoundRobinLoadBalancingSpi> {
    /** {@inheritDoc} */
    @Override protected void spiConfigure(RoundRobinLoadBalancingSpi spi) throws Exception {
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

        getSpiContext().triggerEvent(new TaskEvent(
            null, null, EVT_TASK_FINISHED, ses1.getId(), null, null, false, null));
        getSpiContext().triggerEvent(new TaskEvent(
            null, null, EVT_TASK_FAILED, ses2.getId(), null, null, false, null));
    }
}