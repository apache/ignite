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

package org.apache.ignite.cache.affinity.rendezvous;

import java.io.File;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.affinity.AbstractAffinityFunctionSelfTest;
import org.apache.ignite.cache.affinity.AffinityFunction;
import org.apache.ignite.cache.affinity.AffinityFunctionContext;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteNodeAttributes;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.affinity.GridAffinityFunctionContextImpl;
import org.apache.ignite.testframework.GridTestNode;
import org.apache.ignite.testframework.GridTestUtils;

/**
 * Tests for {@link RendezvousAffinityFunction}.
 */
public class RendezvousAffinityFunctionCalculateDistributionSelfTest extends AbstractAffinityFunctionSelfTest {
    /** MAC prefix. */
    private static final String MAC_PREF = "MAC";

    /** Ignite. */
    private static Ignite ignite;

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /**
     * @param nodesCnt Count of nodes to generate.
     * @return Nodes list.
     */
    private List<ClusterNode> createBaseNodes(int nodesCnt) {
        List<ClusterNode> nodes = new ArrayList<>(nodesCnt);

        for (int i = 0; i < nodesCnt; i++) {
            GridTestNode node = new GridTestNode(UUID.randomUUID());

            // two neighbours nodes
            node.setAttribute(IgniteNodeAttributes.ATTR_MACS, MAC_PREF + i / 2);

            nodes.add(node);
        }
        return nodes;
    }

    /**
     * @throws Exception If failed.
     */
    public void testDistributionCalculationEnabled() throws Exception {
        System.setProperty(IgniteSystemProperties.IGNITE_PART_DISTRIBUTION_WARN_THRESHOLD, String.valueOf(10));

        File file = new File(home() + "/work/log/ignite.log");

        new PrintWriter(file).close();

        ignite = startGrids(2);

        AffinityFunctionContext ctx =
            new GridAffinityFunctionContextImpl(new ArrayList<>(ignite.cluster().nodes()), null, null,
                new AffinityTopologyVersion(1), 1);

        affinityFunction().assignPartitions(ctx);

        assertTrue(FileUtils.readFileToString(file).contains("Partition map has been built (distribution is not even for caches) [cacheName=ignite-atomics-sys-cache, Primary nodeId="
            + grid(1).configuration().getNodeId() + ", totalPartitionsCount=1024 percentageOfTotalPartsCount=51%, parts=524]"));

        assertTrue(FileUtils.readFileToString(file).contains("Partition map has been built (distribution is not even for caches) [cacheName=ignite-atomics-sys-cache, Primary nodeId="
            + grid(0).configuration().getNodeId() + ", totalPartitionsCount=1024 percentageOfTotalPartsCount=49%, parts=500]"));
    }

    /**
     * @throws Exception If failed.
     */
    public void testDistributionCalculationDisabled() throws Exception {
        System.setProperty(IgniteSystemProperties.IGNITE_PART_DISTRIBUTION_WARN_THRESHOLD, String.valueOf(55));

        File file = new File(home() + "/work/log/ignite.log");

        new PrintWriter(file).close();

        ignite = startGrids(2);

        AffinityFunctionContext ctx =
            new GridAffinityFunctionContextImpl(new ArrayList<>(ignite.cluster().nodes()), null, null,
                new AffinityTopologyVersion(1), 1);

        affinityFunction().assignPartitions(ctx);

        assertTrue(FileUtils.readFileToString(file).contains("Partition map has been built (distribution is even)"));
    }

    /**
     * @return affinityFunction AffinityFunction
     */
    @Override protected AffinityFunction affinityFunction() {
        AffinityFunction aff = new RendezvousAffinityFunction(true, 1024);

        GridTestUtils.setFieldValue(aff, "log", log);

        return aff;
    }
}