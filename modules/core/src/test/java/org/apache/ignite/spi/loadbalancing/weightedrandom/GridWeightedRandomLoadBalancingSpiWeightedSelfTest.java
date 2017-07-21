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

package org.apache.ignite.spi.loadbalancing.weightedrandom;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import org.apache.ignite.GridTestJob;
import org.apache.ignite.GridTestTaskSession;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.testframework.GridTestNode;
import org.apache.ignite.testframework.junits.spi.GridSpiAbstractTest;
import org.apache.ignite.testframework.junits.spi.GridSpiTest;
import org.apache.ignite.testframework.junits.spi.GridSpiTestConfig;

import static org.apache.ignite.spi.loadbalancing.weightedrandom.WeightedRandomLoadBalancingSpi.NODE_WEIGHT_ATTR_NAME;

/**
 * {@link WeightedRandomLoadBalancingSpi} self test.
 */
@GridSpiTest(spi = WeightedRandomLoadBalancingSpi.class, group = "Load Balancing SPI")
public class GridWeightedRandomLoadBalancingSpiWeightedSelfTest
    extends GridSpiAbstractTest<WeightedRandomLoadBalancingSpi> {
    /**
     * @return {@code True} if node weights should be considered.
     */
    @GridSpiTestConfig
    public boolean getUseWeights() {
        return true;
    }

    /**
     * @throws Exception If test failed.
     */
    public void testWeights() throws Exception {
        List<ClusterNode> nodes = new ArrayList<>();

        for (int i = 0; i < 10; i++) {
            GridTestNode node = new GridTestNode(UUID.randomUUID());

            node.addAttribute(U.spiAttribute(getSpi(), NODE_WEIGHT_ATTR_NAME), i + 1);

            nodes.add(node);
        }

        // Seal it.
        nodes = Collections.unmodifiableList(nodes);

        int[] cnts = new int[10];

        // Invoke load balancer a large number of times, so statistics won't lie.
        for (int i = 0; i < 100000; i++) {
            ClusterNode node = getSpi().getBalancedNode(new GridTestTaskSession(IgniteUuid.randomUuid()), nodes,
                new GridTestJob());

            int weight = (Integer)node.attribute(U.spiAttribute(getSpi(), NODE_WEIGHT_ATTR_NAME));

            // Increment number of times a node was picked.
            cnts[weight - 1]++;
        }

        for (int i = 0; i < cnts.length - 1; i++) {
            assert cnts[i] < cnts[i + 1] : "Invalid node counts for index [idx=" + i + ", cnts[i]=" + cnts[i] +
                ", cnts[i+1]=" + cnts[i + 1] + ']';
        }

        info("Node counts: " + Arrays.toString(cnts));
    }
}