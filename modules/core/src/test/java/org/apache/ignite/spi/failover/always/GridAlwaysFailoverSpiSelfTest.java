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

package org.apache.ignite.spi.failover.always;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import org.apache.ignite.GridTestJobResult;
import org.apache.ignite.GridTestTaskSession;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.spi.failover.GridFailoverTestContext;
import org.apache.ignite.testframework.GridTestNode;
import org.apache.ignite.testframework.junits.spi.GridSpiAbstractTest;
import org.apache.ignite.testframework.junits.spi.GridSpiTest;

import static org.apache.ignite.spi.failover.always.AlwaysFailoverSpi.FAILED_NODE_LIST_ATTR;

/**
 * Always-failover SPI test.
 */
@GridSpiTest(spi = AlwaysFailoverSpi.class, group = "Failover SPI")
public class GridAlwaysFailoverSpiSelfTest extends GridSpiAbstractTest<AlwaysFailoverSpi> {
    /**
     * @throws Exception If failed.
     */
    public void testSingleNode() throws Exception {
        AlwaysFailoverSpi spi = getSpi();

        List<ClusterNode> nodes = new ArrayList<>();

        ClusterNode node = new GridTestNode(UUID.randomUUID());

        nodes.add(node);

        node = spi.failover(new GridFailoverTestContext(new GridTestTaskSession(), new GridTestJobResult(node)), nodes);

        assert node == null;
    }

    /**
     * @throws Exception If test failed.
     */
    @SuppressWarnings("unchecked")
    public void testTwoNodes() throws Exception {
        AlwaysFailoverSpi spi = getSpi();

        List<ClusterNode> nodes = new ArrayList<>();

        nodes.add(new GridTestNode(UUID.randomUUID()));
        nodes.add(new GridTestNode(UUID.randomUUID()));

        ComputeJobResult jobRes = new GridTestJobResult(nodes.get(0));

        ClusterNode node = spi.failover(new GridFailoverTestContext(new GridTestTaskSession(), jobRes), nodes);

        assert node != null;
        assert node.equals(nodes.get(1));

        checkFailedNodes(jobRes, 1);
    }

    /**
     * @throws Exception If failed.
     */
    public void testMaxAttempts() throws Exception {
        AlwaysFailoverSpi spi = getSpi();

        spi.setMaximumFailoverAttempts(1);

        List<ClusterNode> nodes = new ArrayList<>();

        nodes.add(new GridTestNode(UUID.randomUUID()));
        nodes.add(new GridTestNode(UUID.randomUUID()));

        ComputeJobResult jobRes = new GridTestJobResult(nodes.get(0));

        // First attempt.
        ClusterNode node = spi.failover(new GridFailoverTestContext(new GridTestTaskSession(), jobRes), nodes);

        assert node != null;
        assert node.equals(nodes.get(1));

        checkFailedNodes(jobRes, 1);

        // Second attempt (exceeds default max attempts of 1).
        node = spi.failover(new GridFailoverTestContext(new GridTestTaskSession(), jobRes), nodes);

        assert node == null;

        checkFailedNodes(jobRes, 1);
    }

    /**
     * @param res Job result.
     * @param cnt Failure count.
     */
    @SuppressWarnings("unchecked")
    private void checkFailedNodes(ComputeJobResult res, int cnt) {
        Collection<UUID> failedNodes =
            (Collection<UUID>)res.getJobContext().getAttribute(FAILED_NODE_LIST_ATTR);

        assert failedNodes != null;
        assert failedNodes.size() == cnt;
    }
}