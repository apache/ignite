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
import org.junit.Test;

import static org.apache.ignite.spi.failover.always.AlwaysFailoverSpi.FAILED_NODE_LIST_ATTR;

/**
 * Always-failover SPI test.
 */
@GridSpiTest(spi = AlwaysFailoverSpi.class, group = "Failover SPI")
public class GridAlwaysFailoverSpiSelfTest extends GridSpiAbstractTest<AlwaysFailoverSpi> {
    /**
     * @throws Exception If failed.
     */
    @Test
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
    @Test
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
    @Test
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
    private void checkFailedNodes(ComputeJobResult res, int cnt) {
        Collection<UUID> failedNodes =
            (Collection<UUID>)res.getJobContext().getAttribute(FAILED_NODE_LIST_ATTR);

        assert failedNodes != null;
        assert failedNodes.size() == cnt;
    }
}
