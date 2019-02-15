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

package org.apache.ignite.spi.failover.jobstealing;

import java.util.Collections;
import java.util.UUID;
import org.apache.ignite.GridTestJobResult;
import org.apache.ignite.GridTestTaskSession;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteNodeAttributes;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.collision.jobstealing.JobStealingCollisionSpi;
import org.apache.ignite.spi.failover.GridFailoverTestContext;
import org.apache.ignite.testframework.GridSpiTestContext;
import org.apache.ignite.testframework.GridTestNode;
import org.apache.ignite.testframework.junits.spi.GridSpiAbstractTest;
import org.apache.ignite.testframework.junits.spi.GridSpiTest;
import org.junit.Test;

/**
 * Job stealing failover SPI test for one node.
 */
@GridSpiTest(spi = JobStealingFailoverSpi.class, group = "Failover SPI")
public class GridJobStealingFailoverSpiOneNodeSelfTest extends GridSpiAbstractTest<JobStealingFailoverSpi> {
    /** {@inheritDoc} */
    @Override protected GridSpiTestContext initSpiContext() throws Exception {
        GridSpiTestContext ctx = super.initSpiContext();

        ctx.setLocalNode(addSpiDependency(new GridTestNode(UUID.randomUUID())));

        ctx.addNode(addSpiDependency(new GridTestNode(UUID.randomUUID())));

        return ctx;
    }

    /**
     * Adds Collision SPI attribute.
     *
     * @param node Node to add attribute to.
     * @return Passed in node.
     * @throws Exception If failed.
     */
    private ClusterNode addSpiDependency(GridTestNode node) throws Exception {
        node.addAttribute(
            U.spiAttribute(getSpi(), IgniteNodeAttributes.ATTR_SPI_CLASS),
            JobStealingCollisionSpi.class.getName());

        node.addAttribute(
            U.spiAttribute(getSpi(), IgniteNodeAttributes.ATTR_SPI_CLASS),
            JobStealingCollisionSpi.class.getName());

        return node;
    }

    /**
     * @throws Exception If test failed.
     */
    @Test
    public void testFailover() throws Exception {
        ClusterNode rmt = getSpiContext().remoteNodes().iterator().next();

        GridTestJobResult failed = new GridTestJobResult(rmt);

        failed.getJobContext().setAttribute(JobStealingCollisionSpi.THIEF_NODE_ATTR,
            getSpiContext().localNode().id());

        ClusterNode other = getSpi().failover(new GridFailoverTestContext(new GridTestTaskSession(), failed),
            Collections.singletonList(getSpiContext().remoteNodes().iterator().next()));

        assert other == rmt : "Invalid failed-over node: " + other;
    }

    /**
     * @throws Exception If test failed.
     */
    @Test
    public void testNoFailover() throws Exception {
        ClusterNode rmt = getSpiContext().remoteNodes().iterator().next();

        GridTestJobResult failed = new GridTestJobResult(rmt);

        ClusterNode other = getSpi().failover(new GridFailoverTestContext(new GridTestTaskSession(), failed),
            Collections.singletonList(getSpiContext().remoteNodes().iterator().next()));

        assert other == null;
    }
}
