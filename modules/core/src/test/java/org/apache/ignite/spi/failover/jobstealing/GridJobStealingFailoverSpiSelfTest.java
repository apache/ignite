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

import java.util.ArrayList;
import java.util.Collection;
import java.util.UUID;
import org.apache.ignite.GridTestJobResult;
import org.apache.ignite.GridTestTaskSession;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJobContext;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.collision.jobstealing.JobStealingCollisionSpi;
import org.apache.ignite.spi.failover.GridFailoverTestContext;
import org.apache.ignite.testframework.GridSpiTestContext;
import org.apache.ignite.testframework.GridTestNode;
import org.apache.ignite.testframework.junits.spi.GridSpiAbstractTest;
import org.apache.ignite.testframework.junits.spi.GridSpiTest;
import org.junit.Test;

import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_SPI_CLASS;
import static org.apache.ignite.spi.collision.jobstealing.JobStealingCollisionSpi.THIEF_NODE_ATTR;
import static org.apache.ignite.spi.failover.jobstealing.JobStealingFailoverSpi.FAILED_NODE_LIST_ATTR;
import static org.apache.ignite.spi.failover.jobstealing.JobStealingFailoverSpi.FAILOVER_ATTEMPT_COUNT_ATTR;

/**
 * Self test for {@link JobStealingFailoverSpi} SPI.
 */
@GridSpiTest(spi = JobStealingFailoverSpi.class, group = "Failover SPI")
public class GridJobStealingFailoverSpiSelfTest extends GridSpiAbstractTest<JobStealingFailoverSpi> {
    /** {@inheritDoc} */
    @Override protected GridSpiTestContext initSpiContext() throws Exception {
        GridSpiTestContext ctx = super.initSpiContext();

        GridTestNode loc = new GridTestNode(UUID.randomUUID());

        addSpiDependency(loc);

        ctx.setLocalNode(loc);

        GridTestNode rmt = new GridTestNode(UUID.randomUUID());

        ctx.addNode(rmt);

        addSpiDependency(rmt);

        return ctx;
    }

    /**
     * Adds Collision SPI attribute.
     *
     * @param node Node to add attribute to.
     * @throws Exception If failed.
     */
    private void addSpiDependency(GridTestNode node) throws Exception {
        node.addAttribute(ATTR_SPI_CLASS, JobStealingCollisionSpi.class.getName());

        node.setAttribute(U.spiAttribute(getSpi(), ATTR_SPI_CLASS), getSpi().getClass().getName());
    }

    /**
     * @throws Exception If test failed.
     */
    @Test
    public void testFailover() throws Exception {
        ClusterNode rmt = getSpiContext().remoteNodes().iterator().next();

        GridTestJobResult failed = new GridTestJobResult(rmt);

        failed.getJobContext().setAttribute(THIEF_NODE_ATTR,
            getSpiContext().localNode().id());

        ClusterNode other = getSpi().failover(new GridFailoverTestContext(new GridTestTaskSession(), failed),
            new ArrayList<>(getSpiContext().nodes()));

        assert other == getSpiContext().localNode();

        // This is not a failover but stealing.
        checkAttributes(failed.getJobContext(), null, 0);
    }

    /**
     * @throws Exception If test failed.
     */
    @Test
    public void testMaxHopsExceeded() throws Exception {
        ClusterNode rmt = getSpiContext().remoteNodes().iterator().next();

        GridTestJobResult failed = new GridTestJobResult(rmt);

        failed.getJobContext().setAttribute(THIEF_NODE_ATTR,
            getSpiContext().localNode().id());
        failed.getJobContext().setAttribute(FAILOVER_ATTEMPT_COUNT_ATTR,
            getSpi().getMaximumFailoverAttempts());

        ClusterNode other = getSpi().failover(new GridFailoverTestContext(new GridTestTaskSession(), failed),
            new ArrayList<>(getSpiContext().nodes()));

        assert other == null;
    }

    /**
     * @throws Exception If test failed.
     */
    @Test
    public void testMaxHopsExceededThiefNotSet() throws Exception {
        ClusterNode rmt = getSpiContext().remoteNodes().iterator().next();

        GridTestJobResult failed = new GridTestJobResult(rmt);

        failed.getJobContext().setAttribute(FAILOVER_ATTEMPT_COUNT_ATTR,
            getSpi().getMaximumFailoverAttempts());

        ClusterNode other = getSpi().failover(new GridFailoverTestContext(new GridTestTaskSession(), failed),
            new ArrayList<>(getSpiContext().nodes()));

        assert other == null;
    }

    /**
     * @throws Exception If test failed.
     */
    @Test
    public void testNonZeroFailoverCount() throws Exception {
        ClusterNode rmt = getSpiContext().remoteNodes().iterator().next();

        GridTestJobResult failed = new GridTestJobResult(rmt);

        failed.getJobContext().setAttribute(FAILOVER_ATTEMPT_COUNT_ATTR,
            getSpi().getMaximumFailoverAttempts() - 1);

        ClusterNode other = getSpi().failover(new GridFailoverTestContext(new GridTestTaskSession(), failed),
            new ArrayList<>(getSpiContext().nodes()));

        assert other != null;
        assert other != rmt;

        assert other == getSpiContext().localNode();

        checkAttributes(failed.getJobContext(), rmt, getSpi().getMaximumFailoverAttempts());
    }

    /**
     * @throws Exception If test failed.
     */
    @Test
    public void testThiefNotInTopology() throws Exception {
        ClusterNode rmt = new GridTestNode(UUID.randomUUID());

        GridTestJobResult failed = new GridTestJobResult(rmt);

        failed.getJobContext().setAttribute(THIEF_NODE_ATTR, rmt.id());

        ClusterNode other = getSpi().failover(new GridFailoverTestContext(new GridTestTaskSession(), failed),
            new ArrayList<>(getSpiContext().nodes()));

        assert other != null;
        assert other != rmt;

        assert getSpiContext().nodes().contains(other);

        checkAttributes(failed.getJobContext(), rmt, 1);
    }

    /**
     * @throws Exception If test failed.
     */
    @Test
    public void testThiefEqualsVictim() throws Exception {
        ClusterNode rmt = getSpiContext().remoteNodes().iterator().next();

        GridTestJobResult failed = new GridTestJobResult(rmt);

        failed.getJobContext().setAttribute(THIEF_NODE_ATTR, rmt.id());

        ClusterNode other = getSpi().failover(new GridFailoverTestContext(new GridTestTaskSession(), failed),
            new ArrayList<>(getSpiContext().nodes()));

        assert other != null;
        assert other != rmt;

        assert other.equals(getSpiContext().localNode());

        checkAttributes(failed.getJobContext(), rmt, 1);
    }

    /**
     * @throws Exception If test failed.
     */
    @Test
    public void testThiefIdNotSet() throws Exception {
        ClusterNode rmt = getSpiContext().remoteNodes().iterator().next();

        GridTestJobResult failed = new GridTestJobResult(rmt);

        ClusterNode other = getSpi().failover(new GridFailoverTestContext(new GridTestTaskSession(), failed),
            new ArrayList<>(getSpiContext().nodes()));

        assert other != null;
        assert other != rmt;

        assert other.equals(getSpiContext().localNode());

        checkAttributes(failed.getJobContext(), rmt, 1);
    }

    /**
     * @param ctx Failed job context.
     * @param failed Failed node.
     * @param failCnt Failover count.
     */
    private void checkAttributes(ComputeJobContext ctx, ClusterNode failed, int failCnt) {
        assert (Integer)ctx.getAttribute(FAILOVER_ATTEMPT_COUNT_ATTR) == failCnt;

        if (failed != null) {
            Collection<UUID> failedSet = (Collection<UUID>)ctx.getAttribute(FAILED_NODE_LIST_ATTR);

            assert failedSet.contains(failed.id());
        }
    }
}
