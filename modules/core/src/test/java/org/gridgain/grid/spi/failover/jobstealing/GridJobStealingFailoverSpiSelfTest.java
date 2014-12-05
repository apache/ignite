/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.failover.jobstealing;

import org.apache.ignite.cluster.*;
import org.apache.ignite.compute.*;
import org.gridgain.grid.*;
import org.apache.ignite.spi.collision.jobstealing.*;
import org.gridgain.grid.spi.failover.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.testframework.*;
import org.gridgain.testframework.junits.spi.*;

import java.util.*;

import static org.gridgain.grid.kernal.GridNodeAttributes.*;
import static org.apache.ignite.spi.collision.jobstealing.JobStealingCollisionSpi.*;
import static org.gridgain.grid.spi.failover.jobstealing.JobStealingFailoverSpi.*;

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
    @SuppressWarnings("unchecked")
    private void checkAttributes(ComputeJobContext ctx, ClusterNode failed, int failCnt) {
        assert (Integer)ctx.getAttribute(FAILOVER_ATTEMPT_COUNT_ATTR) == failCnt;

        if (failed != null) {
            Collection<UUID> failedSet = (Collection<UUID>)ctx.getAttribute(FAILED_NODE_LIST_ATTR);

            assert failedSet.contains(failed.id());
        }
    }
}
