/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.spi.failover.jobstealing;

import org.apache.ignite.cluster.*;
import org.gridgain.grid.*;
import org.gridgain.grid.kernal.*;
import org.apache.ignite.spi.collision.jobstealing.*;
import org.apache.ignite.spi.failover.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.testframework.*;
import org.gridgain.testframework.junits.spi.*;

import java.util.*;

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
            U.spiAttribute(getSpi(), GridNodeAttributes.ATTR_SPI_CLASS),
            JobStealingCollisionSpi.class.getName());

        node.addAttribute(
            U.spiAttribute(getSpi(), GridNodeAttributes.ATTR_SPI_CLASS),
            JobStealingCollisionSpi.class.getName());

        return node;
    }

    /**
     * @throws Exception If test failed.
     */
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
    public void testNoFailover() throws Exception {
        ClusterNode rmt = getSpiContext().remoteNodes().iterator().next();

        GridTestJobResult failed = new GridTestJobResult(rmt);

        ClusterNode other = getSpi().failover(new GridFailoverTestContext(new GridTestTaskSession(), failed),
            Collections.singletonList(getSpiContext().remoteNodes().iterator().next()));

        assert other == null;
    }
}
