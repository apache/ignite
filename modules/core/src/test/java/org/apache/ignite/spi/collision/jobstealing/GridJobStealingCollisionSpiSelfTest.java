/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.spi.collision.jobstealing;

import org.apache.ignite.cluster.*;
import org.apache.ignite.lang.*;
import org.gridgain.grid.*;
import org.apache.ignite.spi.collision.*;
import org.apache.ignite.spi.discovery.*;
import org.gridgain.grid.spi.failover.jobstealing.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.testframework.*;
import org.gridgain.testframework.junits.spi.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;

import static org.gridgain.grid.kernal.GridNodeAttributes.*;
import static org.apache.ignite.spi.collision.jobstealing.JobStealingCollisionSpi.*;

/**
 * Job stealing SPI test.
 */
@GridSpiTest(spi = JobStealingCollisionSpi.class, group = "Collision SPI")
public class GridJobStealingCollisionSpiSelfTest extends GridSpiAbstractTest<JobStealingCollisionSpi> {
    /** */
    public GridJobStealingCollisionSpiSelfTest() {
        super(true /*start spi*/);
    }

    /**
     * @return Wait jobs threshold.
     */
    @GridSpiTestConfig
    public int getWaitJobsThreshold() {
        return 0;
    }

    /**
     * @return Active jobs threshold.
     */
    @GridSpiTestConfig
    public int getActiveJobsThreshold() {
        return 1;
    }

    /**
     * @return Maximum stealing attempts.
     */
    @GridSpiTestConfig
    public int getMaximumStealingAttempts() {
        return 1;
    }

    /** {@inheritDoc} */
    @Override protected GridSpiTestContext initSpiContext() throws Exception {
        GridSpiTestContext ctx = super.initSpiContext();

        GridTestNode locNode = new GridTestNode(UUID.randomUUID());

        addSpiDependency(locNode);

        ctx.setLocalNode(locNode);

        GridTestNode rmtNode = new GridTestNode(UUID.randomUUID());

        addSpiDependency(rmtNode);

        rmtNode.setAttribute(U.spiAttribute(getSpi(), WAIT_JOBS_THRESHOLD_NODE_ATTR), getWaitJobsThreshold());

        DiscoveryNodeMetricsAdapter metrics = new DiscoveryNodeMetricsAdapter();

        metrics.setCurrentWaitingJobs(2);

        rmtNode.setMetrics(metrics);

        ctx.addNode(rmtNode);

        return ctx;
    }

    /**
     * Adds Failover SPI attribute.
     *
     * @param node Node to add attribute to.
     * @throws Exception If failed.
     */
    private void addSpiDependency(GridTestNode node) throws Exception {
        node.addAttribute(U.spiAttribute(getSpi(), ATTR_SPI_CLASS), GridJobStealingFailoverSpi.class.getName());
    }

    /**
     * @param ctx Collision job context.
     */
    private void checkActivated(GridTestCollisionJobContext ctx) {
        assert ctx.isActivated();
        assert !ctx.isCanceled();
        assert ctx.getJobContext().getAttribute(THIEF_NODE_ATTR) == null;
    }

    /**
     * @param ctx Collision job context.
     * @param rmtNode Remote node.
     */
    private void checkRejected(GridTestCollisionJobContext ctx, ClusterNode rmtNode) {
        assert ctx.isCanceled();
        assert !ctx.isActivated();
        assert ctx.getJobContext().getAttribute(THIEF_NODE_ATTR).equals(rmtNode.id());
    }

    /**
     * @param ctx Collision job context.
     */
    private void checkNoAction(GridTestCollisionJobContext ctx) {
        assert !ctx.isActivated();
        assert !ctx.isCanceled();
        assert ctx.getJobContext().getAttribute(THIEF_NODE_ATTR) == null;
    }

    /**
     * @throws Exception If test failed.
     */
    public void testTwoPassiveJobs() throws Exception {
        final List<CollisionJobContext> waitCtxs = new ArrayList<>(2);
        final List<CollisionJobContext> activeCtxs = new ArrayList<>(1);

        CI1<GridTestCollisionJobContext> lsnr = new CI1<GridTestCollisionJobContext>() {
            @Override public void apply(GridTestCollisionJobContext c) {
                if (waitCtxs.remove(c))
                    activeCtxs.add(c);
            }
        };

        Collections.addAll(waitCtxs,
            new GridTestCollisionJobContext(createTaskSession(), 1, lsnr),
            new GridTestCollisionJobContext(createTaskSession(), 2, lsnr));

        ClusterNode rmtNode = F.first(getSpiContext().remoteNodes());

        getSpiContext().triggerMessage(rmtNode, new JobStealingRequest(1));

        getSpi().onCollision(new GridCollisionTestContext(activeCtxs, waitCtxs));

        // Activated.
        checkActivated((GridTestCollisionJobContext)activeCtxs.get(0));

        // Rejected.
        checkRejected((GridTestCollisionJobContext)waitCtxs.get(0), rmtNode);

        // Make sure that no message was sent.
        Serializable msg = getSpiContext().removeSentMessage(rmtNode);

        assert msg == null;
    }

    /**
     * @throws Exception If test failed.
     */
    public void testOnePassiveOneActiveJobs() throws Exception {
        List<CollisionJobContext> waitCtxs = new ArrayList<>(1);

        // Add passive.
        Collections.addAll(waitCtxs, new GridTestCollisionJobContext(createTaskSession(), IgniteUuid.randomUuid()));

        List<CollisionJobContext> activeCtxs = new ArrayList<>(1);

        // Add active.
        Collections.addAll(activeCtxs, new GridTestCollisionJobContext(createTaskSession(),
            IgniteUuid.randomUuid()));

        ClusterNode rmtNode = F.first(getSpiContext().remoteNodes());

        getSpiContext().triggerMessage(rmtNode, new JobStealingRequest(1));

        getSpi().onCollision(new GridCollisionTestContext(activeCtxs, waitCtxs));

        // Active job.
        checkNoAction((GridTestCollisionJobContext)activeCtxs.get(0));

        // Rejected.
        checkRejected((GridTestCollisionJobContext)waitCtxs.get(0), rmtNode);

        // Make sure that no message was sent.
        Serializable msg = getSpiContext().removeSentMessage(rmtNode);

        assert msg == null;
    }

    /**
     * @throws Exception If test failed.
     */
    public void testMultiplePassiveOneActive() throws Exception {
        List<CollisionJobContext> waitCtxs = new ArrayList<>(2);

        Collections.addAll(waitCtxs,
            new GridTestCollisionJobContext(createTaskSession(), IgniteUuid.randomUuid()),
            new GridTestCollisionJobContext(createTaskSession(), IgniteUuid.randomUuid()),
            new GridTestCollisionJobContext(createTaskSession(), IgniteUuid.randomUuid()));

        Collection<CollisionJobContext> activeCtxs = new ArrayList<>(1);

        // Add active.
        Collections.addAll(activeCtxs, new GridTestCollisionJobContext(createTaskSession(),
            IgniteUuid.randomUuid()));

        ClusterNode rmtNode = F.first(getSpiContext().remoteNodes());

        // Emulate message to steal 2 jobs.
        getSpiContext().triggerMessage(rmtNode, new JobStealingRequest(2));

        getSpi().onCollision(new GridCollisionTestContext(activeCtxs, waitCtxs));

        // Rejected.
        checkRejected((GridTestCollisionJobContext)waitCtxs.get(0), rmtNode);
        checkRejected((GridTestCollisionJobContext)waitCtxs.get(1), rmtNode);

        // Check no action.
        checkNoAction((GridTestCollisionJobContext)waitCtxs.get(2));

        // Make sure that no message was sent.
        Serializable msg = getSpiContext().removeSentMessage(rmtNode);

        assert msg == null;
    }

    /**
     * @throws Exception If test failed.
     */
    public void testMultiplePassiveZeroActive() throws Exception {
        final List<CollisionJobContext> waitCtxs = new ArrayList<>(2);
        final List<CollisionJobContext> activeCtxs = new ArrayList<>(2);

        CI1<GridTestCollisionJobContext> lsnr = new CI1<GridTestCollisionJobContext>() {
            @Override public void apply(GridTestCollisionJobContext c) {
                if (waitCtxs.remove(c))
                    activeCtxs.add(c);
            }
        };

        Collections.addAll(waitCtxs,
            new GridTestCollisionJobContext(createTaskSession(), 1, lsnr),
            new GridTestCollisionJobContext(createTaskSession(), 2, lsnr),
            new GridTestCollisionJobContext(createTaskSession(), 3, lsnr));

        ClusterNode rmtNode = F.first(getSpiContext().remoteNodes());

        // Emulate message to steal 2 jobs.
        getSpiContext().triggerMessage(rmtNode, new JobStealingRequest(2));

        getSpi().onCollision(new GridCollisionTestContext(activeCtxs, waitCtxs));

        // Make sure that one job got activated.
        checkActivated((GridTestCollisionJobContext)activeCtxs.get(0));

        // Rejected.
        checkRejected((GridTestCollisionJobContext)waitCtxs.get(0), rmtNode);
        checkRejected((GridTestCollisionJobContext)waitCtxs.get(1), rmtNode);

        // Make sure that no message was sent.
        Serializable msg = getSpiContext().removeSentMessage(rmtNode);

        assert msg == null;
    }

    /**
     * @return Session.
     */
    public GridTestTaskSession createTaskSession() {
        return new GridTestTaskSession() {
            @Nullable @Override public Collection<UUID> getTopology() {
                try {
                    return F.nodeIds(getSpiContext().nodes());
                }
                catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        };
    }

    /**
     * @throws Exception If test failed.
     */
    public void testOnePassiveZeroActive() throws Exception {
        List<CollisionJobContext> waitCtxs = new ArrayList<>(1);

        // Add passive.
        Collections.addAll(waitCtxs, new GridTestCollisionJobContext(createTaskSession(), IgniteUuid.randomUuid()));

        Collection<CollisionJobContext> activeCtxs = Collections.emptyList();

        ClusterNode rmtNode = F.first(getSpiContext().remoteNodes());

        getSpiContext().triggerMessage(rmtNode, new JobStealingRequest(1));

        getSpi().onCollision(new GridCollisionTestContext(activeCtxs, waitCtxs));

        // Rejected.
        checkActivated((GridTestCollisionJobContext)waitCtxs.get(0));

        // Make sure that no message was sent.
        Serializable msg = getSpiContext().removeSentMessage(rmtNode);

        assert msg == null;
    }

    /**
     * @throws Exception If test failed.
     */
    public void testZeroPassiveOneActive() throws Exception {
        Collection<CollisionJobContext> empty = Collections.emptyList();

        List<CollisionJobContext> activeCtxs = new ArrayList<>(1);

        // Add active.
        Collections.addAll(activeCtxs, new GridTestCollisionJobContext(createTaskSession(),
            IgniteUuid.randomUuid()));

        ClusterNode rmtNode = F.first(getSpiContext().remoteNodes());

        getSpiContext().triggerMessage(rmtNode, new JobStealingRequest(1));

        getSpi().onCollision(new GridCollisionTestContext(activeCtxs, empty));

        // Active job.
        checkNoAction((GridTestCollisionJobContext)activeCtxs.get(0));

        // Make sure that no message was sent.
        Serializable msg = getSpiContext().removeSentMessage(rmtNode);

        assert msg == null;
    }

    /**
     * @throws Exception If test failed.
     */
    public void testZeroPassiveZeroActive() throws Exception {
        Collection<CollisionJobContext> empty = Collections.emptyList();

        getSpi().onCollision(new GridCollisionTestContext(empty, empty));

        ClusterNode rmtNode = F.first(getSpiContext().remoteNodes());

        JobStealingRequest sentMsg = (JobStealingRequest)getSpiContext().getSentMessage(rmtNode);

        assert sentMsg != null;

        assert sentMsg.delta() == 1 : "Invalid sent message: " + sentMsg;

        Serializable msg = getSpiContext().removeSentMessage(rmtNode);

        assert msg != null;
    }

    /**
     * @throws Exception If test failed.
     */
    public void testMaxHopsExceeded() throws Exception {
        Collection<CollisionJobContext> waitCtxs = new ArrayList<>(2);

        GridTestCollisionJobContext excluded = new GridTestCollisionJobContext(createTaskSession(),
            IgniteUuid.randomUuid());

        GridTestCollisionJobContext ctx1 = new GridTestCollisionJobContext(createTaskSession(),
            IgniteUuid.randomUuid());
        GridTestCollisionJobContext ctx2 = new GridTestCollisionJobContext(createTaskSession(),
            IgniteUuid.randomUuid());

        Collections.addAll(waitCtxs, ctx1, excluded, ctx2);

        Collection<CollisionJobContext> activeCtxs = new ArrayList<>(1);

        // Add active.
        Collections.addAll(activeCtxs, new GridTestCollisionJobContext(createTaskSession(),
            IgniteUuid.randomUuid()));

        ClusterNode rmtNode = F.first(getSpiContext().remoteNodes());

        // Exceed hops.
        excluded.getJobContext().setAttribute(STEALING_ATTEMPT_COUNT_ATTR, 1);

        // Emulate message to steal 2 jobs.
        getSpiContext().triggerMessage(rmtNode, new JobStealingRequest(2));

        getSpi().onCollision(new GridCollisionTestContext(activeCtxs, waitCtxs));

        // Make sure that none happened to 1st job.
        checkNoAction(excluded);

        // Rejected.
        checkRejected(ctx1, rmtNode);
        checkRejected(ctx2, rmtNode);

        // Make sure that no message was sent.
        Serializable msg = getSpiContext().removeSentMessage(rmtNode);

        assert msg == null;
    }
}
