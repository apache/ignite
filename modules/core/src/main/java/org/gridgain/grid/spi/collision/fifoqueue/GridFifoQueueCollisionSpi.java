/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.collision.fifoqueue;

import org.apache.ignite.*;
import org.apache.ignite.resources.*;
import org.apache.ignite.spi.*;
import org.gridgain.grid.spi.collision.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.util.*;

/**
 * This class provides implementation for Collision SPI based on FIFO queue. Jobs are ordered
 * as they arrived and only {@link #getParallelJobsNumber()} number of jobs is allowed to
 * execute in parallel. Other jobs will be buffered in the passive queue.
 * <h1 class="header">Configuration</h1>
 * <h2 class="header">Mandatory</h2>
 * This SPI has no mandatory configuration parameters.
 * <h2 class="header">Optional</h2>
 * This SPI has following optional configuration parameters:
 * <ul>
 * <li>
 *      Number of jobs that can execute in parallel (see {@link #setParallelJobsNumber(int)}).
 *      This number should usually be set to the number of threads in the execution thread pool.
 * </li>
 * </ul>
 * <h2 class="header">Java Example</h2>
 * {@code GridFifoQueueCollisionSpi} can be configured as follows:
 * <pre name="code" class="java">
 * GridFifoQueueCollisionSpi colSpi = new GridFifoQueueCollisionSpi();
 *
 * // Execute all jobs sequentially by setting parallel job number to 1.
 * colSpi.setParallelJobsNumber(1);
 *
 * GridConfiguration cfg = new GridConfiguration();
 *
 * // Override default collision SPI.
 * cfg.setCollisionSpi(colSpi);
 *
 * // Starts grid.
 * G.start(cfg);
 * </pre>
 * <h2 class="header">Spring Example</h2>
 * {@code GridFifoQueueCollisionSpi} can be configured from Spring XML configuration file:
 * <pre name="code" class="xml">
 * &lt;bean id="grid.custom.cfg" class="org.gridgain.grid.GridConfiguration" singleton="true"&gt;
 *       ...
 *       &lt;property name="collisionSpi"&gt;
 *           &lt;bean class="org.gridgain.grid.spi.collision.fifoqueue.GridFifoQueueCollisionSpi"&gt;
 *               &lt;property name="parallelJobsNumber" value="1"/&gt;
 *           &lt;/bean&gt;
 *       &lt;/property&gt;
 *       ...
 * &lt;/bean&gt;
 * </pre>
 */
@IgniteSpiMultipleInstancesSupport(true)
public class GridFifoQueueCollisionSpi extends IgniteSpiAdapter implements CollisionSpi,
    GridFifoQueueCollisionSpiMBean {
    /**
     * Default number of parallel jobs allowed (value is {@code 95} which is
     * slightly less same as default value of threads in the execution thread pool
     * to allow some extra threads for system processing).
     */
    public static final int DFLT_PARALLEL_JOBS_NUM = 95;

    /**
     * Default waiting jobs number. If number of waiting jobs exceeds this number,
     * jobs will be rejected. Default value is {@link Integer#MAX_VALUE}.
     */
    public static final int DFLT_WAIT_JOBS_NUM = Integer.MAX_VALUE;

    /** Number of jobs that can be executed in parallel. */
    private volatile int parallelJobsNum = DFLT_PARALLEL_JOBS_NUM;

    /** Wait jobs number. */
    private volatile int waitJobsNum = DFLT_WAIT_JOBS_NUM;

    /** Grid logger. */
    @IgniteLoggerResource
    private IgniteLogger log;

    /** Number of jobs that were active last time. */
    private volatile int runningCnt;

    /** Number of jobs that were waiting for execution last time. */
    private volatile int waitingCnt;

    /** Number of jobs that are held. */
    private volatile int heldCnt;

    /** {@inheritDoc} */
    @Override public int getParallelJobsNumber() {
        return parallelJobsNum;
    }

    /** {@inheritDoc} */
    @IgniteSpiConfiguration(optional = true)
    @Override public void setParallelJobsNumber(int parallelJobsNum) {
        A.ensure(parallelJobsNum > 0, "parallelJobsNum > 0");

        this.parallelJobsNum = parallelJobsNum;
    }

    /** {@inheritDoc} */
    @Override public int getWaitingJobsNumber() {
        return waitJobsNum;
    }

    /** {@inheritDoc} */
    @IgniteSpiConfiguration(optional = true)
    @Override public void setWaitingJobsNumber(int waitJobsNum) {
        A.ensure(waitJobsNum >= 0, "waitingJobsNum >= 0");

        this.waitJobsNum = waitJobsNum;
    }

    /** {@inheritDoc} */
    @Override public int getCurrentWaitJobsNumber() {
        return waitingCnt;
    }

    /** {@inheritDoc} */
    @Override public int getCurrentActiveJobsNumber() {
        return runningCnt + heldCnt;
    }

    /** {@inheritDoc} */
    @Override public int getCurrentRunningJobsNumber() {
        return runningCnt;
    }

    /** {@inheritDoc} */
    @Override public int getCurrentHeldJobsNumber() {
        return heldCnt;
    }

    /** {@inheritDoc} */
    @Override public void spiStart(String gridName) throws IgniteSpiException {
        assertParameter(parallelJobsNum > 0, "parallelJobsNum > 0");
        assertParameter(waitJobsNum >= 0, "waitingJobsNum >= 0");

        // Start SPI start stopwatch.
        startStopwatch();

        // Ack parameters.
        if (log.isDebugEnabled())
            log.debug(configInfo("parallelJobsNum", parallelJobsNum));

        registerMBean(gridName, this, GridFifoQueueCollisionSpiMBean.class);

        // Ack start.
        if (log.isDebugEnabled())
            log.debug(startInfo());
    }

    /** {@inheritDoc} */
    @Override public void spiStop() throws IgniteSpiException {
        unregisterMBean();

        // Ack ok stop.
        if (log.isDebugEnabled())
            log.debug(stopInfo());
    }

    /** {@inheritDoc} */
    @Override public void setExternalCollisionListener(CollisionExternalListener lsnr) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void onCollision(CollisionContext ctx) {
        assert ctx != null;

        Collection<CollisionJobContext> activeJobs = ctx.activeJobs();
        Collection<CollisionJobContext> waitJobs = ctx.waitingJobs();

        // Save initial sizes to limit iteration.
        int activeSize = activeJobs.size();
        int waitSize = waitJobs.size();

        waitingCnt = waitSize;
        runningCnt = activeSize;
        heldCnt = ctx.heldJobs().size();

        int parallelJobsNum0 = parallelJobsNum;

        Iterator<CollisionJobContext> it = null;

        if (activeSize < parallelJobsNum0) {
            it = waitJobs.iterator();

            while (it.hasNext()) {
                CollisionJobContext waitCtx = it.next();

                waitCtx.activate();

                if (--waitSize == 0)
                    // No more waiting jobs to process (to limit iterations).
                    return;

                // Take actual size, since it might have been changed.
                if (activeJobs.size() >= parallelJobsNum0)
                    // Max active jobs threshold reached.
                    break;
            }
        }

        int waitJobsNum0 = waitJobsNum;

        // Take actual size, since it might have been changed.
        if (waitJobs.size() > waitJobsNum0) {
            if (it == null)
                it = waitJobs.iterator();

            while (it.hasNext()) {
                CollisionJobContext waitCtx = it.next();

                waitCtx.cancel();

                if (--waitSize == 0)
                    // No more waiting jobs to process (to limit iterations).
                    return;

                // Take actual size, since it might have been changed.
                if (waitJobs.size() <= waitJobsNum0)
                    // No need to reject more jobs.
                    return;
            }
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridFifoQueueCollisionSpi.class, this);
    }
}
