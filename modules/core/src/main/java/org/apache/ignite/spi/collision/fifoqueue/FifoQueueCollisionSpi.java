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

package org.apache.ignite.spi.collision.fifoqueue;

import java.util.Collection;
import java.util.Iterator;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.spi.IgniteSpiAdapter;
import org.apache.ignite.spi.IgniteSpiConfiguration;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.IgniteSpiMultipleInstancesSupport;
import org.apache.ignite.spi.collision.CollisionContext;
import org.apache.ignite.spi.collision.CollisionExternalListener;
import org.apache.ignite.spi.collision.CollisionJobContext;
import org.apache.ignite.spi.collision.CollisionSpi;

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
 * {@code FifoQueueCollisionSpi} can be configured as follows:
 * <pre name="code" class="java">
 * FifoQueueCollisionSpi colSpi = new FifoQueueCollisionSpi();
 *
 * // Execute all jobs sequentially by setting parallel job number to 1.
 * colSpi.setParallelJobsNumber(1);
 *
 * IgniteConfiguration cfg = new IgniteConfiguration();
 *
 * // Override default collision SPI.
 * cfg.setCollisionSpi(colSpi);
 *
 * // Starts grid.
 * G.start(cfg);
 * </pre>
 * <h2 class="header">Spring Example</h2>
 * {@code FifoQueueCollisionSpi} can be configured from Spring XML configuration file:
 * <pre name="code" class="xml">
 * &lt;bean id="grid.custom.cfg" class="org.apache.ignite.configuration.IgniteConfiguration" singleton="true"&gt;
 *       ...
 *       &lt;property name="collisionSpi"&gt;
 *           &lt;bean class="org.apache.ignite.spi.collision.fifoqueue.FifoQueueCollisionSpi"&gt;
 *               &lt;property name="parallelJobsNumber" value="1"/&gt;
 *           &lt;/bean&gt;
 *       &lt;/property&gt;
 *       ...
 * &lt;/bean&gt;
 * </pre>
 */
@IgniteSpiMultipleInstancesSupport(true)
public class FifoQueueCollisionSpi extends IgniteSpiAdapter implements CollisionSpi,
    FifoQueueCollisionSpiMBean {
    /**
     * Default number of parallel jobs allowed (set to number of cores times 2).
     */
    public static final int DFLT_PARALLEL_JOBS_NUM = Runtime.getRuntime().availableProcessors() * 2;

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
    @LoggerResource
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

        registerMBean(gridName, this, FifoQueueCollisionSpiMBean.class);

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
        return S.toString(FifoQueueCollisionSpi.class, this);
    }
}