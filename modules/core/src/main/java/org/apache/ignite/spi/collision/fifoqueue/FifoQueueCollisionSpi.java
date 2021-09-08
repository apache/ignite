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
import org.apache.ignite.spi.IgniteSpiMBeanAdapter;
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
public class FifoQueueCollisionSpi extends IgniteSpiAdapter implements CollisionSpi {
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

    /**
     * See {@link #setParallelJobsNumber(int)}
     *
     * @return Number of jobs that can be executed in parallel.
     */
    public int getParallelJobsNumber() {
        return parallelJobsNum;
    }

    /**
     * Sets number of jobs that can be executed in parallel.
     *
     * @param parallelJobsNum Parallel jobs number.
     * @return {@code this} for chaining.
     */
    @IgniteSpiConfiguration(optional = true)
    public FifoQueueCollisionSpi setParallelJobsNumber(int parallelJobsNum) {
        A.ensure(parallelJobsNum > 0, "parallelJobsNum > 0");

        this.parallelJobsNum = parallelJobsNum;

        return this;
    }

    /**
     * See {@link #setWaitingJobsNumber(int)}
     *
     * @return Maximum allowed number of waiting jobs.
     */
    public int getWaitingJobsNumber() {
        return waitJobsNum;
    }

    /**
     * Sets maximum number of jobs that are allowed to wait in waiting queue. If number
     * of waiting jobs ever exceeds this number, excessive jobs will be rejected.
     *
     * @param waitJobsNum Waiting jobs number.
     * @return {@code this} for chaining.
     */
    @IgniteSpiConfiguration(optional = true)
    public FifoQueueCollisionSpi setWaitingJobsNumber(int waitJobsNum) {
        A.ensure(waitJobsNum >= 0, "waitingJobsNum >= 0");

        this.waitJobsNum = waitJobsNum;

        return this;
    }

    /**
     * Gets current number of jobs that wait for the execution.
     *
     * @return Number of jobs that wait for execution.
     */
    public int getCurrentWaitJobsNumber() {
        return waitingCnt;
    }

    /**
     * Gets current number of jobs that are active, i.e. {@code 'running + held'} jobs.
     *
     * @return Number of active jobs.
     */
    public int getCurrentActiveJobsNumber() {
        return runningCnt + heldCnt;
    }

    /**
     * Gets number of currently running (not {@code 'held}) jobs.
     *
     * @return Number of currently running (not {@code 'held}) jobs.
     */
    public int getCurrentRunningJobsNumber() {
        return runningCnt;
    }

    /**
     * Gets number of currently {@code 'held'} jobs.
     *
     * @return Number of currently {@code 'held'} jobs.
     */
    public int getCurrentHeldJobsNumber() {
        return heldCnt;
    }

    /** {@inheritDoc} */
    @Override public void spiStart(String igniteInstanceName) throws IgniteSpiException {
        assertParameter(parallelJobsNum > 0, "parallelJobsNum > 0");
        assertParameter(waitJobsNum >= 0, "waitingJobsNum >= 0");

        // Start SPI start stopwatch.
        startStopwatch();

        // Ack parameters.
        if (log.isDebugEnabled())
            log.debug(configInfo("parallelJobsNum", parallelJobsNum));

        registerMBean(igniteInstanceName, new FifoQueueCollisionSpiMBeanImpl(this), FifoQueueCollisionSpiMBean.class);

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
    @Override public FifoQueueCollisionSpi setName(String name) {
        super.setName(name);

        return this;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(FifoQueueCollisionSpi.class, this);
    }

    /**
     * MBean implementation for FifoQueueCollisionSpi.
     */
    private class FifoQueueCollisionSpiMBeanImpl extends IgniteSpiMBeanAdapter implements FifoQueueCollisionSpiMBean {
        /** {@inheritDoc} */
        FifoQueueCollisionSpiMBeanImpl(IgniteSpiAdapter spiAdapter) {
            super(spiAdapter);
        }

        /** {@inheritDoc} */
        @Override public int getParallelJobsNumber() {
            return FifoQueueCollisionSpi.this.getParallelJobsNumber();
        }

        /** {@inheritDoc} */
        @Override public int getCurrentWaitJobsNumber() {
            return FifoQueueCollisionSpi.this.getCurrentWaitJobsNumber();
        }

        /** {@inheritDoc} */
        @Override public int getCurrentActiveJobsNumber() {
            return FifoQueueCollisionSpi.this.getCurrentActiveJobsNumber();
        }

        /** {@inheritDoc} */
        @Override public int getCurrentRunningJobsNumber() {
            return FifoQueueCollisionSpi.this.getCurrentRunningJobsNumber();
        }

        /** {@inheritDoc} */
        @Override public int getCurrentHeldJobsNumber() {
            return FifoQueueCollisionSpi.this.getCurrentHeldJobsNumber();
        }

        /** {@inheritDoc} */
        @Override public int getWaitingJobsNumber() {
            return FifoQueueCollisionSpi.this.getWaitingJobsNumber();
        }

        /** {@inheritDoc} */
        @Override public void setWaitingJobsNumber(int waitJobsNum) {
            FifoQueueCollisionSpi.this.setWaitingJobsNumber(waitJobsNum);
        }

        /** {@inheritDoc} */
        @Override public void setParallelJobsNumber(int parallelJobsNum) {
            FifoQueueCollisionSpi.this.setParallelJobsNumber(parallelJobsNum);
        }
    }
}
