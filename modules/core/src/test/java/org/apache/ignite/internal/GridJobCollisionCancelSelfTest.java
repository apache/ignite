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

package org.apache.ignite.internal;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobContext;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskFuture;
import org.apache.ignite.compute.ComputeTaskSplitAdapter;
import org.apache.ignite.compute.ComputeTaskTimeoutException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.resources.JobContextResource;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.spi.IgniteSpiAdapter;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.IgniteSpiMultipleInstancesSupport;
import org.apache.ignite.spi.collision.CollisionContext;
import org.apache.ignite.spi.collision.CollisionExternalListener;
import org.apache.ignite.spi.collision.CollisionJobContext;
import org.apache.ignite.spi.collision.CollisionSpi;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;

/**
 * Job collision cancel test.
 */
@SuppressWarnings( {"PublicInnerClass"})
@GridCommonTest(group = "Kernal Self")
public class GridJobCollisionCancelSelfTest extends GridCommonAbstractTest {
    /** */
    private static final Object mux = new Object();

    /** */
    private static final int SPLIT_COUNT = 2;

    /** */
    private static final long maxJobExecTime = 10000;

    /** */
    private static int cancelCnt;

    /** */
    private static int execCnt;

    /** */
    private static int colResolutionCnt;

    /** */
    public GridJobCollisionCancelSelfTest() {
        super(true);
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings( {"AssignmentToCatchBlockParameter"})
    public void testCancel() throws Exception {
        Ignite ignite = G.ignite(getTestGridName());

        ignite.compute().localDeployTask(GridCancelTestTask.class, GridCancelTestTask.class.getClassLoader());

        ComputeTaskFuture<?> res0 =
            executeAsync(ignite.compute().withTimeout(maxJobExecTime * 2), GridCancelTestTask.class.getName(), null);

        try {
            Object res = res0.get();

            info("Cancel test result: " + res);

            synchronized (mux) {
                // Every execute must be called.
                assert execCnt <= SPLIT_COUNT : "Invalid execute count: " + execCnt;

                // Job returns 1 if was cancelled.
                assert (Integer)res <= SPLIT_COUNT  : "Invalid task result: " + res;

                // Should be exactly the same as Jobs number.
                assert cancelCnt <= SPLIT_COUNT : "Invalid cancel count: " + cancelCnt;

                // One per start and one per stop and some that come with heartbeats.
                assert colResolutionCnt > SPLIT_COUNT + 1:
                    "Invalid collision resolution count: " + colResolutionCnt;
            }
        }
        catch (ComputeTaskTimeoutException e) {
            error("Task execution got timed out.", e);
        }
        catch (Exception e) {
            assert e.getCause() != null;

            if (e.getCause() instanceof IgniteCheckedException)
                e = (Exception)e.getCause();

            if (e.getCause() instanceof IOException)
                e = (Exception)e.getCause();

            assert e.getCause() instanceof InterruptedException : "Invalid exception cause: " + e.getCause();
        }
    }

    /**
     * @return Configuration.
     * @throws Exception If failed.
     */
    @Override protected IgniteConfiguration getConfiguration() throws Exception {
        IgniteConfiguration cfg = super.getConfiguration();

        cfg.setCollisionSpi(new GridTestCollision());

        return cfg;
    }

    /**
     *
     */
    public static class GridCancelTestTask extends ComputeTaskSplitAdapter<Serializable, Object> {
        /** */
        @LoggerResource
        private IgniteLogger log;

        /** {@inheritDoc} */
        @Override public Collection<? extends ComputeJob> split(int gridSize, Serializable arg) {
            if (log.isInfoEnabled())
                log.info("Splitting task [task=" + this + ", gridSize=" + gridSize + ", arg=" + arg + ']');

            Collection<GridCancelTestJob> jobs = new ArrayList<>(SPLIT_COUNT);

            for (int i = 0; i < SPLIT_COUNT; i++)
                jobs.add(new GridCancelTestJob());

            return jobs;
        }


        /** {@inheritDoc} */
        @Override public Object reduce(List<ComputeJobResult> results) {
            if (log.isInfoEnabled())
                log.info("Aggregating job [job=" + this + ", results=" + results + ']');

            int res = 0;

            for (ComputeJobResult result : results) {
                assert result != null;

                if (result.getData() != null)
                    res += (Integer)result.getData();
            }

            return res;
        }
    }

    /**
     * Test job.
     */
    public static class GridCancelTestJob extends ComputeJobAdapter {
        /** */
        @LoggerResource
        private IgniteLogger log;

        /** */
        @JobContextResource
        private ComputeJobContext jobCtx;

        /** */
        @SuppressWarnings( {"FieldAccessedSynchronizedAndUnsynchronized"})
        private boolean isCancelled;

        /** */
        private final long thresholdTime;

        /** */
        public GridCancelTestJob() {
            thresholdTime = System.currentTimeMillis() + maxJobExecTime;
        }

        /** {@inheritDoc} */
        @Override public Serializable execute() {
            synchronized (mux) {
                execCnt++;
            }

            if (log.isInfoEnabled())
                log.info("Executing job: " + jobCtx.getJobId());

            long now = System.currentTimeMillis();

            while (!isCancelled && now < thresholdTime) {
                synchronized (mux) {
                    try {
                        mux.wait(thresholdTime - now);
                    }
                    catch (InterruptedException ignored) {
                        // No-op.
                    }
                }

                now = System.currentTimeMillis();
            }

            synchronized (mux) {
                return isCancelled ? 1 : 0;
            }
        }

        /** {@inheritDoc} */
        @Override public void cancel() {
            synchronized (mux) {
                isCancelled = true;

                cancelCnt++;

                mux.notifyAll();
            }

            log.warning("Job cancelled: " + jobCtx.getJobId());
        }
    }


    /**
     * Test collision SPI.
     */
    @IgniteSpiMultipleInstancesSupport(true)
    public static class GridTestCollision extends IgniteSpiAdapter implements CollisionSpi {
        /** */
        @LoggerResource
        private IgniteLogger log;

        /** {@inheritDoc} */
        @Override public void onCollision(CollisionContext ctx) {
            Collection<CollisionJobContext> activeJobs = ctx.activeJobs();
            Collection<CollisionJobContext> waitJobs = ctx.waitingJobs();

            synchronized (mux) {
                colResolutionCnt++;
            }

            for (CollisionJobContext job : waitJobs)
                job.activate();

            for (CollisionJobContext job : activeJobs)
                job.cancel();
        }

        /** {@inheritDoc} */
        @Override public void spiStart(String gridName) throws IgniteSpiException {
            // Start SPI start stopwatch.
            startStopwatch();

            // Ack start.
            if (log.isInfoEnabled())
                log.info(startInfo());
        }

        /** {@inheritDoc} */
        @Override public void spiStop() throws IgniteSpiException {
            // Ack stop.
            if (log.isInfoEnabled())
                log.info(stopInfo());
        }

        /** {@inheritDoc} */
        @Override public void setExternalCollisionListener(CollisionExternalListener lsnr) {
            // No-op.
        }
    }
}