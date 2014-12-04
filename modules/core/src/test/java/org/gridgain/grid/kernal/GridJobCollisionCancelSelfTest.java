/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */
package org.gridgain.grid.kernal;

import org.apache.ignite.*;
import org.apache.ignite.compute.*;
import org.apache.ignite.configuration.*;
import org.gridgain.grid.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.resources.*;
import org.gridgain.grid.spi.*;
import org.gridgain.grid.spi.collision.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.testframework.junits.common.*;

import java.io.*;
import java.util.*;

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
        Ignite ignite = G.grid(getTestGridName());

        ignite.compute().localDeployTask(GridCancelTestTask.class, GridCancelTestTask.class.getClassLoader());

        GridComputeTaskFuture<?> res0 =
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
        catch (GridComputeTaskTimeoutException e) {
            error("Task execution got timed out.", e);
        }
        catch (Exception e) {
            assert e.getCause() != null;

            if (e.getCause() instanceof GridException)
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
    public static class GridCancelTestTask extends GridComputeTaskSplitAdapter<Serializable, Object> {
        /** */
        @GridLoggerResource
        private GridLogger log;

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
        @Override public Object reduce(List<GridComputeJobResult> results) {
            if (log.isInfoEnabled())
                log.info("Aggregating job [job=" + this + ", results=" + results + ']');

            int res = 0;

            for (GridComputeJobResult result : results) {
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
    public static class GridCancelTestJob extends GridComputeJobAdapter {
        /** */
        @GridLoggerResource
        private GridLogger log;

        /** */
        @GridJobContextResource
        private GridComputeJobContext jobCtx;

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
    @GridSpiMultipleInstancesSupport(true)
    public static class GridTestCollision extends GridSpiAdapter implements GridCollisionSpi {
        /** */
        @GridLoggerResource private GridLogger log;

        /** {@inheritDoc} */
        @Override public void onCollision(GridCollisionContext ctx) {
            Collection<GridCollisionJobContext> activeJobs = ctx.activeJobs();
            Collection<GridCollisionJobContext> waitJobs = ctx.waitingJobs();

            synchronized (mux) {
                colResolutionCnt++;
            }

            for (GridCollisionJobContext job : waitJobs)
                job.activate();

            for (GridCollisionJobContext job : activeJobs)
                job.cancel();
        }

        /** {@inheritDoc} */
        @Override public void spiStart(String gridName) throws GridSpiException {
            // Start SPI start stopwatch.
            startStopwatch();

            // Ack start.
            if (log.isInfoEnabled())
                log.info(startInfo());
        }

        /** {@inheritDoc} */
        @Override public void spiStop() throws GridSpiException {
            // Ack stop.
            if (log.isInfoEnabled())
                log.info(stopInfo());
        }

        /** {@inheritDoc} */
        @Override public void setExternalCollisionListener(GridCollisionExternalListener lsnr) {
            // No-op.
        }
    }
}
