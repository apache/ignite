/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal;

import org.apache.ignite.*;
import org.apache.ignite.configuration.*;
import org.gridgain.grid.*;
import org.gridgain.grid.compute.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.resources.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.testframework.junits.common.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * Cancel unused job test.
 */
@GridCommonTest(group = "Kernal Self")
public class GridCancelUnusedJobSelfTest extends GridCommonAbstractTest {
    /** */
    private static final int WAIT_TIME = 100000;

    /** */
    public static final int SPLIT_COUNT = 10;

    /** */
    private static volatile int cancelCnt;

    /** */
    private static volatile int processedCnt;

    /** */
    private static CountDownLatch startSignal = new CountDownLatch(SPLIT_COUNT);

    /** */
    private static CountDownLatch stopSignal = new CountDownLatch(SPLIT_COUNT);

    /** */
    public GridCancelUnusedJobSelfTest() {
        super(true);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(gridName);

        GridTcpDiscoverySpi discoSpi = new GridTcpDiscoverySpi();

        discoSpi.setIpFinder(new GridTcpDiscoveryVmIpFinder(true));

        c.setDiscoverySpi(discoSpi);

        c.setExecutorService(
            new ThreadPoolExecutor(
                SPLIT_COUNT,
                SPLIT_COUNT,
                0, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>()));

        c.setExecutorServiceShutdown(true);

        return c;
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    public void testCancel() throws Exception {
        Ignite ignite = G.grid(getTestGridName());

        ignite.compute().localDeployTask(GridCancelTestTask.class, U.detectClassLoader(GridCancelTestTask.class));

        GridComputeTaskFuture<?> fut = executeAsync(ignite.compute(), GridCancelTestTask.class.getName(), null);

        // Wait until jobs begin execution.
        boolean await = startSignal.await(WAIT_TIME, TimeUnit.MILLISECONDS);

        assert await : "Jobs did not start.";

        info("Test task result: " + fut);

        assert fut != null;

        // Only first job should successfully complete.
        Object res = fut.get();
        assert (Integer)res == 1;

        // Wait for all jobs to finish.
        await = stopSignal.await(WAIT_TIME, TimeUnit.MILLISECONDS);
        assert await : "Jobs did not stop.";

        // One is definitely processed. But there might be some more processed or cancelled or processed and cancelled.
        // Thus total number should be at least SPLIT_COUNT and at most (SPLIT_COUNT - 1) *2 +1
        assert (cancelCnt + processedCnt) >= SPLIT_COUNT && (cancelCnt + processedCnt) <= (SPLIT_COUNT - 1) * 2 +1 :
            "Invalid cancel count value: " + cancelCnt;
    }

    /**
     *
     */
    private static class GridCancelTestTask extends GridComputeTaskSplitAdapter<Object, Object> {
        /** */
        @GridLoggerResource private GridLogger log;

        /** {@inheritDoc} */
        @Override protected Collection<? extends GridComputeJob> split(int gridSize, Object arg) throws GridException {
            if (log.isInfoEnabled())
                log.info("Splitting job [job=" + this + ", gridSize=" + gridSize + ", arg=" + arg + ']');

            Collection<GridComputeJob> jobs = new ArrayList<>(SPLIT_COUNT);

            for (int i = 1; i <= SPLIT_COUNT; i++)
                jobs.add(new GridCancelTestJob(i));

            return jobs;
        }

        /** {@inheritDoc} */
        @Override public GridComputeJobResultPolicy result(GridComputeJobResult res, List<GridComputeJobResult> received) {
            return GridComputeJobResultPolicy.REDUCE;
        }

        /** {@inheritDoc} */
        @Override public Serializable reduce(List<GridComputeJobResult> results) throws GridException {
            if (log.isInfoEnabled())
                log.info("Reducing job [job=" + this + ", results=" + results + ']');

            if (results.size() > 1)
                fail();

            return results.get(0).getData();
        }
    }

    /**
     * Cancel test job.
     */
    private static class GridCancelTestJob extends GridComputeJobAdapter {
        /** */
        @GridLoggerResource
        private GridLogger log;

        /** */
        @GridTaskSessionResource
        private GridComputeTaskSession ses;

        /**
         * @param arg Argument.
         */
        private GridCancelTestJob(Integer arg) {
            super(arg);
        }

        /** {@inheritDoc} */
        @Override public Serializable execute() {
            int arg = this.<Integer>argument(0);

            try {
                if (log.isInfoEnabled())
                    log.info("Executing job [job=" + this + ", arg=" + arg + ']');

                startSignal.countDown();

                try {
                    if (!startSignal.await(WAIT_TIME, TimeUnit.MILLISECONDS))
                        fail();

                    if (arg == 1) {
                        if (log.isInfoEnabled())
                            log.info("Job one is proceeding.");
                    }
                    else
                        Thread.sleep(WAIT_TIME);
                }
                catch (InterruptedException e) {
                    if (log.isInfoEnabled())
                        log.info("Job got cancelled [arg=" + arg + ", ses=" + ses + ", e=" + e + ']');

                    return 0;
                }

                if (log.isInfoEnabled())
                    log.info("Completing job: " + ses);

                return argument(0);
            }
            finally {
                stopSignal.countDown();

                processedCnt++;
            }
        }

        /** {@inheritDoc} */
        @Override public void cancel() {
            cancelCnt++;
        }
    }
}
