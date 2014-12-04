/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.session;

import org.apache.ignite.*;
import org.apache.ignite.compute.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.resources.*;
import org.gridgain.grid.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.testframework.*;
import org.gridgain.testframework.junits.common.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/**
 * Test of session siblings cancellation from future.
 */
@SuppressWarnings({"CatchGenericClass"})
@GridCommonTest(group = "Task Session")
public class GridSessionCancelSiblingsFromFutureSelfTest extends GridCommonAbstractTest {
    /** */
    private static final int WAIT_TIME = 20000;

    /** */
    public static final int SPLIT_COUNT = 5;

    /** */
    public static final int EXEC_COUNT = 5;

    /** */
    private static AtomicInteger[] interruptCnt;

    /** */
    private static CountDownLatch[] startSig;

    /** */
    private static CountDownLatch[] stopSig;

    /**
     *
     */
    public GridSessionCancelSiblingsFromFutureSelfTest() {
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
                SPLIT_COUNT * EXEC_COUNT,
                SPLIT_COUNT * EXEC_COUNT,
                0, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>()));

        c.setExecutorServiceShutdown(true);

        return c;
    }

    /**
     * @throws Exception if failed
     */
    public void testCancelSiblings() throws Exception {
        refreshInitialData();

        for (int i = 0; i < EXEC_COUNT; i++)
            checkTask(i);
    }

    /**
     * @throws Exception if failed
     */
    public void testMultiThreaded() throws Exception {
        refreshInitialData();

        final GridThreadSerialNumber sNum = new GridThreadSerialNumber();

        final AtomicBoolean failed = new AtomicBoolean(false);

        GridTestUtils.runMultiThreaded(new Runnable() {
            @Override public void run() {
                int num = sNum.get();

                try {
                    checkTask(num);
                }
                catch (Throwable e) {
                    error("Failed to execute task.", e);

                    failed.set(true);
                }
            }
        }, EXEC_COUNT, "grid-session-test");

        if (failed.get())
            fail();
    }

    /**
     * @param num Task argument.
     * @throws InterruptedException if failed
     * @throws GridException if failed
     */
    private void checkTask(int num) throws InterruptedException, GridException {
        Ignite ignite = G.grid(getTestGridName());

        ComputeTaskFuture<?> fut = executeAsync(ignite.compute(), GridTaskSessionTestTask.class, num);

        assert fut != null;

        try {
            // Wait until jobs begin execution.
            boolean await = startSig[num].await(WAIT_TIME, TimeUnit.MILLISECONDS);

            assert await : "Jobs did not start.";

            Collection<ComputeJobSibling> jobSiblings = fut.getTaskSession().getJobSiblings();

            // Cancel all jobs.
            for (ComputeJobSibling jobSibling : jobSiblings) {
                jobSibling.cancel();
            }

            Object res = fut.get();

            assert "interrupt-task-data".equals(res) : "Invalid task result: " + res;

            // Wait for all jobs to finish.
            await = stopSig[num].await(WAIT_TIME, TimeUnit.MILLISECONDS);

            assert await : "Jobs did not cancel.";

            int cnt = interruptCnt[num].get();

            assert cnt == SPLIT_COUNT : "Invalid interrupt count value: " + cnt;
        }
        finally {
            // We must wait for the jobs to be sure that they have completed
            // their execution since they use static variable (shared for the tests).
            fut.get();
        }
    }

    /** */
    private void refreshInitialData() {
        interruptCnt = new AtomicInteger[EXEC_COUNT];
        startSig = new CountDownLatch[EXEC_COUNT];
        stopSig = new CountDownLatch[EXEC_COUNT];

        for(int i=0 ; i < EXEC_COUNT; i++){
            interruptCnt[i] = new AtomicInteger(0);

            startSig[i] = new CountDownLatch(SPLIT_COUNT);

            stopSig[i] = new CountDownLatch(SPLIT_COUNT);
        }
    }

    /**
     *
     */
    private static class GridTaskSessionTestTask extends ComputeTaskSplitAdapter<Serializable, String> {
        /** */
        @IgniteLoggerResource
        private IgniteLogger log;

        /** */
        @IgniteTaskSessionResource
        private ComputeTaskSession taskSes;

        /** */
        private volatile int taskNum = -1;

        /** {@inheritDoc} */
        @Override protected Collection<? extends ComputeJob> split(int gridSize, Serializable arg) throws GridException {
            if (log.isInfoEnabled())
                log.info("Splitting jobs [task=" + this + ", gridSize=" + gridSize + ", arg=" + arg + ']');

            assert arg != null;

            taskNum = (Integer)arg;

            assert taskNum != -1;

            Collection<ComputeJob> jobs = new ArrayList<>(SPLIT_COUNT);

            for (int i = 1; i <= SPLIT_COUNT; i++) {
                jobs.add(new ComputeJobAdapter(i) {
                    private volatile Thread thread;

                    @Override public Serializable execute() {
                        assert taskSes != null;

                        thread = Thread.currentThread();

                        if (log.isInfoEnabled())
                            log.info("Computing job [job=" + this + ", arg=" + argument(0) + ']');

                        startSig[taskNum].countDown();

                        try {
                            if (!startSig[taskNum].await(WAIT_TIME, TimeUnit.MILLISECONDS))
                                fail();

                            Thread.sleep(WAIT_TIME);
                        }
                        catch (InterruptedException e) {
                            if (log.isInfoEnabled())
                                log.info("Job got interrupted [arg=" + argument(0) + ", e=" + e + ']');

                            return "interrupt-job-data";
                        }

                        if (log.isInfoEnabled())
                            log.info("Completing job: " + taskSes);

                        return argument(0);
                    }

                    @Override public void cancel() {
                        assert thread != null;

                        interruptCnt[taskNum].incrementAndGet();

                        stopSig[taskNum].countDown();
                    }
                });
            }

            return jobs;
        }

        /** {@inheritDoc} */
        @Override public ComputeJobResultPolicy result(ComputeJobResult res, List<ComputeJobResult> rcvd) throws GridException {
            if (log.isInfoEnabled())
                log.info("Received job result [job=" + this + ", result=" + res + ']');

            return rcvd.size() == SPLIT_COUNT ? ComputeJobResultPolicy.REDUCE : ComputeJobResultPolicy.WAIT;
        }

        /** {@inheritDoc} */
        @Override public String reduce(List<ComputeJobResult> results) throws GridException {
            if (log.isInfoEnabled())
                log.info("Aggregating job [job=" + this + ", results=" + results + ']');

            if (results.size() != SPLIT_COUNT)
                fail("Invalid results size.");

            return "interrupt-task-data";
        }
    }
}
