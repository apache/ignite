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
import org.apache.ignite.lang.*;
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
 * Session cancellation tests.
 */
@SuppressWarnings({"CatchGenericClass"})
@GridCommonTest(group = "Task Session")
public class GridSessionCancelSiblingsFromTaskSelfTest extends GridCommonAbstractTest {
    /** */
    private static final int WAIT_TIME = 20000;

    /** */
    public static final int SPLIT_COUNT = 5;

    /** */
    public static final int EXEC_COUNT = 5;

    /** */
    private static AtomicInteger[] interruptCnt;

    /** */
    private static CountDownLatch[] startSignal;

    /** */
    private static CountDownLatch[] stopSignal;

    /** */
    public GridSessionCancelSiblingsFromTaskSelfTest() {
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
     * @throws Exception If failed.
     */
    public void testCancelSiblings() throws Exception {
        refreshInitialData();

        for (int i = 0; i < EXEC_COUNT; i++)
            checkTask(i);
    }

    /**
     * @throws Exception If failed.
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
     * @param num Task number.
     * @throws InterruptedException If interrupted.
     * @throws GridException If failed.
     */
    private void checkTask(int num) throws InterruptedException, GridException {
        Ignite ignite = G.grid(getTestGridName());

        ComputeTaskFuture<?> fut = executeAsync(ignite.compute(), GridTaskSessionTestTask.class, num);

        assert fut != null;

        try {
            // Wait until jobs begin execution.
            boolean await = startSignal[num].await(WAIT_TIME, TimeUnit.MILLISECONDS);

            assert await : "Jobs did not start.";

            Object res = fut.get();

            assert "interrupt-task-data".equals(res) : "Invalid task result: " + res;

            // Wait for all jobs to finish.
            await = stopSignal[num].await(WAIT_TIME, TimeUnit.MILLISECONDS);

            assert await :
                "Jobs did not cancel [interruptCount=" + Arrays.toString(interruptCnt) + ']';

            int cnt = interruptCnt[num].get();

            assert cnt == SPLIT_COUNT - 1 : "Invalid interrupt count value: " + cnt;
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
        startSignal = new CountDownLatch[EXEC_COUNT];
        stopSignal = new CountDownLatch[EXEC_COUNT];

        for(int i=0 ; i < EXEC_COUNT; i++){
            interruptCnt[i] = new AtomicInteger(0);

            startSignal[i] = new CountDownLatch(SPLIT_COUNT);

            stopSignal[i] = new CountDownLatch(SPLIT_COUNT - 1);
        }
    }

    /**
     *
     */
    @SuppressWarnings({"PublicInnerClass"})
    public static class GridTaskSessionTestTask extends ComputeTaskSplitAdapter<Serializable, String> {
        /** */
        @GridLoggerResource
        private GridLogger log;

        /** */
        @GridTaskSessionResource
        private ComputeTaskSession taskSes;

        /** */
        private volatile int taskNum = -1;

        /** {@inheritDoc} */
        @Override protected Collection<? extends ComputeJob> split(int gridSize, Serializable arg) throws GridException {
            if (log.isInfoEnabled())
                log.info("Splitting job [job=" + this + ", gridSize=" + gridSize + ", arg=" + arg + ']');

            assert arg != null;

            taskNum = (Integer)arg;

            assert taskNum != -1;

            Collection<ComputeJob> jobs = new ArrayList<>(SPLIT_COUNT);

            for (int i = 1; i <= SPLIT_COUNT; i++) {
                jobs.add(new ComputeJobAdapter(i) {
                    /** */
                    private volatile Thread thread;

                    /** {@inheritDoc} */
                    @SuppressWarnings({"BusyWait"})
                    @Override public Serializable execute() {
                        assert taskSes != null;

                        thread = Thread.currentThread();

                        int arg = this.<Integer>argument(0);

                        if (log.isInfoEnabled())
                            log.info("Computing job [job=" + this + ", arg=" + arg + ']');

                        startSignal[taskNum].countDown();

                        try {
                            if (!startSignal[taskNum].await(WAIT_TIME, TimeUnit.MILLISECONDS))
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
                                log.info("Job got interrupted [arg=" + arg + ", e=" + e + ']');

                            return "interrupt-job-data";
                        }

                        if (log.isInfoEnabled())
                            log.info("Completing job: " + taskSes);

                        return arg;
                    }

                    /** {@inheritDoc} */
                    @Override public void cancel() {
                        assert thread != null;

                        interruptCnt[taskNum].incrementAndGet();

                        stopSignal[taskNum].countDown();
                    }
                });
            }

            return jobs;
        }

        /** {@inheritDoc} */
        @SuppressWarnings("deprecation")
        @Override public ComputeJobResultPolicy result(ComputeJobResult result, List<ComputeJobResult> received)
            throws GridException {
            if (received.size() == 1) {
                Collection<ComputeJobSibling> jobSiblings = taskSes.getJobSiblings();

                IgniteUuid jobId = received.get(0).getJobContext().getJobId();

                assert jobId != null;

                // Cancel all jobs except first job with argument 1.
                for (ComputeJobSibling jobSibling : jobSiblings) {
                    if (!jobId.equals(jobSibling.getJobId()))
                        jobSibling.cancel();
                }
            }

            return received.size() == SPLIT_COUNT ? ComputeJobResultPolicy.REDUCE : ComputeJobResultPolicy.WAIT;
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
