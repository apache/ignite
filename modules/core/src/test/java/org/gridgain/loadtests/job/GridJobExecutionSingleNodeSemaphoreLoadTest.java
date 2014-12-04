/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.loadtests.job;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.compute.*;
import org.apache.ignite.lang.*;
import org.gridgain.grid.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.loadtests.util.*;
import org.gridgain.testframework.*;
import org.jdk8.backport.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import static org.apache.ignite.compute.GridComputeJobResultPolicy.*;

/**
 * This test measures the performance of task execution engine by
 * submitting empty tasks and collecting the average tasks/second
 * statistics.
 */
public class GridJobExecutionSingleNodeSemaphoreLoadTest {
    /** Stats update interval in seconds. */
    private static final int UPDATE_INTERVAL_SEC = 10;

    /** Warm-up duration. */
    public static final int WARM_UP_DURATION = 60 * 1000;

    /**
     * @param args Command line arguments:
     *             1-st: Number of worker threads. Default equals to available CPU number / 2.
     *             2-nd: Concurrent tasks count. Default: 1024.
     *             3-rd: Test duration in seconds. 0 means infinite. Default: 0.
     *             4-th: File to output test results to.
     * @throws Exception If failed.
     */
    public static void main(String[] args) throws Exception {
        GridFileLock fileLock = GridLoadTestUtils.fileLock();

        fileLock.lock();

        try {
            // Command line arguments.
            //
            // NOTE: on MacOS better numbers are shown if public pool core and max sizes are
            // equal to CPU count. And producer threads count is equal to CPU count.
            //
            int threadCnt = args.length > 0 ? Integer.parseInt(args[0]) :
                Runtime.getRuntime().availableProcessors() / 2;
            int taskCnt = args.length > 1 ? Integer.parseInt(args[1]) : 1024;
            final int duration = args.length > 2 ? Integer.parseInt(args[2]) : 0;
            final String outputFileName = args.length > 3 ? args[3] : null;

            final LongAdder execCnt = new LongAdder();

            try {
                final Ignite g = G.start("modules/tests/config/grid-job-load.xml");

                X.println("Thread count: " + threadCnt);
                X.println("Task count: " + taskCnt);
                X.println("Duration: " + duration);

                X.println("Warming up...");

                g.compute().execute(GridJobExecutionLoadTestTask.class, null);
                g.compute().execute(GridJobExecutionLoadTestTask.class, null);

                runTest(g, threadCnt, taskCnt, WARM_UP_DURATION, execCnt);

                System.gc();

                execCnt.reset();

                X.println("Running main test.");

                IgniteFuture<Void> collectorFut = GridTestUtils.runAsync(new Callable<Void>() {
                    @Override public Void call() throws Exception {
                        GridCumulativeAverage avgTasksPerSec = new GridCumulativeAverage();

                        try {
                            while (!Thread.currentThread().isInterrupted()) {
                                U.sleep(UPDATE_INTERVAL_SEC * 1000);

                                long curTasksPerSec = execCnt.sumThenReset() / UPDATE_INTERVAL_SEC;

                                X.println(">>> Tasks/s: " + curTasksPerSec);

                                avgTasksPerSec.update(curTasksPerSec);
                            }
                        }
                        catch (GridInterruptedException ignored) {
                            X.println(">>> Interrupted.");

                            Thread.currentThread().interrupt();
                        }

                        X.println(">>> Average tasks/s: " + avgTasksPerSec);

                        if (outputFileName != null) {
                            X.println("Writing test results to a file: " + outputFileName);

                            try {
                                GridLoadTestUtils.appendLineToFile(
                                    outputFileName,
                                    "%s,%d",
                                    GridLoadTestUtils.DATE_TIME_FORMAT.format(new Date()),
                                    avgTasksPerSec.get());
                            }
                            catch (IOException e) {
                                X.error("Failed to output to a file", e);
                            }
                        }

                        return null;
                    }
                });

                runTest(g, threadCnt, taskCnt, duration * 1000, execCnt);

                X.println("All done, stopping.");

                collectorFut.cancel();
            }
            finally {
                G.stopAll(true);
            }
        }
        finally {
            fileLock.close();
        }
    }

    /**
     * Runs the actual load test.
     *
     * @param g Grid.
     * @param threadCnt Number of threads.
     * @param taskCnt Number of tasks.
     * @param dur Test duration.
     * @param iterCntr Iteration counter.
     */
    private static void runTest(final Ignite g, int threadCnt, int taskCnt, long dur,
        final LongAdder iterCntr) {
        final Semaphore sem = new Semaphore(taskCnt);

        final IgniteInClosure<IgniteFuture> lsnr = new CI1<IgniteFuture>() {
            @Override public void apply(IgniteFuture t) {
                sem.release();
            }
        };

        GridLoadTestUtils.runMultithreadedInLoop(new Callable<Object>() {
            @Nullable @Override public Object call() throws Exception {
                sem.acquire();

                IgniteCompute comp = g.compute().enableAsync();

                comp.execute(GridJobExecutionLoadTestTask.class, null);

                GridComputeTaskFuture<Object> f = comp.future();

                f.listenAsync(lsnr);

                iterCntr.increment();

                return null;
            }
        }, threadCnt, dur > 0 ? dur : Long.MAX_VALUE);
    }

    /**
     * Empty task (spawns one empty job).
     */
    private static class GridJobExecutionLoadTestTask implements GridComputeTask<Object, Object> {
        /** {@inheritDoc} */
        @Nullable @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid, @Nullable Object arg)
            throws GridException {
            return F.asMap(new GridJobExecutionLoadTestJob(), subgrid.get(0));
        }

        /** {@inheritDoc} */
        @Override public GridComputeJobResultPolicy result(GridComputeJobResult res, List<GridComputeJobResult> rcvd) throws GridException {
            return REDUCE;
        }

        /** {@inheritDoc} */
        @Nullable @Override public Object reduce(List<GridComputeJobResult> results) throws GridException {
            return null;
        }
    }

    /**
     * Empty job.
     */
    private static class GridJobExecutionLoadTestJob implements ComputeJob {
        /** {@inheritDoc} */
        @Override public Object execute() throws GridException {
            return null;
        }

        /** {@inheritDoc} */
        @Override public void cancel() {
            // No-op.
        }
    }
}
