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
import org.gridgain.grid.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.loadtests.util.*;
import org.gridgain.testframework.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.apache.ignite.compute.GridComputeJobResultPolicy.*;

/**
 * This test measures the performance of task execution engine by
 * submitting empty tasks and collecting the average tasks/second
 * statistics.
 */
public class GridJobExecutionSingleNodeLoadTest {
    /** Stats update interval in seconds. */
    private static final int UPDATE_INTERVAL_SEC = 10;

    /** Warm-up duration. */
    public static final int WARM_UP_DURATION = 60 * 1000;

    /**
     * @param args Command line arguments:
     *             1-st: Number of worker threads. Default: 32.
     *             2-nd: Test duration in seconds. 0 means infinite. Default: 0.
     *             3-rd: File to output test results to.
     * @throws Exception If failed.
     */
    public static void main(String[] args) throws Exception {
        GridFileLock fileLock = GridLoadTestUtils.fileLock();

        fileLock.lock();

        try {
            // Command line arguments.
            int threadCnt = args.length == 0 ? 64 : Integer.parseInt(args[0]);
            final int duration = args.length < 2 ? 0 : Integer.parseInt(args[1]);
            final String outputFileName = args.length < 3 ? null : args[2];

            final AtomicLong tasksCnt = new AtomicLong();

            final AtomicBoolean finish = new AtomicBoolean();

            ExecutorService pool = Executors.newFixedThreadPool(threadCnt);

            Collection<Callable<Object>> producers = new ArrayList<>(threadCnt);

            Thread collector = null;

            Thread timer = null;

            try {
                final Ignite g = G.start("modules/core/src/test/config/grid-job-load.xml");

                X.println("Warming up...");

                GridLoadTestUtils.runMultithreadedInLoop(new Callable<Object>() {
                    @Override public Object call() throws GridException {
                        g.compute().execute(GridJobExecutionLoadTestTask.class, null);

                        return null;
                    }
                }, threadCnt, WARM_UP_DURATION);

                System.gc();

                X.println("Running main test.");

                for (int i = 0; i < threadCnt; i++)
                    producers.add(new Callable<Object>() {
                        @SuppressWarnings({"unchecked", "InfiniteLoopStatement"})
                        @Override public Object call() throws Exception {
                            while (!finish.get()) {
                                try {
                                    g.compute().execute(GridJobExecutionLoadTestTask.class, null);

                                    tasksCnt.incrementAndGet();
                                }
                                catch (GridComputeTaskCancelledException ignored) {
                                    // No-op.
                                }
                                catch (GridException e) {
                                    e.printStackTrace();
                                }
                            }

                            return null;
                        }
                    });

                // Thread that measures and outputs performance statistics.
                collector = new Thread(new Runnable() {
                    @SuppressWarnings({"BusyWait", "InfiniteLoopStatement"})
                    @Override public void run() {
                        GridCumulativeAverage avgTasksPerSec = new GridCumulativeAverage();

                        try {
                            while (!finish.get()) {
                                long cnt0 = tasksCnt.get();

                                Thread.sleep(UPDATE_INTERVAL_SEC * 1000);

                                long cnt1 = tasksCnt.get();

                                long curTasksPerSec = (cnt1 - cnt0) / UPDATE_INTERVAL_SEC;
                                X.println(">>> Tasks/s: " + curTasksPerSec);

                                avgTasksPerSec.update(curTasksPerSec);
                            }
                        }
                        catch (InterruptedException ignored) {
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
                    }
                });
                collector.start();

                if (duration > 0) {
                    // Thread that stops the test after a specified period of time.
                    timer = new Thread(new Runnable() {
                        @Override public void run() {
                            try {
                                Thread.sleep(duration * 1000);

                                finish.set(true);
                            }
                            catch (InterruptedException ignored) {
                                // No-op.
                            }
                        }
                    });
                    timer.start();
                }

                pool.invokeAll(producers);

                X.println("All done, stopping.");

                collector.interrupt();

                pool.shutdown();
            }
            finally {
                if (collector != null && !collector.isInterrupted())
                    collector.interrupt();

                if (timer != null)
                    timer.interrupt();

                G.stopAll(true);
            }
        }
        finally {
            fileLock.close();
        }
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
