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

package org.apache.ignite.loadtests.job;

import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Semaphore;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeJobResultPolicy;
import org.apache.ignite.compute.ComputeTask;
import org.apache.ignite.compute.ComputeTaskFuture;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.loadtests.util.GridCumulativeAverage;
import org.apache.ignite.testframework.GridFileLock;
import org.apache.ignite.testframework.GridLoadTestUtils;
import org.apache.ignite.testframework.GridTestUtils;
import org.jetbrains.annotations.Nullable;
import org.jsr166.LongAdder8;

import static org.apache.ignite.compute.ComputeJobResultPolicy.REDUCE;

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

            final LongAdder8 execCnt = new LongAdder8();

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

                IgniteInternalFuture<Void> collectorFut = GridTestUtils.runAsync(new Callable<Void>() {
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
                        catch (IgniteInterruptedCheckedException ignored) {
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
        final LongAdder8 iterCntr) {
        final Semaphore sem = new Semaphore(taskCnt);

        final IgniteInClosure<IgniteFuture> lsnr = new CI1<IgniteFuture>() {
            @Override public void apply(IgniteFuture t) {
                sem.release();
            }
        };

        GridLoadTestUtils.runMultithreadedInLoop(new Callable<Object>() {
            @Nullable @Override public Object call() throws Exception {
                sem.acquire();

                IgniteCompute comp = g.compute().withAsync();

                comp.execute(GridJobExecutionLoadTestTask.class, null);

                ComputeTaskFuture<Object> f = comp.future();

                f.listen(lsnr);

                iterCntr.increment();

                return null;
            }
        }, threadCnt, dur > 0 ? dur : Long.MAX_VALUE);
    }

    /**
     * Empty task (spawns one empty job).
     */
    private static class GridJobExecutionLoadTestTask implements ComputeTask<Object, Object> {
        /** {@inheritDoc} */
        @Nullable @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid, @Nullable Object arg) {
            return F.asMap(new GridJobExecutionLoadTestJob(), subgrid.get(0));
        }

        /** {@inheritDoc} */
        @Override public ComputeJobResultPolicy result(ComputeJobResult res, List<ComputeJobResult> rcvd) {
            return REDUCE;
        }

        /** {@inheritDoc} */
        @Nullable @Override public Object reduce(List<ComputeJobResult> results) {
            return null;
        }
    }

    /**
     * Empty job.
     */
    private static class GridJobExecutionLoadTestJob implements ComputeJob {
        /** {@inheritDoc} */
        @Override public Object execute() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public void cancel() {
            // No-op.
        }
    }
}