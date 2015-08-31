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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.compute.ComputeTaskFuture;
import org.apache.ignite.internal.util.lang.GridAbsClosure;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.loadtests.util.GridCumulativeAverage;
import org.apache.ignite.testframework.GridFileLock;
import org.apache.ignite.testframework.GridLoadTestUtils;
import org.jetbrains.annotations.Nullable;
import org.jsr166.LongAdder8;

/**
 *
 */
public class GridJobExecutionLoadTestClientSemaphore implements Callable<Object> {
    /** Performance stats update interval in seconds. */
    private static final int UPDATE_INTERVAL_SEC = 10;

    /** Warm-up duration. */
    public static final int WARM_UP_DURATION = 60 * 1000;

    /** Grid. */
    private static Ignite g;

    /** Transaction count. */
    private static LongAdder8 txCnt = new LongAdder8();

    /** Finish flag. */
    private static volatile boolean finish;

    /** */
    private static Semaphore tasksSem;

    /** {@inheritDoc} */
    @SuppressWarnings("InfiniteLoopStatement")
    @Nullable @Override public Object call() throws Exception {
        final IgniteInClosure<IgniteFuture<?>> lsnr = new CI1<IgniteFuture<?>>() {
            @Override public void apply(IgniteFuture<?> t) {
                tasksSem.release();
            }
        };

        ClusterGroup rmts = g.cluster().forRemotes();

        IgniteCompute comp = g.compute(rmts).withAsync();

        while (!finish) {
            tasksSem.acquire();

            comp.execute(GridJobExecutionLoadTestTask.class, null);

            ComputeTaskFuture<Object> f = comp.future();

            f.listen(lsnr);

            txCnt.increment();
        }

        return null;
    }

    /**
     * @param args Args.
     * @throws Exception If failed.
     */
    public static void main(String[] args) throws Exception {
        GridFileLock fileLock = GridLoadTestUtils.fileLock();

        fileLock.lock();

        try {
            final int noThreads = args.length > 0 ? Integer.parseInt(args[0]) :
                Runtime.getRuntime().availableProcessors();
            final int duration = args.length > 1 ? Integer.parseInt(args[1]) : 0;
            int tasksCnt  = args.length > 2 ? Integer.parseInt(args[2]) : 4069;
            final String outputFileName = args.length > 3 ? args[3] : null;

            X.println("Thread count: " + noThreads);
            X.println("Tasks count: " + tasksCnt);

            tasksSem = new Semaphore(tasksCnt);

            g = G.start("modules/tests/config/jobs-load-client.xml");

            warmUp(noThreads);

            final Thread collector = new Thread(new Runnable() {
                @SuppressWarnings("BusyWait")
                @Override public void run() {
                    GridCumulativeAverage avgTxPerSec = new GridCumulativeAverage();

                    try {
                        while (!finish) {
                            Thread.sleep(UPDATE_INTERVAL_SEC * 1000);

                            long txPerSec = txCnt.sumThenReset() / UPDATE_INTERVAL_SEC;

                            X.println(">>>");
                            X.println(">>> Transactions/s: " + txPerSec);

                            avgTxPerSec.update(txPerSec);
                        }
                    }
                    catch (InterruptedException ignored) {
                        X.println(">>> Interrupted.");

                        Thread.currentThread().interrupt();
                    }

                    X.println(">>> Average Transactions/s: " + avgTxPerSec);

                    if (outputFileName != null) {
                        try {
                            X.println("Writing results to file: " + outputFileName);

                            GridLoadTestUtils.appendLineToFile(
                                outputFileName,
                                "%s,%d",
                                GridLoadTestUtils.DATE_TIME_FORMAT.format(new Date()),
                                avgTxPerSec.get()
                            );
                        }
                        catch (IOException e) {
                            X.error("Failed to output results to file.", e);
                        }
                    }
                }
            });

            X.println("Running main test...");

            Thread timer = null;

            try {
                ExecutorService pool = Executors.newFixedThreadPool(noThreads);

                Collection<Callable<Object>> clients = new ArrayList<>(noThreads);

                for (int i = 0; i < noThreads; i++)
                    clients.add(new GridJobExecutionLoadTestClientSemaphore());

                collector.start();

                if (duration > 0) {
                    timer = new Thread(new Runnable() {
                        @Override public void run() {
                            try {
                                Thread.sleep(duration * 1000);

                                finish = true;
                            }
                            catch (InterruptedException ignored) {
                                X.println(">>> Interrupted.");
                            }
                        }
                    });
                    timer.start();
                }

                pool.invokeAll(clients);

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
     * Warms the JVM up.
     *
     * @param noThreads Number of threads to use.
     */
    private static void warmUp(int noThreads) {
        X.println("Warming up...");

        final IgniteCompute rmts = g.compute(g.cluster().forRemotes());

        GridLoadTestUtils.runMultithreadedInLoop(new Callable<Object>() {
            @Nullable @Override public Object call() {
                try {
                    rmts.execute(GridJobExecutionLoadTestTask.class, null);
                }
                catch (IgniteException e) {
                    e.printStackTrace();
                }

                return null;
            }
        }, noThreads, WARM_UP_DURATION);

        // Run GC on all nodes.
        try {
            g.compute().run(new GridAbsClosure() {
                @Override public void apply() {
                    System.gc();
                }
            });
        }
        catch (IgniteException e) {
            throw new IllegalStateException(e);
        }
    }
}