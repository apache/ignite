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
import org.gridgain.grid.util.lang.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.loadtests.util.*;
import org.gridgain.testframework.*;
import org.jdk8.backport.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

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
    private static LongAdder txCnt = new LongAdder();

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

        IgniteCompute comp = g.compute(rmts).enableAsync();

        while (!finish) {
            tasksSem.acquire();

            comp.execute(GridJobExecutionLoadTestTask.class, null);

            ComputeTaskFuture<Object> f = comp.future();

            f.listenAsync(lsnr);

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
                catch (GridException e) {
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
        catch (GridException e) {
            throw new IllegalStateException(e);
        }
    }
}
