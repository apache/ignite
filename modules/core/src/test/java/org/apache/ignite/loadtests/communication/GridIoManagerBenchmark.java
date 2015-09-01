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

package org.apache.ignite.loadtests.communication;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.managers.communication.GridIoManager;
import org.apache.ignite.internal.managers.communication.GridMessageListener;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManager;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.loadtests.util.GridCumulativeAverage;
import org.apache.ignite.testframework.GridLoadTestUtils;
import org.jetbrains.annotations.Nullable;
import org.jsr166.ConcurrentHashMap8;
import org.jsr166.LongAdder8;
import org.jsr166.ThreadLocalRandom8;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.internal.managers.communication.GridIoPolicy.PUBLIC_POOL;
import static org.apache.ignite.testframework.GridLoadTestUtils.appendLineToFile;
import static org.apache.ignite.testframework.GridLoadTestUtils.startDaemon;

/**
 * By default this benchmarks uses original Ignite configuration
 * with message dispatching from NIO threads.
 *
 * By changing {@link #DFLT_CONFIG} constant you can use ForkJoin thread pool instead of JDK default.
 *
 * Note that you should run 2 processes of this test to get it running.
 */
public class GridIoManagerBenchmark {
    /** */
    public static final String DFLT_CONFIG = "modules/tests/config/io-manager-benchmark.xml";

    /** */
    private static final int DFLT_THREADS = 2;

    /** */
    private static final long WARM_UP_DUR = 30 * 1000;

    /** */
    private static final Semaphore sem = new Semaphore(10 * 1024);

    /** */
    public static final int TEST_TOPIC = 1;

    /** */
    private static final LongAdder8 msgCntr = new LongAdder8();

    /** */
    private static final Map<IgniteUuid, CountDownLatch> latches = new ConcurrentHashMap8<>();

    /** */
    private static final byte[][] arrs;

    /** */
    private static boolean testHeavyMsgs;

    /** */
    private static boolean testLatency;

    /**
     *
     */
    static {
        ThreadLocalRandom8 rnd = ThreadLocalRandom8.current();

        arrs = new byte[64][];

        for (int i = 0; i < arrs.length; i++) {
            byte[] arr = new byte[rnd.nextInt(4096, 8192)];

            for (int j = 0; j < arr.length; j++)
                arr[j] = (byte)rnd.nextInt(0, 127);

            arrs[i] = arr;
        }
    }

    /**
     * @param args Command line arguments.
     */
    public static void main(String[] args) {
        int threads = args.length > 0 ? Integer.parseInt(args[0]) : DFLT_THREADS;
        int duration =  args.length > 1 ? Integer.parseInt(args[1]) : 0;
        String outputFilename = args.length > 2 ? args[2] : null;
        String path = args.length > 3 ? args[3] : DFLT_CONFIG;
        testHeavyMsgs = args.length > 4 && "true".equalsIgnoreCase(args[4]);
        testLatency = args.length > 5 && "true".equalsIgnoreCase(args[5]);

//        threads = 128;
//        testLatency = true;
//        testHeavyMsgs = true;

        X.println("Config: " + path);
        X.println("Test heavy messages: " + testHeavyMsgs);
        X.println("Test latency: " + testLatency);
        X.println("Threads: " + threads);
        X.println("Duration: " + duration);
        X.println("Output file name: " + outputFilename);

        IgniteKernal g = (IgniteKernal)G.start(path);

        if (g.localNode().order() > 1) {
            try {
                sendMessages(g, threads, duration, outputFilename);
            }
            finally {
                G.stopAll(false);
            }
        }
        else
            receiveMessages(g);
    }

    /**
     * @param g Kernal.
     * @param threads Number of send threads.
     * @param duration Test duration.
     * @param outputFilename Output file name.
     */
    @SuppressWarnings("deprecation")
    private static void sendMessages(IgniteKernal g, int threads, int duration, @Nullable final String outputFilename) {
        X.println(">>> Sending messages.");

        g.context().io().addMessageListener(TEST_TOPIC, new SenderMessageListener());

        Thread collector = startDaemon(new Runnable() {
            @Override public void run() {
                final long initTs = System.currentTimeMillis();
                long ts = initTs;
                long queries = msgCntr.sum();
                GridCumulativeAverage qpsAvg = new GridCumulativeAverage();

                try {
                    while (!Thread.currentThread().isInterrupted()) {
                        U.sleep(10000);

                        long newTs = System.currentTimeMillis();
                        long newQueries = msgCntr.sum();

                        long executed = newQueries - queries;
                        long time = newTs - ts;

                        long qps = executed * 1000 / time;

                        boolean recordAvg = ts - initTs > WARM_UP_DUR;

                        if (recordAvg) qpsAvg.update(qps);

                        X.println("Communication benchmark [qps=" + qps + (recordAvg ? ", qpsAvg=" + qpsAvg : "") +
                            ", executed=" + executed + ", time=" + time + ']');

                        ts = newTs;
                        queries = newQueries;
                    }
                }
                catch (IgniteInterruptedCheckedException ignored) {
                    // No-op.
                }

                X.println("Average QPS: " + qpsAvg);

                if (outputFilename != null) {
                    try {
                        X.println("Saving results to output file: " + outputFilename);

                        appendLineToFile(outputFilename, "%s,%d", GridLoadTestUtils.DATE_TIME_FORMAT.format(new Date
                            ()), qpsAvg.get());
                    }
                    catch (IOException e) {
                        X.println("Failed to record results to a file: " + e.getMessage());
                    }
                }
            }
        });

        Collection<SendThread> sndThreads = new ArrayList<>(threads);

        for (int i = 0; i < threads; i++) {
            SendThread t = new SendThread(g);

            sndThreads.add(t);

            t.start();
        }

        try {
            U.sleep(duration > 0 ? duration * 1000 + WARM_UP_DUR : Long.MAX_VALUE);
        }
        catch (IgniteInterruptedCheckedException ignored) {
            // No-op.
        }

        collector.interrupt();

        for (SendThread t : sndThreads)
            t.interrupt();
    }

    /**
     * @param g Kernal.
     */
    @SuppressWarnings("deprecation")
    private static void receiveMessages(final IgniteKernal g) {
        X.println(">>> Receiving messages.");

        final GridIoManager io = g.context().io();

        GridMessageListener lsnr = new GridMessageListener() {
            private ClusterNode node;

            @Override public void onMessage(UUID nodeId, Object msg) {
                if (node == null)
                    node = g.context().discovery().node(nodeId);

                GridTestMessage testMsg = ((GridTestMessage)msg);

                testMsg.bytes(null);

                try {
                    io.send(node, TEST_TOPIC, testMsg, PUBLIC_POOL);
                }
                catch (IgniteCheckedException e) {
                    e.printStackTrace();
                }
            }
        };

        io.addMessageListener(TEST_TOPIC, lsnr);
    }

    /**
     *
     */
    private static class SendThread extends Thread {
        /** */
        private final IgniteKernal g;

        /**
         * @param g Kernal.
         */
        SendThread(IgniteKernal g) {
            this.g = g;
        }

        /** {@inheritDoc} */
        @Override public void run() {
            try {
                ClusterNode dst = awaitOther(g.context().discovery());

                GridIoManager io = g.context().io();

                Random rnd = ThreadLocalRandom8.current();

                IgniteUuid msgId = IgniteUuid.randomUuid();

                while (!Thread.interrupted()) {
                    CountDownLatch latch = null;

                    if (testLatency)
                        latches.put(msgId, latch = new CountDownLatch(1));
                    else
                        sem.acquire();

                    io.send(
                        dst,
                        TEST_TOPIC,
                        new GridTestMessage(msgId, testHeavyMsgs ? arrs[rnd.nextInt(arrs.length)] : null),
                        PUBLIC_POOL);

                    if (testLatency && !latch.await(1000, MILLISECONDS))
                        throw new RuntimeException("Failed to await latch.");
                }
            }
            catch (IgniteCheckedException e) {
                e.printStackTrace();
            }
            catch (InterruptedException ignored) {
                // No-op.
            }
        }

        /**
         * @param disc Discovery.
         * @return Second node in the topology.
         * @throws InterruptedException If interrupted.
         */
        @SuppressWarnings("BusyWait")
        private ClusterNode awaitOther(final GridDiscoveryManager disc) throws InterruptedException {
            while (disc.allNodes().size() < 2)
                Thread.sleep(1000);

            for (ClusterNode node : disc.allNodes())
                if (!F.eqNodes(node, disc.localNode()))
                    return node;

            assert false;

            return null;
        }
    }

    /**
     *
     */
    private static class SenderMessageListener implements GridMessageListener {
        /** {@inheritDoc} */
        @Override public void onMessage(UUID nodeId, Object msg) {
            msgCntr.increment();

            if (testLatency)
                latches.get(((GridTestMessage)msg).id()).countDown();
            else
                sem.release();
        }
    }
}