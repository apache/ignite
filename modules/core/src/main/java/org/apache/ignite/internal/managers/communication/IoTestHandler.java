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

package org.apache.ignite.internal.managers.communication;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.GridTopic;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.NodeStoppingException;
import org.apache.ignite.internal.util.future.GridCompoundFuture;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.thread.pool.IgniteThreadPoolExecutor.newFixedThreadPool;

/** Communication SPI test handler. */
public class IoTestHandler {
    /** Test ID generator. */
    private static final AtomicLong ID_GEN = new AtomicLong();

    /** Kernal context. */
    private final GridKernalContext ctx;

    /** Logger. */
    private final IgniteLogger log;

    /** Pending test requests. */
    private final ConcurrentHashMap<Long, IoTestFuture> ioTests = new ConcurrentHashMap<>();

    /** Stop flag. */
    private volatile boolean stopping;

    /** Test-running flag. */
    private final AtomicBoolean testRunning = new AtomicBoolean();

    /** Active test. */
    private volatile IoTestRunFuture activeTest;

    /** Constructor. */
    public IoTestHandler(GridKernalContext ctx) {
        this.ctx = ctx;
        log = ctx.log(getClass());

        ctx.io().addMessageListener(GridTopic.TOPIC_IO_TEST, (nodeId, msg, plc) -> {
            IgniteIoTestMessage msg0 = (IgniteIoTestMessage)msg;

            if (msg0.request()) {
                msg0.onRequestProcessed();

                try {
                    ctx.io().sendToGridTopic(
                        nodeId,
                        GridTopic.TOPIC_IO_TEST,
                        new IgniteIoTestMessage(msg0),
                        GridIoPolicy.SYSTEM_POOL
                    );
                }
                catch (Exception e) {
                    LT.warn(log, "Failed to send IO test response [nodeId=" + nodeId + "]", e);
                }
            }
            else {
                msg0.onResponseProcessed();

                IoTestFuture fut = ioTests.get(msg0.testId());

                if (fut != null)
                    fut.onDone(msg0);
                else if (log.isDebugEnabled())
                    log.debug("Failed to find IO test future [msg=" + msg0 + ']');
            }
        });
    }

    /**
     * Sends one test request to every node.
     *
     * @param nodes Nodes.
     * @param payload Payload.
     * @param procFromNioThread Process messages in NIO threads.
     * @return Aggregate response future.
     */
    public GridCompoundFuture<IgniteIoTestMessage, Void> sendIoTest(
        List<ClusterNode> nodes,
        byte[] payload,
        boolean procFromNioThread
    ) {
        GridCompoundFuture<IgniteIoTestMessage, Void> resFut = new GridCompoundFuture<>();

        nodes.forEach(node -> resFut.add(sendIoTest(node, payload, procFromNioThread)));

        resFut.markInitialized();

        return resFut;
    }

    /**
     * Sends a test request.
     *
     * @param node Node.
     * @param payload Payload.
     * @param procFromNioThread Process messages in NIO threads.
     * @return Response future.
     */
    public IgniteInternalFuture<IgniteIoTestMessage> sendIoTest(
        ClusterNode node,
        byte[] payload,
        boolean procFromNioThread
    ) {
        if (stopping)
            return new GridFinishedFuture<>(stoppingException());

        long id = ID_GEN.getAndIncrement();

        IoTestFuture fut = new IoTestFuture(id);

        ioTests.put(id, fut);

        if (stopping)
            fut.onDone(stoppingException());
        else {
            try {
                ctx.io().sendToGridTopic(
                    node,
                    GridTopic.TOPIC_IO_TEST,
                    new IgniteIoTestMessage(id, payload, procFromNioThread),
                    GridIoPolicy.SYSTEM_POOL
                );
            }
            catch (IgniteCheckedException | RuntimeException e) {
                fut.onDone(e);
            }
        }

        return fut;
    }

    /**
     * Runs a latency test against the supplied nodes.
     *
     * @param warmup Warmup duration in milliseconds.
     * @param duration Test duration in milliseconds.
     * @param threads Thread count.
     * @param latencyLimit RTT histogram upper bound in nanoseconds.
     * @param rangesCnt RTT histogram range count.
     * @param payloadSize Payload size in bytes.
     * @param procFromNioThread Process messages in NIO threads.
     * @param nodes Nodes participating in the test.
     * @return Test result future.
     */
    public IgniteInternalFuture<String> runIoTest(
        long warmup,
        long duration,
        int threads,
        long latencyLimit,
        int rangesCnt,
        int payloadSize,
        boolean procFromNioThread,
        List<ClusterNode> nodes
    ) {
        A.notEmpty(nodes, "nodes");

        if (stopping)
            return new GridFinishedFuture<>(stoppingException());

        A.ensure(testRunning.compareAndSet(false, true), "Communication IO test is already running.");

        List<ClusterNode> testNodes = new ArrayList<>(nodes);
        ExecutorService svc;

        try {
            svc = newFixedThreadPool("io-latency-inspector", ctx.igniteInstanceName(), threads);
        }
        catch (RuntimeException | Error e) {
            testRunning.set(false);

            throw e;
        }

        AtomicBoolean finished = new AtomicBoolean();
        IoTestRunFuture testRes = new IoTestRunFuture(svc, finished);
        AtomicInteger remaining = new AtomicInteger(threads);
        AtomicInteger firstSampleIdx = new AtomicInteger();
        byte[] payload = new byte[payloadSize];
        Map<UUID, IoTestNodeResults> results = new ConcurrentHashMap<>();
        long startNanos = System.nanoTime();
        long warmupNanos = TimeUnit.MILLISECONDS.toNanos(warmup);
        long totalNanos = warmupNanos + TimeUnit.MILLISECONDS.toNanos(duration);
        long responseTimeout = Math.max(1, ctx.config().getFailureDetectionTimeout());

        activeTest = testRes;

        if (stopping) {
            testRes.onDone(stoppingException());

            return testRes;
        }

        try {
            for (int i = 0; i < threads; i++) {
                int workerIdx = i;

                svc.execute(() -> {
                    long targetIdx = workerIdx;

                    try {
                        while (!finished.get() && elapsedNanos(startNanos) < warmupNanos) {
                            ClusterNode node = testNodes.get((int)(targetIdx++ % testNodes.size()));

                            sendAndMeasure(node, payload, procFromNioThread, responseTimeout);
                        }

                        for (int idx; !finished.get() && elapsedNanos(startNanos) < totalNanos &&
                            (idx = firstSampleIdx.getAndIncrement()) < testNodes.size(); ) {
                            recordResult(results, testNodes.get(idx), payload, procFromNioThread,
                                responseTimeout, rangesCnt, latencyLimit);
                        }

                        while (!finished.get() && elapsedNanos(startNanos) < totalNanos)
                            recordResult(results, testNodes.get((int)(targetIdx++ % testNodes.size())), payload,
                                procFromNioThread,
                                responseTimeout, rangesCnt, latencyLimit);
                    }
                    catch (Exception e) {
                        testRes.onDone(e);
                    }
                    finally {
                        if (remaining.decrementAndGet() == 0 && !finished.get()) {
                            if (results.size() != testNodes.size()) {
                                testRes.onDone(new IgniteCheckedException(
                                    "Communication SPI test duration is too short to sample every target node " +
                                        "[targets=" + testNodes.size() + ", sampled=" + results.size() + ']'));
                            }
                            else {
                                try {
                                    testRes.onDone(formatResults(results, payloadSize, warmup, duration, threads,
                                        procFromNioThread));
                                }
                                catch (RuntimeException e) {
                                    testRes.onDone(e);
                                }
                            }
                        }
                    }
                });
            }
        }
        catch (RuntimeException e) {
            testRes.onDone(e);
        }

        return testRes;
    }

    /** Stops this handler and completes pending requests. */
    void stop() {
        stopping = true;

        NodeStoppingException err = stoppingException();

        ioTests.values().forEach(fut -> fut.onDone(err));

        IoTestRunFuture test = activeTest;

        if (test != null)
            test.onDone(err);
    }

    /** Records one round-trip result. */
    private void recordResult(
        Map<UUID, IoTestNodeResults> results,
        ClusterNode node,
        byte[] payload,
        boolean procFromNioThread,
        long responseTimeout,
        int rangesCnt,
        long latencyLimit
    ) throws IgniteCheckedException {
        IgniteIoTestMessage res = sendAndMeasure(node, payload, procFromNioThread, responseTimeout);

        results.computeIfAbsent(node.id(), ignored -> new IoTestNodeResults(rangesCnt, latencyLimit))
            .onResult(res);
    }

    /** Sends a request and measures its round-trip time on the local node. */
    private IgniteIoTestMessage sendAndMeasure(
        ClusterNode node,
        byte[] payload,
        boolean procFromNioThread,
        long responseTimeout
    ) throws IgniteCheckedException {
        IgniteInternalFuture<IgniteIoTestMessage> fut = sendIoTest(node, payload, procFromNioThread);

        try {
            return fut.get(responseTimeout);
        }
        catch (IgniteCheckedException e) {
            throw new IgniteCheckedException("Communication SPI test request failed [nodeId=" + node.id() +
                ", addresses=" + node.addresses() + ']', e);
        }
        finally {
            if (!fut.isDone())
                fut.cancel();
        }
    }

    /** Returns monotonic elapsed time. */
    private static long elapsedNanos(long startNanos) {
        return System.nanoTime() - startNanos;
    }

    /** Formats test results. */
    private String formatResults(
        Map<UUID, IoTestNodeResults> rawResults,
        int payloadSize,
        long warmup,
        long duration,
        int threads,
        boolean procFromNioThread
    ) {
        Map<UUID, IoTestNodeResults> results = new TreeMap<>(rawResults);

        ClusterNode source = ctx.discovery().localNode();
        StringBuilder b = new StringBuilder("Communication SPI test").append(U.nl())
            .append("Source node: ").append(source.id()).append(" [addresses=").append(source.addresses()).append(']')
            .append(U.nl())
            .append("Payload: ").append(payloadSize).append(" bytes each way").append(U.nl())
            .append("Message handling: ").append(procFromNioThread ? "NIO thread" : "system pool").append(U.nl())
            .append("Warmup: ").append(warmup).append(" ms | Duration: ").append(duration)
            .append(" ms | Threads: ").append(threads).append(U.nl());

        for (Map.Entry<UUID, IoTestNodeResults> entry : results.entrySet()) {
            ClusterNode node = ctx.discovery().node(entry.getKey());
            IoTestNodeResults nodeResults = entry.getValue();

            b.append(U.nl())
                .append("Target node: ").append(entry.getKey()).append(" [addresses=")
                .append(node != null ? node.addresses() : "n/a")
                .append(']').append(U.nl())
                .append("Samples: ").append(nodeResults.count).append(U.nl())
                .append(String.format(Locale.ROOT, "End-to-end RTT (ms): min=%.3f, avg=%.3f, max=%.3f%n",
                    nodeResults.minLatency / 1_000_000.0,
                    nodeResults.totalLatency / (double)nodeResults.count / 1_000_000,
                    nodeResults.maxLatency / 1_000_000.0))
                .append("Node-local delays (ms, min/avg/max):").append(U.nl());

            appendTiming(b, "Source request pre-serialization delay", nodeResults.reqSndQueue, 1_000_000);
            appendTiming(b, "Target request dispatch delay", nodeResults.reqRcvQueue, 1_000_000);
            appendTiming(b, "Target response pre-serialization delay", nodeResults.resSndQueue, 1_000_000);
            appendTiming(b, "Source response dispatch delay", nodeResults.resRcvQueue, 1_000_000);

            b.append("Estimated one-way message delay (ms, min/avg/max):").append(U.nl())
                .append("  Clock assumption: synchronized wall clocks; negative values indicate clock offset or ")
                .append("wall-clock adjustment.")
                .append(U.nl());

            appendTiming(b, "Request (serialization start -> deserialization complete)", nodeResults.reqWireTime, 1);
            appendTiming(b, "Response (serialization start -> deserialization complete)", nodeResults.resWireTime, 1);

            b.append("RTT histogram:").append(U.nl());

            for (int i = 0; i < nodeResults.resLatency.length; i++) {
                long bucketCount = nodeResults.resLatency[i];

                if (bucketCount == 0)
                    continue;

                double lowerBound = nodeResults.binUpperBound(i) / 1_000_000.0;
                String range = i < nodeResults.resLatency.length - 1
                    ? String.format(Locale.ROOT, "[%.3f, %.3f) ms", lowerBound,
                        nodeResults.binUpperBound(i + 1) / 1_000_000.0)
                    : String.format(Locale.ROOT, "[%.3f, +inf) ms", lowerBound);

                b.append(String.format(Locale.ROOT, "  %-31s %d (%.2f%%)%n",
                    range + ':',
                    bucketCount,
                    100.0 * bucketCount / nodeResults.count));
            }
        }

        return b.toString();
    }

    /** Appends min/average/max timing values. */
    private static void appendTiming(StringBuilder b, String name, TimingStats timing, double divisor) {
        b.append(String.format(Locale.ROOT, "  %s: min=%.3f, avg=%.3f, max=%.3f%n",
            name, timing.min / divisor, timing.average() / divisor, timing.max / divisor));
    }

    /** Creates a node-stopping error. */
    private NodeStoppingException stoppingException() {
        return new NodeStoppingException("IO test has been cancelled because the local node is stopping: " +
            ctx.localNodeId());
    }

    /** Pending request future. */
    private class IoTestFuture extends GridFutureAdapter<IgniteIoTestMessage> {
        /** Test ID. */
        private final long id;

        /** Constructor. */
        IoTestFuture(long id) {
            this.id = id;
        }

        /** {@inheritDoc} */
        @Override protected boolean onDone(
            @Nullable IgniteIoTestMessage res,
            @Nullable Throwable err,
            boolean cancel
        ) {
            if (super.onDone(res, err, cancel)) {
                ioTests.remove(id, this);

                return true;
            }

            return false;
        }

        /** {@inheritDoc} */
        @Override public boolean cancel() {
            return onCancelled();
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(IoTestFuture.class, this);
        }
    }

    /** Running test future. */
    private class IoTestRunFuture extends GridFutureAdapter<String> {
        /** Test executor. */
        private final ExecutorService svc;

        /** Finished flag shared with workers. */
        private final AtomicBoolean finished;

        /** Constructor. */
        IoTestRunFuture(ExecutorService svc, AtomicBoolean finished) {
            this.svc = svc;
            this.finished = finished;
        }

        /** {@inheritDoc} */
        @Override protected boolean onDone(@Nullable String res, @Nullable Throwable err, boolean cancel) {
            if (!finished.compareAndSet(false, true))
                return false;

            if (cancel || err != null)
                svc.shutdownNow();
            else
                svc.shutdown();

            activeTest = null;
            testRunning.set(false);

            return super.onDone(res, err, cancel);
        }

        /** {@inheritDoc} */
        @Override public boolean cancel() {
            return onCancelled();
        }
    }

    /** Aggregated node results. */
    private static class IoTestNodeResults {
        /** Histogram. */
        private final long[] resLatency;

        /** Histogram range count. */
        private final int rangesCnt;

        /** Maximum expected latency. */
        private final long latencyLimit;

        /** Total latency. */
        private long totalLatency;

        /** Minimum latency. */
        private long minLatency = Long.MAX_VALUE;

        /** Maximum latency. */
        private long maxLatency;

        /** Sample count. */
        private long count;

        /** Request send queue statistics. */
        private final TimingStats reqSndQueue = new TimingStats();

        /** Request receive queue statistics. */
        private final TimingStats reqRcvQueue = new TimingStats();

        /** Response send queue statistics. */
        private final TimingStats resSndQueue = new TimingStats();

        /** Response receive queue statistics. */
        private final TimingStats resRcvQueue = new TimingStats();

        /** Approximate request transfer statistics. */
        private final TimingStats reqWireTime = new TimingStats();

        /** Approximate response transfer statistics. */
        private final TimingStats resWireTime = new TimingStats();

        /** Constructor. */
        IoTestNodeResults(int rangesCnt, long latencyLimit) {
            this.rangesCnt = rangesCnt;
            this.latencyLimit = latencyLimit;

            resLatency = new long[rangesCnt + 1];
        }

        /** Adds a sample. */
        synchronized void onResult(IgniteIoTestMessage msg) {
            long latency = msg.roundTripNanos();
            int idx = latency >= latencyLimit
                ? rangesCnt
                : (int)(latency * (double)rangesCnt / latencyLimit);

            resLatency[idx]++;
            totalLatency += latency;
            minLatency = Math.min(minLatency, latency);
            maxLatency = Math.max(maxLatency, latency);
            count++;

            reqSndQueue.add(msg.requestSendQueueNanos());
            reqRcvQueue.add(msg.requestReceiveQueueNanos());
            resSndQueue.add(msg.responseSendQueueNanos());
            resRcvQueue.add(msg.responseReceiveQueueNanos());
            reqWireTime.add(msg.requestWireTimeMillis());
            resWireTime.add(msg.responseWireTimeMillis());
        }

        /** Returns the upper bound of a histogram bin. */
        double binUpperBound(int bin) {
            return latencyLimit * (double)bin / rangesCnt;
        }
    }

    /** Min/average/max accumulator. */
    private static class TimingStats {
        /** Minimum. */
        private long min = Long.MAX_VALUE;

        /** Maximum. */
        private long max = Long.MIN_VALUE;

        /** Sum. */
        private double total;

        /** Count. */
        private long count;

        /** Adds a value. */
        void add(long val) {
            min = Math.min(min, val);
            max = Math.max(max, val);
            total += val;
            count++;
        }

        /** @return Average. */
        double average() {
            return total / count;
        }
    }
}
