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
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.GridTopic;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.NodeStoppingException;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.internal.util.typedef.internal.S;
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
                try {
                    IgniteIoTestMessage res = new IgniteIoTestMessage(msg0);

                    if (ctx.localNodeId().equals(nodeId))
                        res.onBeforeLocalSend();

                    ctx.io().sendToGridTopic(
                        nodeId,
                        GridTopic.TOPIC_IO_TEST,
                        res,
                        GridIoPolicy.SYSTEM_POOL
                    );
                }
                catch (Exception e) {
                    LT.warn(log, "Failed to send IO test response [nodeId=" + nodeId + "]", e);
                }
            }
            else {
                IoTestFuture fut = ioTests.get(msg0.testId());

                if (fut != null)
                    fut.onResponse(msg0);
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
     * @param processInNioThread Process messages in NIO threads.
     * @return Aggregate response future.
     */
    public IgniteInternalFuture<Void> sendIoTest(
        List<ClusterNode> nodes,
        byte[] payload,
        boolean processInNioThread
    ) {
        IoTestCompoundFuture resFut = new IoTestCompoundFuture();

        try {
            for (ClusterNode node : nodes) {
                if (!resFut.add(node, payload, processInNioThread))
                    break;
            }
        }
        catch (RuntimeException | Error e) {
            resFut.onFailure(e);

            throw e;
        }

        resFut.markInitialized();

        return resFut;
    }

    /**
     * Sends a test request.
     *
     * @param node Node.
     * @param payload Payload.
     * @param processInNioThread Process messages in NIO threads.
     * @return Response future.
     */
    public IgniteInternalFuture<IgniteIoTestMessage> sendIoTest(
        ClusterNode node,
        byte[] payload,
        boolean processInNioThread
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
                IgniteIoTestMessage msg = new IgniteIoTestMessage(id, payload, processInNioThread);

                if (ctx.localNodeId().equals(node.id()))
                    msg.onBeforeLocalSend();

                ctx.io().sendToGridTopic(
                    node,
                    GridTopic.TOPIC_IO_TEST,
                    msg,
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
     * Runs a latency test against the supplied nodes and returns raw structured results.
     *
     * @param warmup Warmup duration in milliseconds.
     * @param duration Test duration in milliseconds.
     * @param threads Thread count.
     * @param payloadSize Payload size in bytes.
     * @param processInNioThread Process messages in NIO threads.
     * @param nodes Nodes participating in the test.
     * @return Test result future.
     */
    public IgniteInternalFuture<IoTestResult> runIoTest(
        long warmup,
        long duration,
        int threads,
        int payloadSize,
        boolean processInNioThread,
        List<ClusterNode> nodes
    ) {
        A.notEmpty(nodes, "nodes");
        A.ensure(warmup >= 0 && warmup <= 3_600_000, "warmup must be between 0 and 3600000");
        A.ensure(duration > 0 && duration <= 3_600_000, "duration must be between 1 and 3600000");
        A.ensure(threads > 0 && threads <= 64, "threads must be between 1 and 64");
        A.ensure(payloadSize >= 0 && payloadSize <= 1024 * 1024, "payloadSize must be between 0 and 1048576");

        Map<UUID, IoTestNodeResults> results = new TreeMap<>();
        List<ClusterNode> testNodes = new ArrayList<>(nodes.size());

        for (ClusterNode node : nodes) {
            if (results.putIfAbsent(node.id(), new IoTestNodeResults(node)) == null)
                testNodes.add(node);
        }

        ClusterNode src = ctx.discovery().localNode();
        AtomicBoolean finished = new AtomicBoolean();

        if (stopping)
            return new GridFinishedFuture<>(stoppingException());

        if (!testRunning.compareAndSet(false, true))
            throw new IgniteException("Communication IO test is already running.");

        ExecutorService svc = null;
        IoTestRunFuture testRes;

        try {
            svc = newFixedThreadPool("io-latency-inspector", ctx.igniteInstanceName(), threads);
            testRes = new IoTestRunFuture(svc, finished);
        }
        catch (RuntimeException | Error e) {
            try {
                if (svc != null)
                    svc.shutdownNow();
            }
            finally {
                testRunning.set(false);
            }

            throw e;
        }

        activeTest = testRes;

        if (stopping) {
            testRes.onDone(stoppingException());

            return testRes;
        }

        try {
            AtomicInteger remaining = new AtomicInteger(threads);
            AtomicInteger firstSampleIdx = new AtomicInteger();
            byte[] payload = new byte[payloadSize];
            AtomicLong startNanos = new AtomicLong();
            CyclicBarrier startBarrier = new CyclicBarrier(threads, () -> startNanos.set(System.nanoTime()));
            long warmupNanos = TimeUnit.MILLISECONDS.toNanos(warmup);
            long durationNanos = TimeUnit.MILLISECONDS.toNanos(duration);
            long totalNanos = warmupNanos + durationNanos;
            long resTimeout = Math.max(1, ctx.config().getFailureDetectionTimeout());

            for (int i = 0; i < threads; i++) {
                int workerIdx = i;

                svc.execute(() -> {
                    long targetIdx = workerIdx;

                    try {
                        startBarrier.await();

                        long startNanos0 = startNanos.get();

                        // Warm up connections without recording results.
                        while (!finished.get() && elapsedNanos(startNanos0) < warmupNanos) {
                            ClusterNode node = testNodes.get((int)(targetIdx++ % testNodes.size()));

                            measureTarget(results.get(node.id()), node, payload, processInNioThread, resTimeout, false);
                        }

                        // Record one sample for every target even if the configured duration has already expired.
                        for (int idx; !finished.get() &&
                            (idx = firstSampleIdx.getAndIncrement()) < testNodes.size(); ) {
                            ClusterNode node = testNodes.get(idx);

                            measureTarget(results.get(node.id()), node, payload, processInNioThread, resTimeout, true);
                        }

                        // Continue collecting samples while the configured duration remains.
                        while (!finished.get() && elapsedNanos(startNanos0) < totalNanos) {
                            ClusterNode node = testNodes.get((int)(targetIdx++ % testNodes.size()));

                            measureTarget(results.get(node.id()), node, payload, processInNioThread, resTimeout, true);
                        }
                    }
                    catch (Exception | Error e) {
                        testRes.onDone(e);

                        if (e instanceof Error)
                            throw (Error)e;
                    }
                    finally {
                        if (remaining.decrementAndGet() == 0 && !finished.get()) {
                            try {
                                testRes.onDone(createResult(src, results, payloadSize, warmup, duration, threads,
                                    processInNioThread));
                            }
                            catch (RuntimeException | Error e) {
                                testRes.onDone(e);

                                if (e instanceof Error)
                                    throw (Error)e;
                            }
                        }
                    }
                });
            }
        }
        catch (RuntimeException | Error e) {
            testRes.onDone(e);

            if (e instanceof Error)
                throw (Error)e;
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

    /** Sends one request and optionally records the sample. */
    private void measureTarget(
        IoTestNodeResults nodeResults,
        ClusterNode node,
        byte[] payload,
        boolean processInNioThread,
        long resTimeout,
        boolean record
    ) throws IgniteCheckedException {
        IgniteIoTestMessage res = sendAndMeasure(node, payload, processInNioThread, resTimeout);

        if (record)
            nodeResults.onResult(res);
    }

    /** Sends a request and measures its round-trip time on the local node. */
    private IgniteIoTestMessage sendAndMeasure(
        ClusterNode node,
        byte[] payload,
        boolean processInNioThread,
        long resTimeout
    ) throws IgniteCheckedException {
        IgniteInternalFuture<IgniteIoTestMessage> fut = sendIoTest(node, payload, processInNioThread);

        try {
            return fut.get(resTimeout);
        }
        catch (IgniteCheckedException e) {
            throw new IgniteCheckedException("Communication SPI test request failed [nodeId=" + node.id() + ']', e);
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

    /** Creates a test result. */
    private static IoTestResult createResult(
        ClusterNode src,
        Map<UUID, IoTestNodeResults> rawResults,
        int payloadSize,
        long warmup,
        long duration,
        int threads,
        boolean processInNioThread
    ) {
        List<IoTestResult.TargetResult> targets = new ArrayList<>(rawResults.size());

        for (IoTestNodeResults nodeResults : rawResults.values())
            targets.add(nodeResults.snapshot());

        return new IoTestResult(
            src.id(),
            consistentId(src),
            warmup,
            duration,
            threads,
            payloadSize,
            processInNioThread,
            targets
        );
    }

    /** Returns a serializable display form of a node consistent ID. */
    @Nullable private static String consistentId(ClusterNode node) {
        Object consistentId = node.consistentId();

        return consistentId == null ? null : consistentId.toString();
    }

    /** Creates a node-stopping error. */
    private NodeStoppingException stoppingException() {
        return new NodeStoppingException("IO test has been cancelled because the local node is stopping: " +
            ctx.localNodeId());
    }

    /** Aggregate future that owns and cancels every pending per-node request. */
    private class IoTestCompoundFuture extends GridFutureAdapter<Void> {
        /** Pending child futures. Guarded by {@link #mux}. */
        private final List<IgniteInternalFuture<IgniteIoTestMessage>> pending = new ArrayList<>();

        /** State mutex. */
        private final Object mux = new Object();

        /** Initialization flag. Guarded by {@link #mux}. */
        private boolean initialized;

        /** Terminal-state flag. Guarded by {@link #mux}. */
        private boolean finished;

        /**
         * Atomically checks aggregate state, sends a request, and registers its future as an owned child.
         *
         * @return {@code False} if a previous child has already failed.
         */
        boolean add(ClusterNode node, byte[] payload, boolean processInNioThread) {
            IgniteInternalFuture<IgniteIoTestMessage> fut;

            synchronized (mux) {
                if (finished)
                    return false;

                fut = sendIoTest(node, payload, processInNioThread);

                pending.add(fut);
            }

            fut.listen(this::onChildDone);

            return true;
        }

        /** Marks that all target nodes have been added. */
        void markInitialized() {
            boolean complete;

            synchronized (mux) {
                initialized = true;
                complete = !finished && pending.isEmpty();

                if (complete)
                    finished = true;
            }

            if (complete)
                onDone();
        }

        /** Completes this aggregate on child completion. */
        private void onChildDone(IgniteInternalFuture<IgniteIoTestMessage> fut) {
            Throwable err = null;

            try {
                fut.get();
            }
            catch (IgniteCheckedException | RuntimeException | AssertionError e) {
                err = e;
            }

            List<IgniteInternalFuture<IgniteIoTestMessage>> toCancel = null;
            boolean complete = false;

            synchronized (mux) {
                pending.remove(fut);

                if (finished)
                    return;

                if (err != null) {
                    finished = true;
                    toCancel = new ArrayList<>(pending);
                    pending.clear();
                }
                else if (initialized && pending.isEmpty()) {
                    finished = true;
                    complete = true;
                }
            }

            if (err != null) {
                cancelAll(toCancel, err);
                onDone(err);
            }
            else if (complete)
                onDone();
        }

        /** Completes this aggregate after an exception in the add loop. */
        void onFailure(Throwable err) {
            List<IgniteInternalFuture<IgniteIoTestMessage>> toCancel;

            synchronized (mux) {
                if (finished)
                    return;

                finished = true;
                toCancel = new ArrayList<>(pending);
                pending.clear();
            }

            cancelAll(toCancel, err);
            onDone(err);
        }

        /** {@inheritDoc} */
        @Override public boolean cancel() {
            List<IgniteInternalFuture<IgniteIoTestMessage>> toCancel;

            synchronized (mux) {
                if (finished)
                    return false;

                finished = true;
                toCancel = new ArrayList<>(pending);
                pending.clear();
            }

            cancelAll(toCancel, null);

            return onCancelled();
        }

        /** Cancels child futures, attaching cancellation errors to the aggregate failure when available. */
        private void cancelAll(
            List<IgniteInternalFuture<IgniteIoTestMessage>> futs,
            @Nullable Throwable err
        ) {
            for (IgniteInternalFuture<IgniteIoTestMessage> fut : futs) {
                if (fut.isDone())
                    continue;

                try {
                    fut.cancel();
                }
                catch (IgniteCheckedException e) {
                    if (err != null)
                        err.addSuppressed(e);
                    else
                        LT.warn(log, "Failed to cancel IO test request.", e);
                }
            }
        }
    }

    /** Pending request future. */
    private class IoTestFuture extends GridFutureAdapter<IgniteIoTestMessage> {
        /** Test ID. */
        private final long id;

        /** Request start timestamp from the source node monotonic clock. */
        private final long startNanos = System.nanoTime();

        /** Constructor. */
        IoTestFuture(long id) {
            this.id = id;
        }

        /** Completes this future with a response and records its end-to-end RTT. */
        void onResponse(IgniteIoTestMessage res) {
            res.roundTripNanos(System.nanoTime() - startNanos);

            onDone(res);
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

    /** Running structured test future. */
    private class IoTestRunFuture extends GridFutureAdapter<IoTestResult> {
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
        @Override protected boolean onDone(@Nullable IoTestResult res, @Nullable Throwable err, boolean cancel) {
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
        /** Target node ID. */
        private final UUID nodeId;

        /** Target node consistent ID. */
        private final String consistentId;

        /** Total RTT. */
        private long totalRttNanos;

        /** Minimum RTT. */
        private long minRttNanos = Long.MAX_VALUE;

        /** Maximum RTT. */
        private long maxRttNanos;

        /** Sample count. */
        private long count;

        /** Estimated request delivery statistics. */
        private final LatencyStats reqDelivery = new LatencyStats();

        /** Estimated response delivery statistics. */
        private final LatencyStats resDelivery = new LatencyStats();

        /** Constructor. */
        IoTestNodeResults(ClusterNode node) {
            nodeId = node.id();
            consistentId = IoTestHandler.consistentId(node);
        }

        /** Adds a sample. */
        synchronized void onResult(IgniteIoTestMessage msg) {
            long rttNanos = msg.roundTripNanos();

            totalRttNanos += rttNanos;
            minRttNanos = Math.min(minRttNanos, rttNanos);
            maxRttNanos = Math.max(maxRttNanos, rttNanos);
            count++;

            reqDelivery.add(msg.requestDeliveryTimeMillis());
            resDelivery.add(msg.responseDeliveryTimeMillis());
        }

        /** Creates an immutable snapshot. */
        synchronized IoTestResult.TargetResult snapshot() {
            if (count == 0)
                throw new IllegalStateException("Communication SPI test did not sample target: " + nodeId);

            return new IoTestResult.TargetResult(
                nodeId,
                consistentId,
                count,
                minRttNanos,
                totalRttNanos / (double)count,
                maxRttNanos,
                reqDelivery.snapshot(),
                resDelivery.snapshot()
            );
        }
    }

    /** Min/average/max accumulator. */
    private static class LatencyStats {
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

        /** Creates an immutable snapshot. */
        IoTestResult.LatencySummary snapshot() {
            return new IoTestResult.LatencySummary(min, average(), max);
        }
    }
}
