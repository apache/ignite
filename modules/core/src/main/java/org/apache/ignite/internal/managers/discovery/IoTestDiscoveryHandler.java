/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.managers.discovery;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.LongSummaryStatistics;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteFutureTimeoutCheckedException;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;

/** Runs a bounded latency test through the discovery ring. */
public class IoTestDiscoveryHandler {
    /** Cancellation polling interval. */
    private static final long CANCEL_POLL_INTERVAL_MILLIS = 100;

    /** */
    private final GridKernalContext ctx;

    /** */
    private final IgniteLogger log;

    /** Pending sample. */
    private volatile IoTestDiscoveryFuture pendingTest;

    /** Ensures that only one test runs on the coordinator. */
    private final AtomicBoolean testRunning = new AtomicBoolean();

    /** @param ctx Kernal context. */
    public IoTestDiscoveryHandler(GridKernalContext ctx) {
        this.ctx = ctx;
        log = ctx.log(getClass());

        ctx.discovery().setCustomEventListener(IoTestDiscoveryMessage.class, (topVer, snd, msg) ->
            msg.onProcessed(ctx.localNodeId()));

        ctx.discovery().setCustomEventListener(IoTestDiscoveryAckMessage.class, (topVer, snd, msg) -> {
            if (!U.isLocalNodeCoordinator(ctx.discovery()))
                return;

            IoTestDiscoveryFuture fut = pendingTest;

            if (fut != null && fut.requestId.equals(msg.requestId()))
                fut.onAck(msg);
            else if (log.isDebugEnabled())
                log.debug("Ignoring unknown discovery IO test acknowledgement: " + msg.requestId());
        });
    }

    /**
     * @param samples Number of samples.
     * @param intervalMillis Interval between samples.
     * @param payloadSize Payload size.
     * @param cancelled Cancellation flag.
     * @return Test report.
     */
    public String runTest(int samples, long intervalMillis, int payloadSize, BooleanSupplier cancelled) {
        A.ensure(ctx.discovery().getInjectedDiscoverySpi() instanceof TcpDiscoverySpi,
            "Discovery IO test requires TcpDiscoverySpi.");
        A.ensure(U.isLocalNodeCoordinator(ctx.discovery()), "Should be executed on the coordinator node.");
        A.ensure(ctx.discovery().aliveServerNodes().size() > 1,
            "Discovery IO test requires at least two server nodes.");
        A.notNull(cancelled, "cancelled");
        A.ensure(testRunning.compareAndSet(false, true), "Discovery IO test is already running.");

        try {
            byte[] payload = new byte[payloadSize];
            long timeout = ctx.config().getNetworkTimeout();
            List<Long> ringTimes = new ArrayList<>(samples);
            List<LongSummaryStatistics> hopTimes = new ArrayList<>();
            List<UUID> path = null;

            for (int i = 0; i < samples; i++) {
                ensureNotCancelled(cancelled);

                IoTestDiscoveryFuture fut = send(payload);
                IoTestDiscoveryAckMessage ack;

                try {
                    ack = await(fut, timeout, cancelled);
                }
                catch (IgniteCheckedException e) {
                    throw new IgniteException("Discovery IO test sample timed out or failed.", e);
                }
                finally {
                    if (pendingTest == fut)
                        pendingTest = null;
                }

                if (path == null)
                    path = new ArrayList<>(ack.path);
                else if (!path.equals(ack.path))
                    throw new IgniteException("Discovery ring path changed during the test.");

                if (ack.hopTimesMillis.size() != path.size())
                    throw new IgniteException("Incomplete discovery ring timing data.");

                while (hopTimes.size() < path.size())
                    hopTimes.add(new LongSummaryStatistics());

                for (int hop = 0; hop < path.size(); hop++)
                    hopTimes.get(hop).accept(ack.hopTimesMillis.get(hop));

                ringTimes.add(ack.ringTimeNanos);

                if (i + 1 < samples)
                    sleep(intervalMillis, cancelled);
            }

            return formatSummary(payloadSize, intervalMillis, ringTimes, path, hopTimes);
        }
        finally {
            testRunning.set(false);
        }
    }

    /** Sends one test message. */
    private IoTestDiscoveryFuture send(byte[] payload) {
        IoTestDiscoveryMessage msg = new IoTestDiscoveryMessage(payload);
        IoTestDiscoveryFuture fut = new IoTestDiscoveryFuture(msg.id());

        pendingTest = fut;

        try {
            ctx.discovery().sendCustomEvent(msg);
        }
        catch (IgniteCheckedException | RuntimeException e) {
            fut.onDone(e);
        }

        return fut;
    }

    /** Waits for one sample while observing job cancellation. */
    private static IoTestDiscoveryAckMessage await(
        IoTestDiscoveryFuture fut,
        long timeout,
        BooleanSupplier cancelled
    ) throws IgniteCheckedException {
        long startNanos = System.nanoTime();
        long remaining = Math.max(1, timeout);

        while (true) {
            ensureNotCancelled(cancelled);

            try {
                IoTestDiscoveryAckMessage res = fut.get(Math.min(remaining, CANCEL_POLL_INTERVAL_MILLIS));

                ensureNotCancelled(cancelled);

                return res;
            }
            catch (IgniteFutureTimeoutCheckedException e) {
                ensureNotCancelled(cancelled);

                remaining = timeout - TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);

                if (remaining <= 0)
                    throw e;
            }
        }
    }

    /** Sleeps between samples while observing job cancellation. */
    private static void sleep(long millis, BooleanSupplier cancelled) {
        for (long remaining = millis; remaining > 0; ) {
            ensureNotCancelled(cancelled);

            long delay = Math.min(remaining, CANCEL_POLL_INTERVAL_MILLIS);

            try {
                U.sleep(delay);
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException("Discovery IO test was interrupted.", e);
            }

            remaining -= delay;
        }
    }

    /** Fails the test if its management job was cancelled. */
    private static void ensureNotCancelled(BooleanSupplier cancelled) {
        if (cancelled.getAsBoolean())
            throw new IgniteException("Discovery IO test was cancelled.");
    }

    /** Formats a compact report. */
    private String formatSummary(
        int payloadSize,
        long intervalMillis,
        List<Long> ringTimes,
        List<UUID> path,
        List<LongSummaryStatistics> hopTimes
    ) {
        ringTimes.sort(Long::compare);

        StringBuilder sb = new StringBuilder();

        sb.append("TcpDiscoverySpi ring test\n");
        sb.append("Coordinator: ").append(ctx.localNodeId()).append('\n');
        sb.append("Samples: ").append(ringTimes.size()).append(" | Inter-sample delay: ").append(intervalMillis)
            .append(" ms\n");
        sb.append("Request application payload: ").append(payloadSize).append(" bytes\n");
        sb.append("Server ring path: ");

        for (UUID nodeId : path)
            sb.append(nodeId).append(" -> ");

        sb.append(ctx.localNodeId()).append('\n');
        sb.append(String.format(Locale.ROOT,
            "Request ring latency (submission -> local ACK, ms): min=%.3f, p50=%.3f, p95=%.3f, max=%.3f%n",
            toMillis(ringTimes.get(0)),
            toMillis(percentile(ringTimes, 50)),
            toMillis(percentile(ringTimes, 95)),
            toMillis(ringTimes.get(ringTimes.size() - 1))));
        sb.append("Estimated per-hop one-way message delay (ms, min/avg/max):\n");
        sb.append("  Clock assumption: synchronized wall clocks; negative values indicate clock offset or ")
            .append("wall-clock adjustment.\n");

        for (int hop = 0; hop < path.size(); hop++) {
            LongSummaryStatistics timing = hopTimes.get(hop);

            sb.append(String.format(Locale.ROOT, "  %s -> %s: min=%d, avg=%.3f, max=%d%n",
                path.get(hop), path.get((hop + 1) % path.size()),
                timing.getMin(), timing.getAverage(), timing.getMax()));
        }

        return sb.toString();
    }

    /** Returns the nearest-rank percentile. */
    private static long percentile(List<Long> sorted, int percentile) {
        int idx = (int)Math.ceil(sorted.size() * percentile / 100.0) - 1;

        return sorted.get(idx);
    }

    /** Converts nanoseconds to milliseconds. */
    private static double toMillis(long nanos) {
        return nanos / 1_000_000.0;
    }

    /** Pending discovery test. */
    private static class IoTestDiscoveryFuture extends GridFutureAdapter<IoTestDiscoveryAckMessage> {
        /** Request ID. */
        private final IgniteUuid requestId;

        /** Local start timestamp. */
        private final long startNanos = System.nanoTime();

        /** @param requestId Request ID. */
        IoTestDiscoveryFuture(IgniteUuid requestId) {
            this.requestId = requestId;
        }

        /** Completes this sample from the discovery listener. */
        void onAck(IoTestDiscoveryAckMessage ack) {
            ack.ringTimeNanos = System.nanoTime() - startNanos;

            onDone(ack);
        }
    }
}
