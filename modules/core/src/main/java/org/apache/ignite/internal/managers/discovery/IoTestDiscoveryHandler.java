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
import java.util.Collection;
import java.util.List;
import java.util.LongSummaryStatistics;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;

/** Runs a latency test through the discovery ring. */
public class IoTestDiscoveryHandler {
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
     * @return Structured test result.
     */
    public IoTestDiscoveryResult runTest(
        int samples,
        long intervalMillis,
        int payloadSize,
        BooleanSupplier cancelled
    ) {
        A.ensure(ctx.discovery().getInjectedDiscoverySpi() instanceof TcpDiscoverySpi,
            "Discovery IO test requires TcpDiscoverySpi.");
        A.ensure(U.isLocalNodeCoordinator(ctx.discovery()), "Should be executed on the coordinator node.");
        A.ensure(samples > 0, "samples must be positive");

        Collection<ClusterNode> serverNodes = ctx.discovery().aliveServerNodes();

        A.ensure(serverNodes.size() > 1, "Discovery IO test requires at least two server nodes.");
        A.notNull(cancelled, "cancelled");

        if (!testRunning.compareAndSet(false, true))
            throw new IgniteException("Discovery IO test is already running.");

        try {
            byte[] payload = new byte[payloadSize];
            ClusterNode coordinator = ctx.discovery().localNode();
            Map<UUID, String> servers = serverSnapshot(serverNodes);
            LongSummaryStatistics ringTimes = new LongSummaryStatistics();
            List<LongSummaryStatistics> hopTimes = new ArrayList<>();
            List<UUID> path = new ArrayList<>();

            for (int i = 0; i < samples; i++) {
                ensureNotCancelled(cancelled);

                IoTestDiscoveryAckMessage ack = runSample(payload);

                ensureNotCancelled(cancelled);

                List<UUID> samplePath = validatedPath(ack, servers.keySet());

                if (path.isEmpty())
                    path = samplePath;
                else if (!path.equals(samplePath))
                    throw new IgniteCheckedException("Discovery ring path changed during the test.");

                while (hopTimes.size() < path.size())
                    hopTimes.add(new LongSummaryStatistics());

                for (int hop = 0; hop < path.size(); hop++)
                    hopTimes.get(hop).accept(ack.hopTimesMillis.get(hop));

                ringTimes.accept(ack.ringTimeNanos);

                if (i + 1 < samples)
                    U.sleep(intervalMillis);
            }

            return new IoTestDiscoveryResult(
                coordinator.id(),
                servers,
                ringLatency(ringTimes),
                hopLatencies(path, hopTimes)
            );
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e.getMessage(), e);
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

    /** Sends and waits for one sample. */
    private IoTestDiscoveryAckMessage runSample(byte[] payload) throws IgniteCheckedException {
        IoTestDiscoveryFuture fut = send(payload);

        try {
            return fut.get();
        }
        finally {
            if (pendingTest == fut)
                pendingTest = null;
        }
    }

    /** Fails the test if its management job was cancelled. */
    private static void ensureNotCancelled(BooleanSupplier cancelled) {
        if (cancelled.getAsBoolean())
            throw new IgniteException("Discovery IO test was cancelled.");
    }

    /** Validates and copies the discovery path returned by one sample. */
    private static List<UUID> validatedPath(IoTestDiscoveryAckMessage ack, Set<UUID> serverIds)
        throws IgniteCheckedException {
        if (ack.path == null || ack.hopTimesMillis == null)
            throw new IgniteCheckedException("Discovery ring acknowledgement contains no timing data.");

        List<UUID> path = new ArrayList<>(ack.path);

        if (path.size() != serverIds.size() || !new TreeSet<>(path).equals(serverIds))
            throw new IgniteCheckedException("Discovery ring path does not match the initial server topology.");

        if (ack.hopTimesMillis.size() != path.size())
            throw new IgniteCheckedException("Incomplete discovery ring timing data.");

        return path;
    }

    /** Creates a ring latency aggregate from completed samples. */
    private static IoTestDiscoveryResult.RingLatencySummary ringLatency(LongSummaryStatistics ringTimes) {
        return new IoTestDiscoveryResult.RingLatencySummary(
            (int)ringTimes.getCount(),
            toMillis(ringTimes.getMin()),
            toMillis(ringTimes.getAverage()),
            toMillis(ringTimes.getMax())
        );
    }

    /** Creates per-hop latency aggregates in ring order. */
    private static List<IoTestDiscoveryResult.HopLatencySummary> hopLatencies(
        List<UUID> path,
        List<LongSummaryStatistics> hopTimes
    ) {
        List<IoTestDiscoveryResult.HopLatencySummary> res = new ArrayList<>(path.size());

        for (int hop = 0; hop < path.size(); hop++) {
            LongSummaryStatistics timing = hopTimes.get(hop);

            res.add(new IoTestDiscoveryResult.HopLatencySummary(
                path.get(hop),
                path.get((hop + 1) % path.size()),
                timing.getMin(),
                timing.getAverage(),
                timing.getMax()
            ));
        }

        return res;
    }

    /** Creates a deterministic snapshot of server node IDs and consistent IDs. */
    private static Map<UUID, String> serverSnapshot(Collection<ClusterNode> nodes) {
        Map<UUID, String> nodeConsistentIds = new TreeMap<>();

        for (ClusterNode node : nodes) {
            Object consistentId = node.consistentId();

            nodeConsistentIds.put(node.id(), consistentId == null ? null : consistentId.toString());
        }

        return nodeConsistentIds;
    }

    /** Converts nanoseconds to milliseconds. */
    private static double toMillis(double nanos) {
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
