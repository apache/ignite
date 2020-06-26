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

package org.apache.ignite.internal.processors.performancestatistics;

import java.io.Serializable;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteFeatures;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryType;
import org.apache.ignite.internal.util.GridIntList;
import org.apache.ignite.internal.util.distributed.DistributedProcess;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteFutureCancelledException;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.spi.discovery.DiscoveryDataBag;

import static org.apache.ignite.internal.GridComponent.DiscoveryDataExchangeType.PERFORMANCE_STAT_PROC;
import static org.apache.ignite.internal.IgniteFeatures.allNodesSupports;
import static org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType.PERFORMANCE_STATISTICS;

/**
 * Performance statistics processor.
 * <p>
 * Manages collecting statistics.
 */
public class PerformaceStatisticsProcessor extends GridProcessorAdapter implements IgnitePerformanceStatistics {
    /** Process to start/stop statistics. */
    private final DistributedProcess<Boolean, Boolean> proc;

    /** Performance statistics writer. */
    private final FilePerformanceStatistics writer;

    /** Synchronization mutex for request futures. */
    private final Object mux = new Object();

    /** Enable/disable statistics request futures. */
    private final ConcurrentMap<UUID, GridFutureAdapter<Void>> reqFuts = new ConcurrentHashMap<>();

    /** Disconnected flag. */
    private volatile boolean disconnected;

    /** Stopped flag. */
    private volatile boolean stopped;

    /** @param ctx Kernal context. */
    public PerformaceStatisticsProcessor(GridKernalContext ctx) {
        super(ctx);

        writer = new FilePerformanceStatistics(ctx);

        proc = new DistributedProcess<>(ctx, PERFORMANCE_STATISTICS, start -> {
            if (start) {
                return ctx.closure().callLocalSafe(() -> {
                    if (start)
                        writer.start();

                    return true;
                });
            }

            return writer.stop().chain(f -> true);
        }, (uuid, res, err) -> {
            if (!F.isEmpty(err) && enabled())
                writer.stop();

            synchronized (mux) {
                GridFutureAdapter<Void> fut = reqFuts.get(uuid);

                if (fut != null) {
                    if (!F.isEmpty(err))
                        fut.onDone(new IgniteException("Unable to process request [err=" + err + ']'));
                    else
                        fut.onDone();
                }
            }
        });
    }

    /** @return {@code True} if collecting performance statistics is enabled. */
    public boolean enabled() {
        return writer.performanceStatisticsEnabled();
    }

    /**
     * Starts collecting performance statistics.
     *
     * @return Future to be completed on collecting started.
     */
    public IgniteInternalFuture<Void> startCollectStatistics() {
        if (!allNodesSupports(ctx.discovery().allNodes(), IgniteFeatures.PERFORMANCE_STATISTICS)) {
            return new GridFinishedFuture<>(
                new IllegalStateException("Not all nodes in the cluster support collecting performance statistics."));
        }

        GridFutureAdapter<Void> fut = new GridFutureAdapter<>();

        UUID uuid = UUID.randomUUID();

        synchronized (mux) {
            if (disconnected || stopped) {
                return new GridFinishedFuture<>(
                    new IgniteFutureCancelledException("Node " + (stopped ? "stopped" : "disconnected")));
            }

            reqFuts.put(uuid, fut);
        }

        proc.start(uuid, true);

        return fut;
    }

    /**
     * Stops collecting performance statistics.
     *
     * @return Future to be completed on collecting stopped.
     */
    public IgniteInternalFuture<Void> stopCollectStatistics() {
        GridFutureAdapter<Void> fut = new GridFutureAdapter<>();

        UUID uuid = UUID.randomUUID();

        synchronized (mux) {
            if (disconnected || stopped) {
                return new GridFinishedFuture<>(
                    new IgniteFutureCancelledException("Node " + (stopped ? "stopped" : "disconnected")));
            }

            reqFuts.put(uuid, fut);
        }

        proc.start(uuid, false);

        return fut;
    }

    /** {@inheritDoc} */
    @Override public void cacheOperation(CacheOperationType type, int cacheId, long startTime, long duration) {
        writer.cacheOperation(type, cacheId, startTime, duration);
    }

    /** {@inheritDoc} */
    @Override public void transaction(GridIntList cacheIds, long startTime, long duration, boolean commit) {
        writer.transaction(cacheIds, startTime, duration, commit);
    }

    /** {@inheritDoc} */
    @Override public void query(GridCacheQueryType type, String text, long id, long startTime, long duration,
        boolean success) {
        writer.query(type, text, id, startTime, duration, success);
    }

    /** {@inheritDoc} */
    @Override public void queryReads(GridCacheQueryType type, UUID queryNodeId, long id, long logicalReads,
        long physicalReads) {
        writer.queryReads(type, queryNodeId, id, logicalReads, physicalReads);
    }

    /** {@inheritDoc} */
    @Override public void task(IgniteUuid sesId, String taskName, long startTime, long duration, int affPartId) {
        writer.task(sesId, taskName, startTime, duration, affPartId);
    }

    /** {@inheritDoc} */
    @Override public void job(IgniteUuid sesId, long queuedTime, long startTime, long duration, boolean timedOut) {
        writer.job(sesId, queuedTime, startTime, duration, timedOut);
    }

    /** {@inheritDoc} */
    @Override public void collectGridNodeData(DiscoveryDataBag dataBag) {
        if (!enabled() || dataBag.commonDataCollectedFor(PERFORMANCE_STAT_PROC.ordinal()))
            return;

        dataBag.addNodeSpecificData(PERFORMANCE_STAT_PROC.ordinal(), new DiscoveryData(enabled()));
    }

    /** {@inheritDoc} */
    @Override public void onGridDataReceived(DiscoveryDataBag.GridDiscoveryData data) {
        DiscoveryData discoData = (DiscoveryData)data.commonData();

        if (discoData.statEnabled)
            startCollectStatistics();
    }

    /** {@inheritDoc} */
    @Override public void onKernalStop(boolean cancel) {
        if (enabled())
            writer.stop();

        synchronized (mux) {
            stopped = true;

            cancelFutures("Kernal stopped.");
        }
    }

    /** {@inheritDoc} */
    @Override public void onDisconnected(IgniteFuture<?> reconnectFut) {
        synchronized (mux) {
            assert !disconnected;

            disconnected = true;

            cancelFutures("Client node was disconnected from topology (operation result is unknown).");
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> onReconnected(boolean clusterRestarted) {
        synchronized (mux) {
            assert disconnected;

            disconnected = false;

            return null;
        }
    }

    /** @param msg Error message. */
    private void cancelFutures(String msg) {
        synchronized (mux) {
            reqFuts.forEach((uuid, fut) -> fut.onDone(new IgniteFutureCancelledException(msg)));
        }
    }

    /** */
    private static class DiscoveryData implements Serializable {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        final boolean statEnabled;

        /** */
        DiscoveryData(boolean statEnabled) {
            this.statEnabled = statEnabled;
        }
    }
}
