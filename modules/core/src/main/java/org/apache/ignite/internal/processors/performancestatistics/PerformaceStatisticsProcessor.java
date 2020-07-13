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

import java.util.UUID;
import java.util.function.Consumer;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteFeatures;
import org.apache.ignite.internal.NodeStoppingException;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryType;
import org.apache.ignite.internal.processors.metastorage.DistributedMetaStorage;
import org.apache.ignite.internal.processors.metastorage.DistributedMetastorageLifecycleListener;
import org.apache.ignite.internal.processors.metastorage.ReadableDistributedMetaStorage;
import org.apache.ignite.internal.util.GridIntList;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.IgniteFeatures.allNodesSupports;

/**
 * Performance statistics processor.
 * <p>
 * Manages collecting performance statistics.
 *
 * @see FilePerformanceStatisticsWriter
 * @see FilePerformanceStatisticsReader
 */
public class PerformaceStatisticsProcessor extends GridProcessorAdapter {
    /** Prefix for performance statistics enabled property name. */
    private static final String PERFORMANCE_STAT_ENABLED_PREFIX = "performanceStatistics.enabled";

    /** Performance statistics writer. */
    @Nullable private volatile FilePerformanceStatisticsWriter writer;

    /** Performance statistics enabled flag. */
    private volatile boolean enabled;

    /** Metastorage with the write access. */
    @Nullable private volatile DistributedMetaStorage metastorage;

    /** Synchronization mutex for start/stop collecting performance statistics operations. */
    private final Object mux = new Object();

    /** @param ctx Kernal context. */
    public PerformaceStatisticsProcessor(GridKernalContext ctx) {
        super(ctx);

        ctx.internalSubscriptionProcessor().registerDistributedMetastorageListener(
            new DistributedMetastorageLifecycleListener() {
            @Override public void onReadyForRead(ReadableDistributedMetaStorage metastorage) {
                metastorage.listen(PERFORMANCE_STAT_ENABLED_PREFIX::equals, (key, oldVal, newVal) -> {
                    // Skip history on local join.
                    if (!ctx.discovery().localJoinFuture().isDone())
                        return;

                    onMetastorageUpdate((boolean)newVal);
                });
            }

            @Override public void onReadyForWrite(DistributedMetaStorage metastorage) {
                PerformaceStatisticsProcessor.this.metastorage = metastorage;

                try {
                    Boolean performanceStatsEnabled = metastorage.read(PERFORMANCE_STAT_ENABLED_PREFIX);

                    if (performanceStatsEnabled == null)
                        return;

                    onMetastorageUpdate(performanceStatsEnabled);
                }
                catch (IgniteCheckedException e) {
                    throw new IgniteException(e);
                }
            }
        });
    }

    /**
     * @param type Operation type.
     * @param cacheId Cache id.
     * @param startTime Start time in milliseconds.
     * @param duration Duration in nanoseconds.
     */
    public void cacheOperation(OperationType type, int cacheId, long startTime, long duration) {
        write(writer -> writer.cacheOperation(type, cacheId, startTime, duration));
    }

    /**
     * @param cacheIds Cache IDs.
     * @param startTime Start time in milliseconds.
     * @param duration Duration in nanoseconds.
     * @param commited {@code True} if commited.
     */
    public void transaction(GridIntList cacheIds, long startTime, long duration, boolean commited) {
        write(writer -> writer.transaction(cacheIds, startTime, duration, commited));
    }

    /**
     * @param type Cache query type.
     * @param text Query text in case of SQL query. Cache name in case of SCAN query.
     * @param id Query id.
     * @param startTime Start time in milliseconds.
     * @param duration Duration in nanoseconds.
     * @param success Success flag.
     */
    public void query(GridCacheQueryType type, String text, long id, long startTime, long duration, boolean success) {
        write(writer -> writer.query(type, text, id, startTime, duration, success));
    }

    /**
     * @param type Cache query type.
     * @param queryNodeId Originating node id.
     * @param id Query id.
     * @param logicalReads Number of logical reads.
     * @param physicalReads Number of physical reads.
     */
    public void queryReads(GridCacheQueryType type, UUID queryNodeId, long id, long logicalReads, long physicalReads) {
        write(writer -> writer.queryReads(type, queryNodeId, id, logicalReads, physicalReads));
    }

    /**
     * @param sesId Session id.
     * @param taskName Task name.
     * @param startTime Start time in milliseconds.
     * @param duration Duration.
     * @param affPartId Affinity partition id.
     */
    public void task(IgniteUuid sesId, String taskName, long startTime, long duration, int affPartId) {
        write(writer -> writer.task(sesId, taskName, startTime, duration, affPartId));
    }

    /**
     * @param sesId Session id.
     * @param queuedTime Time job spent on waiting queue.
     * @param startTime Start time in milliseconds.
     * @param duration Job execution time.
     * @param timedOut {@code True} if job is timed out.
     */
    public void job(IgniteUuid sesId, long queuedTime, long startTime, long duration, boolean timedOut) {
        write(writer -> writer.job(sesId, queuedTime, startTime, duration, timedOut));
    }

    /**
     * Starts collecting performance statistics.
     *
     * @throws IgniteCheckedException If starting failed.
     */
    public void startCollectStatistics() throws IgniteCheckedException {
        A.notNull(metastorage, "Metastorage not ready. Node not started?");

        if (!allNodesSupports(ctx.discovery().allNodes(), IgniteFeatures.PERFORMANCE_STATISTICS))
            throw new IllegalStateException("Not all nodes in the cluster support collecting performance statistics.");

        if (ctx.isStopping())
            throw new NodeStoppingException("Operation has been cancelled (node is stopping)");

        metastorage.write(PERFORMANCE_STAT_ENABLED_PREFIX, true);
    }

    /**
     * Stops collecting performance statistics.
     *
     * @throws IgniteCheckedException If stopping failed.
     */
    public void stopCollectStatistics() throws IgniteCheckedException {
        A.notNull(metastorage, "Metastorage not ready. Node not started?");

        if (ctx.isStopping())
            throw new NodeStoppingException("Operation has been cancelled (node is stopping)");

        metastorage.write(PERFORMANCE_STAT_ENABLED_PREFIX, false);
    }

    /** @return {@code True} if collecting performance statistics is enabled. */
    public boolean enabled() {
        return enabled;
    }

    /** {@inheritDoc} */
    @Override public void onKernalStop(boolean cancel) {
        if (enabled())
            stopWriter();
    }

    /** {@inheritDoc} */
    @Override public void onDisconnected(IgniteFuture<?> reconnectFut) {
        if (enabled())
            stopWriter();
    }

    /** Starts or stops collecting statistics on metastorage update. */
    private void onMetastorageUpdate(boolean start) {
        ctx.closure().runLocalSafe(() -> {
            if (start)
                startWriter();
            else
                stopWriter();
        });
    }

    /** Starts performance statistics writer. */
    private void startWriter() {
        try {
            boolean started;

            synchronized (mux) {
                if (enabled)
                    return;

                writer = new FilePerformanceStatisticsWriter(ctx);

                writer.start();

                enabled = true;

                started = true;
            }

            if (started)
                log.info("Performance statistics writer started.");
        }
        catch (Exception e) {
            log.error("Failed to start performance statistics writer.", e);
        }
    }

    /** Stops performance statistics writer. */
    private void stopWriter() {
        boolean stopped;

        synchronized (mux) {
            if (!enabled)
                return;

            enabled = false;

            writer.stop();

            writer = null;

            stopped = true;
        }

        if (stopped)
            log.info("Performance statistics writer stopped.");
    }

    /** Writes statistics through passed writer. */
    private void write(Consumer<FilePerformanceStatisticsWriter> c) {
        FilePerformanceStatisticsWriter writer = this.writer;

        if (writer != null)
            c.accept(writer);
    }
}
