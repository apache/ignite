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
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteFeatures;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryType;
import org.apache.ignite.internal.processors.metastorage.DistributedMetaStorage;
import org.apache.ignite.internal.processors.metastorage.DistributedMetastorageLifecycleListener;
import org.apache.ignite.internal.processors.metastorage.ReadableDistributedMetaStorage;
import org.apache.ignite.internal.util.GridIntList;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteUuid;

import static org.apache.ignite.internal.IgniteFeatures.allNodesSupports;

/**
 * Performance statistics processor.
 * <p>
 * Manages collecting statistics.
 */
public class PerformaceStatisticsProcessor extends GridProcessorAdapter {
    /** Prefix for performance statistics enabled property name. */
    private static final String STAT_ENABLED_PREFIX = "performanceStatistics.enabled";

    /** Performance statistics writer. */
    private final FilePerformanceStatisticsWriter writer;

    /** Metastorage with the write access. */
    private volatile DistributedMetaStorage metastorage;

    /** @param ctx Kernal context. */
    public PerformaceStatisticsProcessor(GridKernalContext ctx) {
        super(ctx);

        writer = new FilePerformanceStatisticsWriter(ctx);

        ctx.internalSubscriptionProcessor().registerDistributedMetastorageListener(
            new DistributedMetastorageLifecycleListener() {
            @Override public void onReadyForRead(ReadableDistributedMetaStorage metastorage) {
                metastorage.listen(STAT_ENABLED_PREFIX::equals, (key, oldVal, newVal) -> {
                    // Skip history on local join.
                    if (!ctx.discovery().localJoinFuture().isDone())
                        return;

                    onMetastorageUpdate((boolean)newVal);
                });
            }

            @Override public void onReadyForWrite(DistributedMetaStorage metastorage) {
                PerformaceStatisticsProcessor.this.metastorage = metastorage;

                try {
                    Boolean start = metastorage.read(STAT_ENABLED_PREFIX);

                    if (start == null)
                        return;

                    onMetastorageUpdate(start);
                }
                catch (IgniteCheckedException e) {
                    throw new IgniteException(e);
                }
            }
        });
    }

    /** @return {@code True} if collecting performance statistics is enabled. */
    public boolean enabled() {
        return writer.enabled();
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

        metastorage.write(STAT_ENABLED_PREFIX, true);
    }

    /**
     * Stops collecting performance statistics.
     *
     * @throws IgniteCheckedException If stopping failed.
     */
    public void stopCollectStatistics() throws IgniteCheckedException {
        A.notNull(metastorage, "Metastorage not ready. Node not started?");

        metastorage.write(STAT_ENABLED_PREFIX, false);
    }

    /**
     * @param type Operation type.
     * @param cacheId Cache id.
     * @param startTime Start time in milliseconds.
     * @param duration Duration in nanoseconds.
     */
    public void cacheOperation(OperationType type, int cacheId, long startTime, long duration) {
        writer.cacheOperation(type, cacheId, startTime, duration);
    }

    /**
     * @param cacheIds Cache IDs.
     * @param startTime Start time in milliseconds.
     * @param duration Duration in nanoseconds.
     * @param commited {@code True} if commited.
     */
    public void transaction(GridIntList cacheIds, long startTime, long duration, boolean commited) {
        writer.transaction(cacheIds, startTime, duration, commited);
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
        writer.query(type, text, id, startTime, duration, success);
    }

    /**
     * @param type Cache query type.
     * @param queryNodeId Originating node id.
     * @param id Query id.
     * @param logicalReads Number of logical reads.
     * @param physicalReads Number of physical reads.
     */
    public void queryReads(GridCacheQueryType type, UUID queryNodeId, long id, long logicalReads, long physicalReads) {
        writer.queryReads(type, queryNodeId, id, logicalReads, physicalReads);
    }

    /**
     * @param sesId Session id.
     * @param taskName Task name.
     * @param startTime Start time in milliseconds.
     * @param duration Duration.
     * @param affPartId Affinity partition id.
     */
    public void task(IgniteUuid sesId, String taskName, long startTime, long duration, int affPartId) {
        writer.task(sesId, taskName, startTime, duration, affPartId);
    }

    /**
     * @param sesId Session id.
     * @param queuedTime Time job spent on waiting queue.
     * @param startTime Start time in milliseconds.
     * @param duration Job execution time.
     * @param timedOut {@code True} if job is timed out.
     */
    public void job(IgniteUuid sesId, long queuedTime, long startTime, long duration, boolean timedOut) {
        writer.job(sesId, queuedTime, startTime, duration, timedOut);
    }

    /** {@inheritDoc} */
    @Override public void onKernalStop(boolean cancel) {
        if (enabled())
            writer.stop();
    }

    /** {@inheritDoc} */
    @Override public void onDisconnected(IgniteFuture<?> reconnectFut) {
        if (enabled())
            writer.stop();
    }

    /** Starts or stops collecting statistics on metastorage update. */
    private void onMetastorageUpdate(boolean start) {
        ctx.closure().runLocalSafe(() -> {
            if (start)
                writer.start();
            else
                writer.stop();
        });
    }
}
