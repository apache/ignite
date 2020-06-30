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
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteFeatures;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryType;
import org.apache.ignite.internal.processors.metastorage.DistributedMetaStorage;
import org.apache.ignite.internal.processors.metastorage.DistributedMetastorageLifecycleListener;
import org.apache.ignite.internal.processors.metastorage.ReadableDistributedMetaStorage;
import org.apache.ignite.internal.util.GridIntList;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.lang.IgniteUuid;

import static org.apache.ignite.internal.IgniteFeatures.allNodesSupports;

/**
 * Performance statistics processor.
 * <p>
 * Manages collecting statistics.
 */
public class PerformaceStatisticsProcessor extends GridProcessorAdapter implements PerformanceStatisticsHandler {
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
                    boolean start = (boolean)newVal;
                    System.out.println("MY enabled="+start);

                    if (start)
                        ctx.closure().runLocalSafe(writer::start);
                    else
                        writer.stop();
                });
            }

            @Override public void onReadyForWrite(DistributedMetaStorage metastorage) {
                PerformaceStatisticsProcessor.this.metastorage = metastorage;
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
    @Override public void onKernalStop(boolean cancel) {
        if (enabled())
            writer.stop();
    }
}
