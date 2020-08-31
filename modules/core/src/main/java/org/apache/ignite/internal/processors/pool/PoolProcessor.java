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

package org.apache.ignite.internal.processors.pool;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.managers.communication.GridIoPolicy;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.processors.plugin.IgnitePluginProcessor;
import org.apache.ignite.plugin.extensions.communication.IoPool;
import org.jetbrains.annotations.Nullable;

/**
 * Processor which abstracts out thread pool management.
 */
public class PoolProcessor extends GridProcessorAdapter {
    /** Map of {@link IoPool}-s injected by Ignite plugins. */
    private final IoPool[] extPools = new IoPool[128];

    /** Custom named pools. */
    private final Map<String, ? extends ExecutorService> customExecs;

    /**
     * Constructor.
     *
     * @param ctx Kernal context.
     */
    public PoolProcessor(GridKernalContext ctx) {
        super(ctx);

        IgnitePluginProcessor plugins = ctx.plugins();

        if (plugins != null) {
            // Process custom IO messaging pool extensions:
            final IoPool[] executorExtensions = ctx.plugins().extensions(IoPool.class);

            if (executorExtensions != null) {
                // Store it into the map and check for duplicates:
                for (IoPool ex : executorExtensions) {
                    final byte id = ex.id();

                    // 1. Check the pool id is non-negative:
                    if (id < 0)
                        throw new IgniteException("Failed to register IO executor pool because its ID is " +
                            "negative: " + id);

                    // 2. Check the pool id is in allowed range:
                    if (GridIoPolicy.isReservedGridIoPolicy(id))
                        throw new IgniteException("Failed to register IO executor pool because its ID in in the " +
                            "reserved range: " + id);

                    // 3. Check the pool for duplicates:
                    if (extPools[id] != null)
                        throw new IgniteException("Failed to register IO executor pool because its ID as " +
                            "already used: " + id);

                    extPools[id] = ex;
                }
            }
        }

        customExecs = ctx.customExecutors();
    }

    /** {@inheritDoc} */
    @Override public void stop(boolean cancel) throws IgniteCheckedException {
        // Avoid external thread pools GC retention.
        Arrays.fill(extPools, null);
    }

    /**
     * @return P2P pool.
     */
    public Executor p2pPool() {
        return ctx.getPeerClassLoadingExecutorService();
    }

    /**
     * Get executor service for policy.
     *
     * @param plc Policy.
     * @return Executor service.
     * @throws IgniteCheckedException If failed.
     */
    public Executor poolForPolicy(byte plc) throws IgniteCheckedException {
        switch (plc) {
            case GridIoPolicy.P2P_POOL:
                return ctx.getPeerClassLoadingExecutorService();
            case GridIoPolicy.SYSTEM_POOL:
                return ctx.getSystemExecutorService();
            case GridIoPolicy.PUBLIC_POOL:
                return ctx.getExecutorService();
            case GridIoPolicy.MANAGEMENT_POOL:
                return ctx.getManagementExecutorService();
            case GridIoPolicy.AFFINITY_POOL:
                return ctx.getAffinityExecutorService();

            case GridIoPolicy.IDX_POOL:
                assert ctx.getIndexingExecutorService() != null : "Indexing pool is not configured.";

                return ctx.getIndexingExecutorService();

            case GridIoPolicy.UTILITY_CACHE_POOL:
                assert ctx.utilityCachePool() != null : "Utility cache pool is not configured.";

                return ctx.utilityCachePool();

            case GridIoPolicy.SERVICE_POOL:
                assert ctx.getServiceExecutorService() != null : "Service pool is not configured.";

                return ctx.getServiceExecutorService();

            case GridIoPolicy.DATA_STREAMER_POOL:
                assert ctx.getDataStreamerExecutorService() != null : "Data streamer pool is not configured.";

                return ctx.getDataStreamerExecutorService();

            case GridIoPolicy.QUERY_POOL:
                assert ctx.getQueryExecutorService() != null : "Query pool is not configured.";

                return ctx.getQueryExecutorService();

            case GridIoPolicy.SCHEMA_POOL:
                assert ctx.getSchemaExecutorService() != null : "Query pool is not configured.";

                return ctx.getSchemaExecutorService();

            default: {
                if (plc < 0)
                    throw new IgniteCheckedException("Policy cannot be negative: " + plc);

                if (GridIoPolicy.isReservedGridIoPolicy(plc))
                    throw new IgniteCheckedException("Policy is reserved for internal usage (range 0-31): " + plc);

                IoPool pool = extPools[plc];

                if (pool == null)
                    throw new IgniteCheckedException("No pool is registered for policy: " + plc);

                assert plc == pool.id();

                Executor res = pool.executor();

                if (res == null)
                    throw new IgniteCheckedException("Thread pool for policy is null: " + plc);

                return res;
            }
        }
    }

    /**
     * Gets executor service for custom policy by executor name.
     *
     * @param name Executor name.
     * @return Executor service.
     */
    @Nullable public Executor customExecutor(String name) {
        assert name != null;

        Executor exec = null;

        if (customExecs != null)
            exec = customExecs.get(name);

        return exec;
    }
}
