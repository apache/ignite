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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.managers.communication.GridIoPolicy;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.processors.plugin.IgnitePluginProcessor;
import org.apache.ignite.plugin.extensions.communication.IoPool;

import java.util.Arrays;
import java.util.concurrent.Executor;

import static org.apache.ignite.internal.managers.communication.GridIoPolicy.AFFINITY_POOL;
import static org.apache.ignite.internal.managers.communication.GridIoPolicy.IGFS_POOL;
import static org.apache.ignite.internal.managers.communication.GridIoPolicy.MANAGEMENT_POOL;
import static org.apache.ignite.internal.managers.communication.GridIoPolicy.MARSH_CACHE_POOL;
import static org.apache.ignite.internal.managers.communication.GridIoPolicy.P2P_POOL;
import static org.apache.ignite.internal.managers.communication.GridIoPolicy.PUBLIC_POOL;
import static org.apache.ignite.internal.managers.communication.GridIoPolicy.SYSTEM_POOL;
import static org.apache.ignite.internal.managers.communication.GridIoPolicy.UTILITY_CACHE_POOL;
import static org.apache.ignite.internal.managers.communication.GridIoPolicy.isReservedGridIoPolicy;

/**
 * Processor which abstracts out thread pool management.
 */
public class PoolProcessor extends GridProcessorAdapter {
    /** Map of {@link IoPool}-s injected by Ignite plugins. */
    private final IoPool[] extPools = new IoPool[128];

    /**
     * Constructor.
     *
     * @param ctx Kernal context.
     */
    public PoolProcessor(GridKernalContext ctx) {
        super(ctx);
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteCheckedException {
        // Assuming that pool processor is started after plugin processor, so that extensions are already initialized.
        IgnitePluginProcessor plugins = ctx.plugins();

        assert plugins != null;

        // Process custom IO messaging pool extensions:
        final IoPool[] executorExtensions = ctx.plugins().extensions(IoPool.class);

        if (executorExtensions != null) {
            // Store it into the map and check for duplicates:
            for (IoPool ex : executorExtensions) {
                final byte id = ex.id();

                // 1. Check the pool id is non-negative:
                if (id < 0)
                    throw new IgniteCheckedException("Failed to register IO executor pool because its Id is negative " +
                        "[id=" + id + ']');

                // 2. Check the pool id is in allowed range:
                if (isReservedGridIoPolicy(id))
                    throw new IgniteCheckedException("Failed to register IO executor pool because its Id in in the " +
                        "reserved range (0-31) [id=" + id + ']');

                // 3. Check the pool for duplicates:
                if (extPools[id] != null)
                    throw new IgniteCheckedException("Failed to register IO executor pool because its " +
                        "Id as already used [id=" + id + ']');

                extPools[id] = ex;
            }
        }
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
            case P2P_POOL:
                return ctx.getPeerClassLoadingExecutorService();
            case SYSTEM_POOL:
                return ctx.getSystemExecutorService();
            case PUBLIC_POOL:
                return ctx.getExecutorService();
            case MANAGEMENT_POOL:
                return ctx.getManagementExecutorService();
            case AFFINITY_POOL:
                return ctx.getAffinityExecutorService();

            case UTILITY_CACHE_POOL:
                assert ctx.utilityCachePool() != null : "Utility cache pool is not configured.";

                return ctx.utilityCachePool();

            case MARSH_CACHE_POOL:
                assert ctx.marshallerCachePool() != null : "Marshaller cache pool is not configured.";

                return ctx.marshallerCachePool();

            case IGFS_POOL:
                assert ctx.getIgfsExecutorService() != null : "IGFS pool is not configured.";

                return ctx.getIgfsExecutorService();

            default: {
                assert plc >= 0 : "Negative policy: " + plc;

                if (plc < 0)
                    throw new IgniteCheckedException("Policy cannot be negative: " + plc);

                if (GridIoPolicy.isReservedGridIoPolicy(plc))
                    throw new IgniteCheckedException("Policy is reserved for internal usage (range 0-31): " + plc);

                IoPool pool = extPools[plc];

                if (pool == null)
                    throw new IgniteCheckedException("No pool is registered for policy: " + plc);

                assert plc == pool.id();

                Executor ex = pool.executor();

                if (ex == null)
                    throw new IgniteCheckedException("Thread pool for policy is null: " + plc);

                return ex;
            }
        }
    }
}
