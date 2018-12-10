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

package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

/**
 * Cache context information. Required to support query infrastructure for not started caches on non affinity nodes.
 */
@GridToStringExclude
public class GridCacheContextInfo<K, V> {
    /** Full cache context. Can be {@code null} in case a cache is not started. */
    @Nullable private volatile GridCacheContext gridCacheContext;

    /** Cache is client or not. */
    private final boolean clientCache;

    /** Kernal context. */
    private final GridKernalContext ctx;

    /** Dynamic cache deployment ID. */
    private volatile IgniteUuid dynamicDeploymentId;

    /** Cache configuration. */
    private final CacheConfiguration config;

    /** Cache group ID. */
    private final int groupId;

    /** Cache ID. */
    private final int cacheId;

    /**
     * Constructor of full cache context.
     *
     * @param gridCacheContext Cache context.
     * @param clientCache Client cache or not.
     */
    public GridCacheContextInfo(GridCacheContext<K, V> gridCacheContext, boolean clientCache) {
        this.gridCacheContext = gridCacheContext;
        this.ctx = gridCacheContext.kernalContext();
        this.config = gridCacheContext.config();
        this.dynamicDeploymentId = gridCacheContext.dynamicDeploymentId();
        this.groupId = gridCacheContext.groupId();
        this.cacheId = gridCacheContext.cacheId();
        this.clientCache = clientCache;
    }

    /**
     * Constructor of not started cache context.
     *
     * @param cacheDesc Cache descriptor.
     * @param ctx Kernal context.
     */
    public GridCacheContextInfo(DynamicCacheDescriptor cacheDesc, GridKernalContext ctx) {
        this.config = cacheDesc.cacheConfiguration();
        this.dynamicDeploymentId = cacheDesc.deploymentId();
        this.groupId = cacheDesc.groupId();
        this.ctx = ctx;
        this.clientCache = true;

        this.cacheId = CU.cacheId(config.getName());

    }

    /**
     * @return Cache configuration.
     */
    public CacheConfiguration config() {
        return isCacheContextInited() ? gridCacheContext.config() : config;
    }

    /**
     * @return Cache name.
     */
    public String name() {
        return isCacheContextInited() ? gridCacheContext.name() : config.getName();
    }

    /**
     * @return {@code true} in case cache use custom affinity mapper.
     */
    public boolean customAffinityMapper() {
        return isCacheContextInited() && gridCacheContext.customAffinityMapper();
    }

    /**
     * @return Cache group id.
     */
    public int groupId() {
        return isCacheContextInited() ? gridCacheContext.groupId() : groupId;
    }

    /**
     * @return Cache id.
     */
    public int cacheId() {
        return isCacheContextInited() ? gridCacheContext.cacheId() : cacheId;
    }

    /**
     * @return {@code true} in case affinity node.
     */
    public boolean affinityNode() {
        return isCacheContextInited() && gridCacheContext.affinityNode();
    }

    /**
     * @return Cache context. {@code null} for not started cache.
     */
    @Nullable public GridCacheContext gridCacheContext() {
        return gridCacheContext;
    }

    /**
     * @return Dynamic deployment ID.
     */
    public IgniteUuid dynamicDeploymentId() {
        return dynamicDeploymentId;
    }

    /**
     * Set real cache context in case cache has been fully initted and start.
     *
     * @param gridCacheCtx Initted cache context.
     */
    public void initCacheContext(GridCacheContext<?, ?> gridCacheCtx) {
        assert this.gridCacheContext == null : this.gridCacheContext;
        assert gridCacheCtx != null;

        this.gridCacheContext = gridCacheCtx;
    }

    /**
     * @return {@code true} For client cache.
     */
    public boolean isClientCache() {
        return clientCache;
    }

    /**
     * @return Kernal context.
     */
    public GridKernalContext context() {
        return ctx;
    }

    /**
     * @return {@code true} If Cache context is initted.
     */
    public boolean isCacheContextInited() {
        return gridCacheContext != null;
    }

    /**
     * Change local deployment id to cluster-wide during exchange.
     */
    public void changeDeploymentId(IgniteUuid clusterWideDeploymentId) {
        this.dynamicDeploymentId = clusterWideDeploymentId;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "GridCacheContextInfo: " + name() + " " + (isCacheContextInited() ? "started" : "not started");
    }
}
