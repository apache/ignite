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
import org.apache.ignite.internal.processors.query.schema.operation.SchemaAddQueryEntityOperation;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

/**
 * Cache context information. Required to support query infrastructure for not started caches on non affinity nodes.
 */
@GridToStringExclude
public class GridCacheContextInfo<K, V> {
    /** Cache is client or not. */
    private final boolean clientCache;

    /** Dynamic cache deployment ID. */
    private final IgniteUuid dynamicDeploymentId;

    /** Cache configuration. */
    private volatile CacheConfiguration<K, V> config;

    /** Cache group ID. */
    private final int groupId;

    /** Cache ID. */
    private final int cacheId;

    /** Full cache context. Can be {@code null} in case a cache is not started. */
    @Nullable private volatile GridCacheContext<K, V> cctx;

    /**
     * Constructor of full cache context.
     *
     * @param cctx Cache context.
     * @param clientCache Client cache or not.
     */
    public GridCacheContextInfo(GridCacheContext<K, V> cctx, boolean clientCache) {
        config = cctx.config();
        dynamicDeploymentId = null;
        groupId = cctx.groupId();
        cacheId = cctx.cacheId();

        this.clientCache = clientCache;

        this.cctx = cctx;
    }

    /**
     * Constructor of not started cache context.
     *
     * @param cacheDesc Cache descriptor.
     */
    public GridCacheContextInfo(DynamicCacheDescriptor cacheDesc) {
        config = cacheDesc.cacheConfiguration();
        dynamicDeploymentId = cacheDesc.deploymentId();
        groupId = cacheDesc.groupId();
        cacheId = CU.cacheId(config.getName());

        clientCache = true;
    }

    /**
     * @return Cache configuration.
     */
    public CacheConfiguration<K, V> config() {
        return config;
    }

    /**
     * @return Cache name.
     */
    public String name() {
        return config.getName();
    }

    /**
     * @return Cache group id.
     */
    public int groupId() {
        return groupId;
    }

    /**
     * @return Cache id.
     */
    public int cacheId() {
        return cacheId;
    }

    /**
     * @return {@code true} in case affinity node.
     */
    public boolean affinityNode() {
        return cctx != null && cctx.affinityNode();
    }

    /**
     * @return Cache context. {@code null} for not started cache.
     */
    @Nullable public GridCacheContext<K, V> cacheContext() {
        return cctx;
    }

    /**
     * @return Dynamic deployment ID.
     */
    public IgniteUuid dynamicDeploymentId() {
        GridCacheContext<K, V> cctx0 = cctx;

        if (cctx0 != null)
            return cctx0.dynamicDeploymentId();

        assert dynamicDeploymentId != null : "Deployment id is not set and cache context is not initialized: " + this;

        return dynamicDeploymentId;
    }

    /**
     * Set real cache context in case cache has been fully initted and start.
     *
     * @param cctx Initted cache context.
     */
    public void initCacheContext(GridCacheContext<K, V> cctx) {
        assert this.cctx == null : this.cctx;
        assert cctx != null;

        this.cctx = cctx;
    }

    /**
     * Clear real cache context; the method is used on cache.close() on not-affinity nodes to
     * set up cache on idle state (not started on client, similar to state after join client node).
     */
    public void clearCacheContext( ) {
        cctx = null;
    }

    /**
     * @return {@code true} For client cache.
     */
    public boolean isClientCache() {
        return clientCache;
    }

    /**
     * @return {@code true} If Cache context is initted.
     */
    public boolean isCacheContextInited() {
        return cctx != null;
    }

    /**
     * Apply changes from {@link SchemaAddQueryEntityOperation}.
     *
     * @param op Add query entity schema operation.
     */
    public void onSchemaAddQueryEntity(SchemaAddQueryEntityOperation op) {
        if (cctx != null) {
            cctx.onSchemaAddQueryEntity(op);

            config = cctx.config();
        }
        else {
            CacheConfiguration<K, V> oldCfg = config;

            config = GridCacheUtils.patchCacheConfiguration(oldCfg, op);
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "GridCacheContextInfo: " + name() + " " + (isCacheContextInited() ? "started" : "not started");
    }
}
