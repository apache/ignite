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
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.query.QuerySchema;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

import java.io.Serializable;
import java.util.UUID;

/**
 * Cache start/stop request.
 */
public class DynamicCacheChangeRequest implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private UUID reqId;

    /** Start ID. */
    private IgniteUuid deploymentId;

    /** Stop cache name. */
    @GridToStringExclude
    private String cacheName;

    /** Cache start configuration. */
    private CacheConfiguration startCfg;

    /** Cache type. */
    private CacheType cacheType;

    /** Near node ID in case if near cache is being started. */
    private UUID initiatingNodeId;

    /** Near cache configuration. */
    private NearCacheConfiguration nearCacheCfg;

    /** Start only client cache, do not start data nodes. */
    private boolean clientStartOnly;

    /** Stop flag. */
    private boolean stop;

    /** Destroy. */
    private boolean destroy;

    /** Close flag. */
    private boolean close;

    /** Fail if exists flag. */
    private boolean failIfExists;

    /** Template configuration flag. */
    private boolean template;

    /** */
    private UUID rcvdFrom;

    /** Cache state. Set to non-null when global state is changed. */
    private ClusterState state;

    /** Reset lost partitions flag. */
    private boolean resetLostPartitions;

    /** Dynamic schema. */
    private QuerySchema schema;

    /**
     * @param reqId Unique request ID.
     * @param cacheName Cache stop name.
     * @param initiatingNodeId Initiating node ID.
     */
    public DynamicCacheChangeRequest(UUID reqId, String cacheName, UUID initiatingNodeId) {
        assert reqId != null;
        assert cacheName != null;
        assert initiatingNodeId != null;

        this.reqId = reqId;
        this.cacheName = cacheName;
        this.initiatingNodeId = initiatingNodeId;
    }

    /**
     * @param reqId Unique request ID.
     * @param state New cluster state.
     * @param initiatingNodeId Initiating node ID.
     */
    public DynamicCacheChangeRequest(UUID reqId, ClusterState state, UUID initiatingNodeId) {
        assert reqId != null;
        assert state != null;
        assert initiatingNodeId != null;

        this.reqId = reqId;
        this.state = state;
        this.initiatingNodeId = initiatingNodeId;
    }

    /**
     * @param ctx Context.
     * @param cacheName Cache name.
     * @return Request to reset lost partitions.
     */
    static DynamicCacheChangeRequest resetLostPartitions(GridKernalContext ctx, String cacheName) {
        DynamicCacheChangeRequest req = new DynamicCacheChangeRequest(UUID.randomUUID(), cacheName, ctx.localNodeId());

        req.markResetLostPartitions();

        return req;
    }

    /**
     * @param ctx Context.
     * @param cfg0 Template configuration.
     * @return Request to add template.
     */
    static DynamicCacheChangeRequest addTemplateRequest(GridKernalContext ctx, CacheConfiguration<?, ?> cfg0) {
        CacheConfiguration<?, ?> cfg = new CacheConfiguration<>(cfg0);

        DynamicCacheChangeRequest req = new DynamicCacheChangeRequest(UUID.randomUUID(), cfg.getName(), ctx.localNodeId());

        req.template(true);
        req.startCacheConfiguration(cfg);
        req.schema(new QuerySchema(cfg.getQueryEntities()));
        req.deploymentId(IgniteUuid.randomUuid());

        return req;
    }

    /**
     * @param ctx Context.
     * @param cacheName Cache name.
     * @return Request to close client cache.
     */
    static DynamicCacheChangeRequest closeRequest(GridKernalContext ctx, String cacheName) {
        DynamicCacheChangeRequest req = new DynamicCacheChangeRequest(UUID.randomUUID(), cacheName, ctx.localNodeId());

        req.close(true);

        return req;
    }

    /**
     * @param ctx Context.
     * @param cacheName Cache name.
     * @param destroy Destroy flag.
     * @return Cache stop request.
     */
    static DynamicCacheChangeRequest stopRequest(GridKernalContext ctx, String cacheName, boolean destroy) {
        DynamicCacheChangeRequest req = new DynamicCacheChangeRequest(UUID.randomUUID(), cacheName, ctx.localNodeId());

        req.stop(true);
        req.destroy(destroy);

        return req;
    }

    /**
     * @return Request ID.
     */
    public UUID requestId() {
        return reqId;
    }

    /**
     * @return State.
     */
    public ClusterState state() {
        return state;
    }

    /**
     * @return {@code True} if global caches state is changes.
     */
    public boolean globalStateChange() {
        return state != null;
    }

    /**
     * @param template {@code True} if this is request for adding template configuration.
     */
    public void template(boolean template) {
        this.template = template;
    }

    /**
     * @return {@code True} if this is template configuration.
     */
    public boolean template() {
        return template;
    }

    /**
     * @return Deployment ID.
     */
    public IgniteUuid deploymentId() {
        return deploymentId;
    }

    /**
     * @param deploymentId Deployment ID.
     */
    public void deploymentId(IgniteUuid deploymentId) {
        this.deploymentId = deploymentId;
    }

    /**
     * @return {@code True} if this is a start request.
     */
    public boolean start() {
        return !template && startCfg != null;
    }

    /**
     * Set resetLostPartitions flag.
     */
    public void markResetLostPartitions() {
        resetLostPartitions = true;
    }

    /**
     * @return Reset lost partitions flag.
     */
    public boolean resetLostPartitions() {
        return resetLostPartitions;
    }

    /**
     * @return {@code True} if this is a stop request.
     */
    public boolean stop() {
        return stop;
    }

    /**
     *
     */
    public boolean destroy(){
        return destroy;
    }

    /**
     * @param destroy Destroy.
     */
    public void destroy(boolean destroy) {
        this.destroy = destroy;
    }

    /**
     * @param stop New stop flag.
     */
    public void stop(boolean stop) {
        this.stop = stop;
    }

    /**
     * @return Cache name.
     */
    public String cacheName() {
        return cacheName;
    }

    /**
     * @param cacheName Cache name.
     */
    public void cacheName(String cacheName) {
        this.cacheName = cacheName;
    }

    /**
     * @return Near node ID.
     */
    public UUID initiatingNodeId() {
        return initiatingNodeId;
    }

    /**
     * @return Near cache configuration.
     */
    public NearCacheConfiguration nearCacheConfiguration() {
        return nearCacheCfg;
    }

    /**
     * @param nearCacheCfg Near cache configuration.
     */
    public void nearCacheConfiguration(NearCacheConfiguration nearCacheCfg) {
        this.nearCacheCfg = nearCacheCfg;
    }

    /**
     * @return Cache configuration.
     */
    public CacheConfiguration startCacheConfiguration() {
        return startCfg;
    }

    /**
     * @param startCfg Cache configuration.
     */
    public void startCacheConfiguration(CacheConfiguration startCfg) {
        this.startCfg = startCfg;
    }

    /**
     * @param cacheType Cache type.
     */
    public void cacheType(CacheType cacheType) {
        this.cacheType = cacheType;
    }

    /**
     * @return Cache type.
     */
    public CacheType cacheType() {
        return cacheType;
    }

    /**
     * @return Client start only.
     */
    public boolean clientStartOnly() {
        return clientStartOnly;
    }

    /**
     * @param clientStartOnly Client start only.
     */
    public void clientStartOnly(boolean clientStartOnly) {
        this.clientStartOnly = clientStartOnly;
    }

    /**
     * @return Fail if exists flag.
     */
    public boolean failIfExists() {
        return failIfExists;
    }

    /**
     * @param failIfExists Fail if exists flag.
     */
    public void failIfExists(boolean failIfExists) {
        this.failIfExists = failIfExists;
    }

    /**
     * @return Close flag.
     */
    public boolean close() {
        return close;
    }

    /**
     * @param close New close flag.
     */
    public void close(boolean close) {
        this.close = close;
    }

    /**
     * @param nodeId ID of node provided cache configuration in discovery data.
     */
    public void receivedFrom(UUID nodeId) {
        rcvdFrom = nodeId;
    }

    /**
     * @return ID of node provided cache configuration in discovery data.
     */
    @Nullable public UUID receivedFrom() {
        return rcvdFrom;
    }

    /**
     * @return Dynamic schema.
     */
    public QuerySchema schema() {
        return schema;
    }

    /**
     * @param schema Dynamic schema.
     */
    public void schema(QuerySchema schema) {
        this.schema = schema != null ? schema.copy() : null;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(DynamicCacheChangeRequest.class, this, "cacheName", cacheName());
    }
}
