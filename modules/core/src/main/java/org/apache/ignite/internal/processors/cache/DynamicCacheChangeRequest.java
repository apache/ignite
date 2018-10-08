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
    @GridToStringExclude
    private CacheConfiguration startCfg;

    /** Cache type. */
    private CacheType cacheType;

    /** Near node ID in case if near cache is being started. */
    private UUID initiatingNodeId;

    /** Near cache configuration. */
    @GridToStringExclude
    private NearCacheConfiguration nearCacheCfg;

    /** Start only client cache, do not start data nodes. */
    private boolean clientStartOnly;

    /** Stop flag. */
    private boolean stop;

    /** Restart flag. */
    private boolean restart;

    /** Cache active on start or not*/
    private boolean disabledAfterStart;

    /** Cache data destroy flag. Setting to <code>true</code> will cause removing all cache data.*/
    private boolean destroy;

    /** Whether cache was created through SQL. */
    private boolean sql;

    /** Fail if exists flag. */
    private boolean failIfExists;

    /** Template configuration flag. */
    private boolean template;

    /** */
    private UUID rcvdFrom;

    /** Reset lost partitions flag. */
    private boolean resetLostPartitions;

    /** Dynamic schema. */
    private QuerySchema schema;

    /** */
    private transient boolean locallyConfigured;

    /** Encryption key. */
    @Nullable private byte[] encKey;

    /**
     * @param reqId Unique request ID.
     * @param cacheName Cache stop name.
     * @param initiatingNodeId Initiating node ID.
     */
    public DynamicCacheChangeRequest(UUID reqId, String cacheName, UUID initiatingNodeId) {
        assert reqId != null;
        assert cacheName != null;

        this.reqId = reqId;
        this.cacheName = cacheName;
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
     * @param sql {@code true} if the cache must be stopped only if it was created by SQL command {@code CREATE TABLE}.
     * @param destroy Cache data destroy flag. Setting to <code>true</code> will cause removing all cache data.
     * @return Cache stop request.
     */
    public static DynamicCacheChangeRequest stopRequest(
        GridKernalContext ctx,
        String cacheName,
        boolean sql,
        boolean destroy
    ) {
        DynamicCacheChangeRequest req = new DynamicCacheChangeRequest(UUID.randomUUID(), cacheName, ctx.localNodeId());

        req.sql(sql);
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
     * @return Cache data destroy flag. Setting to <code>true</code> will remove all cache data.
     */
    public boolean destroy(){
        return destroy;
    }

    /**
     * Sets cache data destroy flag. Setting to <code>true</code> will cause removing all cache data.
     * @param destroy Destroy flag.
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
     * @return {@code True} if this is a restart request.
     */
    public boolean restart() {
        return restart;
    }

    /**
     * @param restart New restart flag.
     */
    public void restart(boolean restart) {
        this.restart = restart;
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
     * @return SQL flag.
     */
    public boolean sql() {
        return sql;
    }

    /**
     * Sets if cache is created using create table.
     *
     * @param sql New SQL flag.
     */
    public void sql(boolean sql) {
        this.sql = sql;
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

    /**
     * @return Locally configured flag.
     */
    public boolean locallyConfigured() {
        return locallyConfigured;
    }

    /**
     * @param locallyConfigured Locally configured flag.
     */
    public void locallyConfigured(boolean locallyConfigured) {
        this.locallyConfigured = locallyConfigured;
    }

    /**
     * @return state of cache after start
     */
    public boolean disabledAfterStart() {
        return disabledAfterStart;
    }

    /**
     * @param disabledAfterStart state of cache after start
     */
    public void disabledAfterStart(boolean disabledAfterStart) {
        this.disabledAfterStart = disabledAfterStart;
    }

    /**
     * @param encKey Encryption key.
     */
    public void encryptionKey(@Nullable byte[] encKey) {
        this.encKey = encKey;
    }

    /**
     * @return Encryption key.
     */
    @Nullable public byte[] encryptionKey() {
        return encKey;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "DynamicCacheChangeRequest [cacheName=" + cacheName() +
            ", hasCfg=" + (startCfg != null) +
            ", nodeId=" + initiatingNodeId +
            ", clientStartOnly=" + clientStartOnly +
            ", stop=" + stop +
            ", destroy=" + destroy +
            ", disabledAfterStart" + disabledAfterStart +
            ']';
    }
}
