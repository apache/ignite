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

import java.io.Serializable;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.CoreMessagesProvider;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.MarshallableMessage;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.processors.query.QuerySchema;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.marshaller.Marshaller;
import org.jetbrains.annotations.Nullable;

/**
 * Cache start/stop request.
 */
public class DynamicCacheChangeRequest implements MarshallableMessage, Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    @Order(0)
    UUID reqId;

    /** Start ID. */
    @Order(1)
    IgniteUuid deploymentId;

    /** Stop cache name. */
    @GridToStringExclude
    @Order(2)
    String cacheName;

    /** Cache start configuration. */
    @GridToStringExclude
    private CacheConfiguration<?, ?> startCfg;

    /** Bytes of {@link #startCfg}. */
    @Order(3)
    byte[] cfgBytes;

    /** Cache type. */
    @Order(4)
    CacheType cacheType;

    /** Near node ID in case if near cache is being started. */
    @Order(5)
    UUID initiatingNodeId;

    /** Near cache configuration. */
    @GridToStringExclude
    private NearCacheConfiguration<?, ?> nearCacheCfg;

    /** Bytes of {@link #nearCacheCfg}. */
    @Order(6)
    byte[] nearCfgBytes;

    /** Start only client cache, do not start data nodes. */
    @Order(7)
    boolean clientStartOnly;

    /** Stop flag. */
    @Order(8)
    boolean stop;

    /** Restart flag. */
    @Order(9)
    boolean restart;

    /** Finalize update counters flag. */
    @Order(10)
    boolean finalizePartitionCounters;

    /** Restart operation id. */
    @Order(11)
    IgniteUuid restartId;

    /** Cache active on start or not*/
    @Order(12)
    boolean disabledAfterStart;

    /** Cache data destroy flag. Setting to <code>true</code> will cause removing all cache data.*/
    @Order(13)
    boolean destroy;

    /** Whether cache was created through SQL. */
    @Order(14)
    boolean sql;

    /** Fail if exists flag. */
    @Order(15)
    boolean failIfExists;

    /** Template configuration flag. */
    @Order(16)
    boolean template;

    /** Reset lost partitions flag. */
    @Order(17)
    boolean resetLostPartitions;

    /** Dynamic schema. */
    QuerySchema schema;

    /** Bytes of {@link #schema}. */
    @Order(18)
    byte[] schemaBytes;

    /** Is transient. */
    private boolean locallyConfigured;

    /** Encryption key. */
    @Order(19)
    @Nullable byte[] encKey;

    /** Id of encryption key. */
    @Order(20)
    int encKeyId;

    /** Master key digest. */
    @Order(21)
    @Nullable byte[] masterKeyDigest;

    /** Cache configuration enrichment. */
    @Order(22)
    CacheConfigurationEnrichment cacheCfgEnrichment;

    /** Empty constructor for {@link CoreMessagesProvider}. */
    public DynamicCacheChangeRequest() {
        // No-op.
    }

    /**
     * @param reqId Unique request ID.
     * @param cacheName Cache stop name.
     * @param initiatingNodeId Initiating node ID.
     */
    public DynamicCacheChangeRequest(UUID reqId, String cacheName, UUID initiatingNodeId) {
        assert reqId != null;

        this.reqId = reqId;
        this.cacheName = cacheName;
        this.initiatingNodeId = initiatingNodeId;
    }

    /** {@inheritDoc} */
    @Override public void prepareMarshal(Marshaller marsh) throws IgniteCheckedException {
        cfgBytes = U.marshal(marsh, startCfg);

        if (nearCacheCfg != null)
            nearCfgBytes = U.marshal(marsh, nearCacheCfg);

        if (schema != null)
            schemaBytes = U.marshal(marsh, schema);
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(Marshaller marsh, ClassLoader clsLdr) throws IgniteCheckedException {
        startCfg = U.unmarshal(marsh, cfgBytes, clsLdr);

        if (nearCfgBytes != null)
            nearCacheCfg = U.unmarshal(marsh, nearCfgBytes, clsLdr);

        if (schemaBytes != null)
            schema = U.unmarshal(marsh, schemaBytes, clsLdr);

        cfgBytes = null;
        nearCfgBytes = null;
        schemaBytes = null;
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
     * @return Request to finalize partition update counters.
     */
    static DynamicCacheChangeRequest finalizePartitionCounters(GridKernalContext ctx) {
        DynamicCacheChangeRequest req = new DynamicCacheChangeRequest(UUID.randomUUID(), null, ctx.localNodeId());

        req.markFinalizePartitionCounters();

        return req;
    }

    /**
     * @param ctx Context.
     * @param cfg0 Template configuration.
     * @param splitCfg Cache configuration splitter.
     * @return Request to add template.
     */
    static DynamicCacheChangeRequest addTemplateRequest(
        GridKernalContext ctx,
        CacheConfiguration<?, ?> cfg0,
        T2<CacheConfiguration, CacheConfigurationEnrichment> splitCfg
    ) {
        DynamicCacheChangeRequest req = new DynamicCacheChangeRequest(UUID.randomUUID(), cfg0.getName(), ctx.localNodeId());

        req.template(true);

        req.startCacheConfiguration(splitCfg.get1());
        req.cacheConfigurationEnrichment(splitCfg.get2());

        req.schema(new QuerySchema(cfg0.getQueryEntities()));
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
    public boolean destroy() {
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
     * Set finalize partition update counters flag.
     */
    public void markFinalizePartitionCounters() {
        finalizePartitionCounters = true;
    }

    /**
     * Finalize partition update counters flag.
     */
    public boolean finalizePartitionCounters() {
        return finalizePartitionCounters;
    }

    /**
     * @return Id of restart to allow only initiator start the restarting cache.
     */
    public IgniteUuid restartId() {
        return restartId;
    }

    /**
     * @param restartId Id of cache restart requester.
     */
    public void restartId(IgniteUuid restartId) {
        this.restartId = restartId;
    }

    /**
     * @return Cache name.
     */
    public String cacheName() {
        return cacheName;
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
    public NearCacheConfiguration<?, ?> nearCacheConfiguration() {
        return nearCacheCfg;
    }

    /**
     * @param nearCacheCfg Near cache configuration.
     */
    public void nearCacheConfiguration(NearCacheConfiguration<?, ?> nearCacheCfg) {
        this.nearCacheCfg = nearCacheCfg;
    }

    /**
     * @return Cache configuration.
     */
    public CacheConfiguration<?, ?> startCacheConfiguration() {
        return startCfg;
    }

    /**
     * @param startCfg Cache configuration.
     */
    public void startCacheConfiguration(CacheConfiguration<?, ?> startCfg) {
        this.startCfg = startCfg;

        if (startCfg.getNearConfiguration() != null)
            nearCacheCfg = startCfg.getNearConfiguration();
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

    /**
     * Sets encryption key id.
     *
     * @param encKeyId Encryption key id.
     */
    public void encryptionKeyId(int encKeyId) {
        this.encKeyId = encKeyId;
    }

    /**
     * @return Encryption key id.
     */
    @Nullable public int encryptionKeyId() {
        return encKeyId;
    }

    /** @param masterKeyDigest Master key digest. */
    public void masterKeyDigest(@Nullable byte[] masterKeyDigest) {
        this.masterKeyDigest = masterKeyDigest;
    }

    /** @return Master key digest that encrypted the group encryption key. */
    @Nullable public byte[] masterKeyDigest() {
        return masterKeyDigest;
    }

    /**
     * @return Cache configuration enrichment.
     */
    public CacheConfigurationEnrichment cacheConfigurationEnrichment() {
        return cacheCfgEnrichment;
    }

    /**
     * @param cacheCfgEnrichment Cache config enrichment.
     */
    public void cacheConfigurationEnrichment(CacheConfigurationEnrichment cacheCfgEnrichment) {
        this.cacheCfgEnrichment = cacheCfgEnrichment;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "DynamicCacheChangeRequest [cacheName=" + cacheName() +
            ", hasCfg=" + (startCfg != null) +
            ", nodeId=" + initiatingNodeId +
            ", clientStartOnly=" + clientStartOnly +
            ", stop=" + stop +
            ", destroy=" + destroy +
            ", disabledAfterStart=" + disabledAfterStart +
            ']';
    }
}
