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

import java.util.Collection;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cacheobject.IgniteCacheObjectProcessor;
import org.apache.ignite.internal.processors.query.QuerySchema;
import org.apache.ignite.internal.processors.query.QuerySchemaPatch;
import org.apache.ignite.internal.processors.query.schema.message.SchemaFinishDiscoveryMessage;
import org.apache.ignite.internal.processors.query.schema.operation.SchemaAbstractOperation;
import org.apache.ignite.internal.processors.query.schema.operation.SchemaAddQueryEntityOperation;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

/**
 * Cache start descriptor.
 */
public class DynamicCacheDescriptor {
    /** Cache start ID. */
    private IgniteUuid deploymentId;

    /** Cache configuration. */
    @GridToStringExclude
    private volatile CacheConfiguration cacheCfg;

    /** Statically configured flag. */
    private final boolean staticCfg;

    /** SQL flag - whether the cache is created by an SQL command such as {@code CREATE TABLE}. */
    private boolean sql;

    /** Cache type. */
    private CacheType cacheType;

    /** Template configuration flag. */
    private boolean template;

    /** */
    private boolean updatesAllowed = true;

    /** */
    private int cacheId;

    /** */
    private final UUID rcvdFrom;

    /** Mutex. */
    private final Object mux = new Object();

    /** Cached object context for marshalling issues when cache isn't started. */
    private volatile CacheObjectContext objCtx;

    /** */
    private boolean rcvdOnDiscovery;

    /** */
    private AffinityTopologyVersion startTopVer;

    /** */
    private AffinityTopologyVersion rcvdFromVer;

    /** */
    private AffinityTopologyVersion clientCacheStartVer;

    /** Mutex to control schema. */
    private final Object schemaMux = new Object();

    /** Current schema. */
    private QuerySchema schema;

    /** */
    private final CacheGroupDescriptor grpDesc;

    /** Cache config enrichment. */
    private final @Nullable CacheConfigurationEnrichment cacheCfgEnrichment;

    /** Cache config enriched. */
    private volatile boolean cacheCfgEnriched;

    /**
     * @param ctx Context.
     * @param cacheCfg Cache configuration.
     * @param cacheType Cache type.
     * @param grpDesc Group descriptor.
     * @param template {@code True} if this is template configuration.
     * @param rcvdFrom ID of node provided cache configuration
     * @param staticCfg {@code True} if cache statically configured.
     * @param sql SQL flag - whether the cache is created by an SQL command such as {@code CREATE TABLE}.
     * @param deploymentId Deployment ID.
     * @param schema Query schema.
     * @param cacheCfgEnrichment Cache configuration enrichment.
     */
    @SuppressWarnings("unchecked")
    public DynamicCacheDescriptor(GridKernalContext ctx,
        CacheConfiguration cacheCfg,
        CacheType cacheType,
        CacheGroupDescriptor grpDesc,
        boolean template,
        UUID rcvdFrom,
        boolean staticCfg,
        boolean sql,
        IgniteUuid deploymentId,
        QuerySchema schema,
        @Nullable CacheConfigurationEnrichment cacheCfgEnrichment
    ) {
        assert cacheCfg != null;
        assert grpDesc != null || template;
        assert schema != null;

        if (cacheCfg.getCacheMode() == CacheMode.REPLICATED && cacheCfg.getNearConfiguration() != null) {
            cacheCfg = new CacheConfiguration(cacheCfg);

            cacheCfg.setNearConfiguration(null);
        }

        this.cacheCfg = cacheCfg;
        this.cacheType = cacheType;
        this.grpDesc = grpDesc;
        this.template = template;
        this.rcvdFrom = rcvdFrom;
        this.staticCfg = staticCfg;
        this.sql = sql;
        this.deploymentId = deploymentId;

        cacheId = CU.cacheId(cacheCfg.getName());

        synchronized (schemaMux) {
            this.schema = schema.copy();
        }

        this.cacheCfgEnrichment = cacheCfgEnrichment;
    }

    /**
     * @return Cache group ID.
     */
    public int groupId() {
        assert grpDesc != null : this;

        return grpDesc.groupId();
    }

    /**
     * @return Cache group descriptor.
     */
    public CacheGroupDescriptor groupDescriptor() {
        assert grpDesc != null : this;

        return grpDesc;
    }

    /**
     * @return Cache ID.
     */
    public int cacheId() {
        return cacheId;
    }

    /**
     * @return {@code True} if this is template configuration.
     */
    public boolean template() {
        return template;
    }

    /**
     * @return Cache type.
     */
    public CacheType cacheType() {
        return cacheType;
    }

    /**
     * @return Start ID.
     */
    public IgniteUuid deploymentId() {
        return deploymentId;
    }

    /**
     * @return {@code True} if statically configured.
     */
    public boolean staticallyConfigured() {
        return staticCfg;
    }

    /**
     * @return SQL flag.
     */
    public boolean sql() {
        return sql;
    }

    /**
     * @return Cache name.
     */
    public String cacheName() {
        assert cacheCfg != null : this;

        return cacheCfg.getName();
    }

    /**
     * @return Cache configuration.
     */
    public CacheConfiguration cacheConfiguration() {
        return cacheCfg;
    }

    /**
     * @param cacheCfg Cache config.
     */
    public void cacheConfiguration(CacheConfiguration cacheCfg) {
        this.cacheCfg = cacheCfg;
    }

    /**
     * Creates and caches cache object context if needed.
     *
     * @param proc Object processor.
     * @return Cache object context.
     * @throws IgniteCheckedException If failed.
     */
    public CacheObjectContext cacheObjectContext(IgniteCacheObjectProcessor proc) throws IgniteCheckedException {
        if (objCtx == null) {
            synchronized (mux) {
                if (objCtx == null)
                    objCtx = proc.contextForCache(cacheCfg);
            }
        }

        return objCtx;
    }

    /**
     * @return Updates allowed flag.
     */
    public boolean updatesAllowed() {
        return updatesAllowed;
    }

    /**
     * @param updatesAllowed Updates allowed flag.
     */
    public void updatesAllowed(boolean updatesAllowed) {
        this.updatesAllowed = updatesAllowed;
    }

    /**
     * @return {@code True} if received in discovery data.
     */
    boolean receivedOnDiscovery() {
        return rcvdOnDiscovery;
    }

    /**
     * @param rcvdOnDiscovery {@code True} if received in discovery data.
     */
    void receivedOnDiscovery(boolean rcvdOnDiscovery) {
        this.rcvdOnDiscovery = rcvdOnDiscovery;
    }

    /**
     * @return ID of node provided cache configuration in discovery data.
     */
    @Nullable public UUID receivedFrom() {
        return rcvdFrom;
    }

    /**
     * @return Topology version when node provided cache configuration was started.
     */
    @Nullable AffinityTopologyVersion receivedFromStartVersion() {
        return rcvdFromVer;
    }

    /**
     * @param rcvdFromVer Topology version when node provided cache configuration was started.
     */
    void receivedFromStartVersion(AffinityTopologyVersion rcvdFromVer) {
        this.rcvdFromVer = rcvdFromVer;
    }

    /**
     * @return Start topology version or {@code null} if cache configured statically.
     */
    @Nullable public AffinityTopologyVersion startTopologyVersion() {
        return startTopVer;
    }

    /**
     * @param startTopVer Start topology version.
     */
    public void startTopologyVersion(AffinityTopologyVersion startTopVer) {
        this.startTopVer = startTopVer;
    }

    /**
     * @return Version when client cache on local node was started.
     */
    @Nullable AffinityTopologyVersion clientCacheStartVersion() {
        return clientCacheStartVer;
    }

    /**
     * @param clientCacheStartVer Version when client cache on local node was started.
     */
    void clientCacheStartVersion(AffinityTopologyVersion clientCacheStartVer) {
        this.clientCacheStartVer = clientCacheStartVer;
    }

    /**
     * @return Schema.
     */
    public QuerySchema schema() {
        synchronized (schemaMux) {
            return schema.copy();
        }
    }

    /**
     * Set schema
     *
     * @param schema Schema.
     */
    public void schema(QuerySchema schema) {
        assert schema != null;

        synchronized (schemaMux) {
            this.schema = schema.copy();
        }
    }

    /**
     * Try applying finish message.
     *
     * @param msg Message.
     */
    public void schemaChangeFinish(SchemaFinishDiscoveryMessage msg) {
        synchronized (schemaMux) {
            schema.finish(msg);

            if (msg.operation() instanceof SchemaAddQueryEntityOperation) {
                cacheCfg = GridCacheUtils.patchCacheConfiguration(cacheCfg,
                        (SchemaAddQueryEntityOperation)msg.operation());
            }
        }
    }

    /**
     * Make schema patch for this cache.
     *
     * @param cacheData Stored cache by which current schema should be expanded.
     * @return Patch which contains operations for expanding schema of this cache.
     * @see QuerySchemaPatch
     */
    public QuerySchemaPatch makeSchemaPatch(StoredCacheData cacheData) {
        synchronized (schemaMux) {
            return schema.makePatch(cacheData.config(), cacheData.queryEntities());
        }
    }

    /**
     * Make schema patch for this cache.
     *
     * @param target Query entity list which current schema should be expanded to.
     * @return Patch which contains operations for expanding schema of this cache.
     * @see QuerySchemaPatch
     */
    public QuerySchemaPatch makeSchemaPatch(Collection<QueryEntity> target) {
        synchronized (schemaMux) {
            return schema.makePatch(target);
        }
    }

    /**
     * Apply query schema patch for changing current schema.
     *
     * @param patch patch to apply.
     * @return {@code true} if applying was success and {@code false} otherwise.
     */
    public boolean applySchemaPatch(QuerySchemaPatch patch) {
        synchronized (schemaMux) {
            boolean res = schema.applyPatch(patch);

            if (res) {
                for (SchemaAbstractOperation op: patch.getPatchOperations()) {
                    if (op instanceof SchemaAddQueryEntityOperation)
                        cacheCfg = GridCacheUtils.patchCacheConfiguration(cacheCfg, (SchemaAddQueryEntityOperation)op);
                }
            }

            return res;
        }
    }

    /**
     * Form a {@link StoredCacheData} with all data to correctly restore cache params when its configuration is read
     * from page store. Essentially, this method takes from {@link DynamicCacheDescriptor} all that's needed to start
     * cache correctly, leaving out everything else.
     */
    public StoredCacheData toStoredData(CacheConfigurationSplitter splitter) {
        assert schema != null;

        StoredCacheData res = new StoredCacheData(cacheConfiguration());

        res.queryEntities(schema().entities());
        res.sql(sql());

        if (isConfigurationEnriched()) {
            T2<CacheConfiguration, CacheConfigurationEnrichment> splitCfg = splitter.split(cacheCfg);

            res.config(splitCfg.get1());

            // If original enrichment is present, it should be written instead of result of split.
            res.cacheConfigurationEnrichment(cacheCfgEnrichment == null ? splitCfg.get2() : cacheCfgEnrichment);
        }
        else
            res.cacheConfigurationEnrichment(cacheCfgEnrichment);

        return res;
    }

    /**
     * @return Cache configuration enrichment.
     */
    public CacheConfigurationEnrichment cacheConfigurationEnrichment() {
        return cacheCfgEnrichment;
    }

    /**
     * @return {@code True} if configuration is already enriched.
     */
    public boolean isConfigurationEnriched() {
        return cacheCfgEnrichment == null || cacheCfgEnriched;
    }

    /**
     * @param cacheCfgEnriched Flag indicates that configuration is enriched.
     */
    public void configurationEnriched(boolean cacheCfgEnriched) {
        this.cacheCfgEnriched = cacheCfgEnriched;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(DynamicCacheDescriptor.class, this, "cacheName", U.maskName(cacheCfg.getName()));
    }
}
