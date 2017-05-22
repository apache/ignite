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

import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cacheobject.IgniteCacheObjectProcessor;
import org.apache.ignite.internal.processors.plugin.CachePluginManager;
import org.apache.ignite.internal.processors.query.QuerySchema;
import org.apache.ignite.internal.processors.query.schema.message.SchemaFinishDiscoveryMessage;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
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
    private CacheConfiguration cacheCfg;

    /** Statically configured flag. */
    private final boolean staticCfg;

    /** Cache type. */
    private CacheType cacheType;

    /** Template configuration flag. */
    private boolean template;

    /** Cache plugin manager. */
    private final CachePluginManager pluginMgr;

    /** */
    private boolean updatesAllowed = true;

    /** */
    private Integer cacheId;

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

    /**
     * @param ctx Context.
     * @param cacheCfg Cache configuration.
     * @param cacheType Cache type.
     * @param template {@code True} if this is template configuration.
     * @param rcvdFrom ID of node provided cache configuration
     * @param staticCfg {@code True} if cache statically configured.
     * @param deploymentId Deployment ID.
     * @param schema Query schema.
     */
    @SuppressWarnings("unchecked")
    public DynamicCacheDescriptor(GridKernalContext ctx,
        CacheConfiguration cacheCfg,
        CacheType cacheType,
        boolean template,
        UUID rcvdFrom,
        boolean staticCfg,
        IgniteUuid deploymentId,
        QuerySchema schema) {
        assert cacheCfg != null;
        assert schema != null;

        if (cacheCfg.getCacheMode() == CacheMode.REPLICATED && cacheCfg.getNearConfiguration() != null) {
            cacheCfg = new CacheConfiguration(cacheCfg);

            cacheCfg.setNearConfiguration(null);
        }

        this.cacheCfg = cacheCfg;
        this.cacheType = cacheType;
        this.template = template;
        this.rcvdFrom = rcvdFrom;
        this.staticCfg = staticCfg;
        this.deploymentId = deploymentId;

        pluginMgr = new CachePluginManager(ctx, cacheCfg);

        cacheId = CU.cacheId(cacheCfg.getName());

        synchronized (schemaMux) {
            this.schema = schema.copy();
        }
    }

    /**
     * @return Cache ID.
     */
    public Integer cacheId() {
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
     * Creates and caches cache object context if needed.
     *
     * @param proc Object processor.
     * @return Cache object context.
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
     * @return Cache plugin manager.
     */
    public CachePluginManager pluginManager() {
        return pluginMgr;
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
     * @return Start topology version.
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
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(DynamicCacheDescriptor.class, this, "cacheName", U.maskName(cacheCfg.getName()));
    }
}
