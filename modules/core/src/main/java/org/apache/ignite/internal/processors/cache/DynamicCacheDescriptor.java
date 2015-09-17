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

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.plugin.CachePluginManager;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;

/**
 * Cache start descriptor.
 */
public class DynamicCacheDescriptor {
    /** Cache start ID. */
    private IgniteUuid deploymentId;

    /** Cache configuration. */
    @GridToStringExclude
    private CacheConfiguration cacheCfg;

    /** Cancelled flag. */
    private boolean cancelled;

    /** Locally configured flag. */
    private boolean locCfg;

    /** Statically configured flag. */
    private boolean staticCfg;

    /** Started flag. */
    private boolean started;

    /** Cache type. */
    private CacheType cacheType;

    /** */
    private volatile Map<UUID, CacheConfiguration> rmtCfgs;

    /** Template configuration flag. */
    private boolean template;

    /** Cache plugin manager. */
    private final CachePluginManager pluginMgr;

    /** */
    private boolean updatesAllowed = true;

    /**
     * @param ctx Context.
     * @param cacheCfg Cache configuration.
     * @param cacheType Cache type.
     * @param template {@code True} if this is template configuration.
     * @param deploymentId Deployment ID.
     */
    public DynamicCacheDescriptor(GridKernalContext ctx,
        CacheConfiguration cacheCfg,
        CacheType cacheType,
        boolean template,
        IgniteUuid deploymentId) {
        this.cacheCfg = cacheCfg;
        this.cacheType = cacheType;
        this.template = template;
        this.deploymentId = deploymentId;

        pluginMgr = new CachePluginManager(ctx, cacheCfg);
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
     * @param deploymentId Deployment ID.
     */
    public void deploymentId(IgniteUuid deploymentId) {
        this.deploymentId = deploymentId;
    }

    /**
     * @return Locally configured flag.
     */
    public boolean locallyConfigured() {
        return locCfg;
    }

    /**
     * @param locCfg Locally configured flag.
     */
    public void locallyConfigured(boolean locCfg) {
        this.locCfg = locCfg;
    }

    /**
     * @return {@code True} if statically configured.
     */
    public boolean staticallyConfigured() {
        return staticCfg;
    }

    /**
     * @param staticCfg {@code True} if statically configured.
     */
    public void staticallyConfigured(boolean staticCfg) {
        this.staticCfg = staticCfg;
    }

    /**
     * @return {@code True} if started flag was flipped by this call.
     */
    public boolean onStart() {
        if (!started) {
            started = true;

            return true;
        }

        return false;
    }

    /**
     * @return Cache configuration.
     */
    public CacheConfiguration cacheConfiguration() {
        return cacheCfg;
    }

    /**
     * @return Cache plugin manager.
     */
    public CachePluginManager pluginManager() {
        return pluginMgr;
    }

    /**
     * @param nodeId Remote node ID.
     * @return Configuration.
     */
    public CacheConfiguration remoteConfiguration(UUID nodeId) {
        Map<UUID, CacheConfiguration> cfgs = rmtCfgs;

        return cfgs == null ? null : cfgs.get(nodeId);
    }

    /**
     * @param nodeId Remote node ID.
     * @param cfg Remote node configuration.
     */
    public void addRemoteConfiguration(UUID nodeId, CacheConfiguration cfg) {
        Map<UUID, CacheConfiguration> cfgs = rmtCfgs;

        if (cfgs == null)
            rmtCfgs = cfgs = new HashMap<>();

        cfgs.put(nodeId, cfg);
    }

    /**
     *
     */
    public void clearRemoteConfigurations() {
        rmtCfgs = null;
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

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(DynamicCacheDescriptor.class, this, "cacheName", U.maskName(cacheCfg.getName()));
    }
}
