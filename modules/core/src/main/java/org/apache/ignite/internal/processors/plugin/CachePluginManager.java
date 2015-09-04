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

package org.apache.ignite.internal.processors.plugin;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.GridCachePluginContext;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.cache.CacheConflictResolutionManager;
import org.apache.ignite.internal.processors.cache.CacheOsConflictResolutionManager;
import org.apache.ignite.internal.processors.cache.GridCacheManagerAdapter;
import org.apache.ignite.internal.processors.cache.dr.GridCacheDrManager;
import org.apache.ignite.internal.processors.cache.dr.GridOsCacheDrManager;
import org.apache.ignite.internal.processors.cache.store.CacheOsStoreManager;
import org.apache.ignite.internal.processors.cache.store.CacheStoreManager;
import org.apache.ignite.plugin.CachePluginConfiguration;
import org.apache.ignite.plugin.CachePluginContext;
import org.apache.ignite.plugin.CachePluginProvider;

/**
 * Cache plugin manager.
 */
public class CachePluginManager extends GridCacheManagerAdapter {
    /** Providers list. To have providers order. */
    private final List<CachePluginProvider> providersList = new ArrayList<>();

    /** */
    private final Map<CachePluginContext, CachePluginProvider> providersMap = new HashMap<>();

    /** */
    private final GridKernalContext ctx;

    /** */
    private final CacheConfiguration cfg;

    /**
     * @param ctx Context.
     * @param cfg Cache config.
     */
    public CachePluginManager(GridKernalContext ctx, CacheConfiguration cfg) {
        this.ctx = ctx;
        this.cfg = cfg;
        
        if (cfg.getPluginConfigurations() != null) {
            for (CachePluginConfiguration cachePluginCfg : cfg.getPluginConfigurations()) {
                CachePluginContext pluginCtx = new GridCachePluginContext(ctx, cfg, cachePluginCfg);

                CachePluginProvider provider = cachePluginCfg.createProvider(pluginCtx);

                providersList.add(provider);
                providersMap.put(pluginCtx, provider);
            }
        }
    }

    /** {@inheritDoc} */
    @Override protected void onKernalStart0() throws IgniteCheckedException {
        for (CachePluginProvider provider : providersList)
            provider.onIgniteStart();
    }

    /** {@inheritDoc} */
    @Override protected void onKernalStop0(boolean cancel) {
        for (ListIterator<CachePluginProvider> iter = providersList.listIterator(); iter.hasPrevious();)
            iter.previous().onIgniteStop(cancel);
    }

    /** {@inheritDoc} */
    @Override protected void start0() throws IgniteCheckedException {
        for (CachePluginProvider provider : providersList)
            provider.start();
    }

    /** {@inheritDoc} */
    @Override protected void stop0(boolean cancel) {
        for (ListIterator<CachePluginProvider> iter = providersList.listIterator(); iter.hasPrevious();)
            iter.previous().stop(cancel);
    }

    /**
     * Creates optional component.
     *
     * @param cls Component class.
     * @return Created component.
     */
    @SuppressWarnings("unchecked")
    public <T> T createComponent(Class<T> cls) {
        for (CachePluginProvider provider : providersList) {
            T res = (T)provider.createComponent(cls);
            
            if (res != null)
                return res;
        }
        
        if (cls.equals(GridCacheDrManager.class))
            return (T)new GridOsCacheDrManager();
        else if (cls.equals(CacheConflictResolutionManager.class)) {
            T cmp = (T)ctx.createComponent(CacheConflictResolutionManager.class);

            if (cmp != null)
                return cmp;
            else
                return (T)new CacheOsConflictResolutionManager<>();
        }
        else if (cls.equals(CacheStoreManager.class))
            return (T)new CacheOsStoreManager(ctx, cfg);

        throw new IgniteException("Unsupported component type: " + cls);
    }

    /**
     * Validates cache plugin configurations. Throw exception if validation failed.
     *
     * @throws IgniteCheckedException If validation failed.
     */
    public void validate() throws IgniteCheckedException {
        for (CachePluginProvider provider : providersList)
            provider.validate();
    }

    /**
     * Checks that remote caches has configuration compatible with the local.
     *    
     * @param rmtCfg Remote cache configuration.
     * @param rmtNode Remote rmtNode.
     * @throws IgniteCheckedException If failed.
     */
    @SuppressWarnings("unchecked")
    public void validateRemotes(CacheConfiguration rmtCfg, ClusterNode rmtNode) throws IgniteCheckedException {
        for (Map.Entry<CachePluginContext, CachePluginProvider> entry : providersMap.entrySet()) {
            CachePluginContext cctx = entry.getKey();
            CachePluginProvider provider = entry.getValue();
            
            provider.validateRemote(cctx.igniteCacheConfiguration(), cctx.cacheConfiguration(), rmtCfg, rmtNode);
        }
    }
}