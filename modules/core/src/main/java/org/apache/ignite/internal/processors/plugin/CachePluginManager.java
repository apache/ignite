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

import org.apache.ignite.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.processors.cache.dr.*;
import org.apache.ignite.internal.processors.cache.store.*;
import org.apache.ignite.plugin.*;

import java.util.*;

/**
 * Cache plugin manager.
 */
public class CachePluginManager extends GridCacheManagerAdapter {
    /** */
    private final List<CachePluginProvider> providers = new ArrayList<>();

    /**
     * @param ctx Context.
     * @param cfg Cache config.
     */
    public CachePluginManager(GridKernalContext ctx, CacheConfiguration cfg) {
        if (cfg.getPluginConfigurations() != null) {
            for (CachePluginConfiguration cachePluginCfg : cfg.getPluginConfigurations()) {
                CachePluginContext pluginCtx = new GridCachePluginContext(ctx, cfg, cachePluginCfg);

                CachePluginProvider provider = cachePluginCfg.createProvider(pluginCtx);

                providers.add(provider);
            }
        }
    }

    /** {@inheritDoc} */
    @Override protected void onKernalStart0() throws IgniteCheckedException {
        for (CachePluginProvider provider : providers)
            provider.onIgniteStart();
    }

    /** {@inheritDoc} */
    @Override protected void onKernalStop0(boolean cancel) {
        for (ListIterator<CachePluginProvider> iter = providers.listIterator(); iter.hasPrevious();)
            iter.previous().onIgniteStop(cancel);
    }

    /** {@inheritDoc} */
    @Override protected void start0() throws IgniteCheckedException {
        for (CachePluginProvider provider : providers)
            provider.start();
    }

    /** {@inheritDoc} */
    @Override protected void stop0(boolean cancel) {
        for (ListIterator<CachePluginProvider> iter = providers.listIterator(); iter.hasPrevious();)
            iter.previous().stop(cancel);
    }

    /**
     * Creates optional component.
     *
     * @param ctx Kernal context.
     * @param cfg Cache configuration.
     * @param cls Component class.
     * @return Created component.
     */
    @SuppressWarnings("unchecked")
    public <T> T createComponent(GridKernalContext ctx, CacheConfiguration cfg, Class<T> cls) {
        for (CachePluginProvider provider : providers) {
            T res = (T)provider.createComponent(cls);
            
            if (res != null)
                return res;
        }
        
        if (cls.equals(GridCacheDrManager.class))
            return (T)new GridOsCacheDrManager();
        else if (cls.equals(CacheConflictResolutionManager.class))
            return (T)new CacheOsConflictResolutionManager<>();
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
        for (CachePluginProvider provider : providers)
            provider.validate();
    }
}
