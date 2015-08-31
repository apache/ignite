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

package org.apache.ignite.internal;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.plugin.CachePluginConfiguration;
import org.apache.ignite.plugin.CachePluginContext;

/**
 * Cache plugin context.
 */
public class GridCachePluginContext<C extends CachePluginConfiguration> implements CachePluginContext<C> {
    /** */
    private final GridKernalContext ctx;

    /** */
    private final CacheConfiguration igniteCacheCfg;

    /** */
    private final CachePluginConfiguration cachePluginCfg;

    /**
     * @param ctx Kernal context.
     * @param cachePluginCfg Cache plugin config.
     * @param igniteCacheCfg Ignite config.
     */
    public GridCachePluginContext(GridKernalContext ctx, CacheConfiguration igniteCacheCfg,
        CachePluginConfiguration cachePluginCfg) {
        this.ctx = ctx;
        this.cachePluginCfg = cachePluginCfg;
        this.igniteCacheCfg = igniteCacheCfg;
    }

    @Override public IgniteConfiguration igniteConfiguration() {
        return ctx.config();
    }

    /** {@inheritDoc} */
    @Override public C cacheConfiguration() {
        return (C)cachePluginCfg;
    }

    /** {@inheritDoc} */
    @Override public CacheConfiguration igniteCacheConfiguration() {
        return igniteCacheCfg;
    }

    /** {@inheritDoc} */
    @Override public Ignite grid() {        
        return ctx.grid();
    }
    
    /** {@inheritDoc} */
    @Override public ClusterNode localNode() {
        return ctx.discovery().localNode();
    }

    /** {@inheritDoc} */
    @Override public IgniteLogger log(Class<?> cls) {
        return ctx.log(cls);
    }
}