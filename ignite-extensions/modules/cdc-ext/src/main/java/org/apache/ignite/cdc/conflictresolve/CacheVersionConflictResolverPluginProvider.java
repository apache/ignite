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

package org.apache.ignite.cdc.conflictresolve;

import java.io.Serializable;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.version.CacheVersionConflictResolver;
import org.apache.ignite.plugin.CachePluginContext;
import org.apache.ignite.plugin.CachePluginProvider;
import org.apache.ignite.plugin.ExtensionRegistry;
import org.apache.ignite.plugin.IgnitePlugin;
import org.apache.ignite.plugin.PluginConfiguration;
import org.apache.ignite.plugin.PluginContext;
import org.apache.ignite.plugin.PluginProvider;
import org.apache.ignite.plugin.PluginValidationException;
import org.jetbrains.annotations.Nullable;

/**
 * Plugin to enable {@link CacheVersionConflictResolverImpl} for provided caches.
 *
 * @see CacheVersionConflictResolverImpl
 * @see CacheVersionConflictResolver
 */
public class CacheVersionConflictResolverPluginProvider<C extends PluginConfiguration> implements PluginProvider<C> {
    /** Cluster id. */
    private byte clusterId;

    /** Cache names. */
    private Set<String> caches;

    /** Plugin name. */
    private String name = "Cache version conflict resolver";

    /**
     * Field for conflict resolve.
     * Value of this field will be used to compare two entries in case of conflicting changes.
     * Note, values of this field must implement {@link Comparable}.
     *
     * @see CacheVersionConflictResolverImpl
     */
    private String conflictResolveField;

    /** Cache plugin provider. */
    private CachePluginProvider<?> provider;

    /** Log. */
    private IgniteLogger log;

    /** */
    public CacheVersionConflictResolverPluginProvider() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return name + " [clusterId=" + clusterId + ", conflictResolveField=" + conflictResolveField + ", caches=" + caches + ']';
    }

    /** {@inheritDoc} */
    @Override public String version() {
        return "1.0.0-SNAPSHOT";
    }

    /** {@inheritDoc} */
    @Override public String copyright() {
        return "Apache Software Foundation";
    }

    /** {@inheritDoc} */
    @Override public void initExtensions(PluginContext ctx, ExtensionRegistry registry) {
        this.log = ctx.log(CacheVersionConflictResolverPluginProvider.class);
        this.provider = new CacheVersionConflictResolverCachePluginProvider<>(conflictResolveField, clusterId);
    }

    /** {@inheritDoc} */
    @Override public CachePluginProvider createCacheProvider(CachePluginContext ctx) {
        String cacheName = ctx.igniteCacheConfiguration().getName();

        if (caches.contains(cacheName)) {
            log.info("ConflictResolver provider set for cache [cacheName=" + cacheName + ']');

            return provider;
        }

        log.info("Skip ConflictResolver provider for cache [cacheName=" + cacheName + ']');

        return null;
    }

    /** {@inheritDoc} */
    @Override public void onIgniteStart() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public IgnitePlugin plugin() {
        return new IgnitePlugin() { /* No-op. */ };
    }

    /** @param clusterId Data center ID. */
    public void setClusterId(byte clusterId) {
        this.clusterId = clusterId;
    }

    /** @param caches Caches to replicate. */
    public void setCaches(Set<String> caches) {
        this.caches = caches;
    }

    /** @param conflictResolveField Field to resolve conflicts. */
    public void setConflictResolveField(String conflictResolveField) {
        this.conflictResolveField = conflictResolveField;
    }

    /** @param name Plugin name. */
    public void setName(String name) {
        this.name = name;
    }

    /** {@inheritDoc} */
    @Override public void start(PluginContext ctx) {
        ((IgniteEx)ctx.grid()).context().cache().context().versions().dataCenterId(clusterId);
    }

    /** {@inheritDoc} */
    @Override public void stop(boolean cancel) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void onIgniteStop(boolean cancel) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public @Nullable Serializable provideDiscoveryData(UUID nodeId) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void receiveDiscoveryData(UUID nodeId, Serializable data) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void validateNewNode(ClusterNode node) throws PluginValidationException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Nullable @Override public <T> T createComponent(PluginContext ctx, Class<T> cls) {
        return null;
    }
}
