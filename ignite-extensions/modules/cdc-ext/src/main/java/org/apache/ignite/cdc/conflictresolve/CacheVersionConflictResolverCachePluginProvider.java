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

import javax.cache.Cache;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.processors.cache.CacheConflictResolutionManager;
import org.apache.ignite.internal.processors.cache.version.CacheVersionConflictResolver;
import org.apache.ignite.plugin.CachePluginConfiguration;
import org.apache.ignite.plugin.CachePluginProvider;
import org.jetbrains.annotations.Nullable;

/**
 * Intermediate component to provide {@link CacheConflictResolutionManagerImpl} for specific cache.
 *
 * @see CacheConflictResolutionManagerImpl
 * @see CacheVersionConflictResolverImpl
 * @see CacheVersionConflictResolver
 */
public class CacheVersionConflictResolverCachePluginProvider<K, V, C extends CachePluginConfiguration<K, V>>
    implements CachePluginProvider<C> {
    /**
     * Field for conflict resolve.
     * Value of this field will be used to compare two entries in case of conflicting changes.
     * Note, values of this field must implement {@link Comparable}.
     *
     * @see CacheVersionConflictResolverImpl
     */
    private final String conflictResolveField;

    /** Cluster Id. */
    private final byte clusterId;

    /**
     * @param conflictResolveField Field to resolve conflicts.
     * @param clusterId Cluster ID.
     */
    public CacheVersionConflictResolverCachePluginProvider(String conflictResolveField, byte clusterId) {
        this.conflictResolveField = conflictResolveField;
        this.clusterId = clusterId;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Nullable @Override public <T> T createComponent(Class<T> cls) {
        if (cls.equals(CacheConflictResolutionManager.class))
            return (T)new CacheConflictResolutionManagerImpl<>(conflictResolveField, clusterId);

        return null;
    }

    /** {@inheritDoc} */
    @Override public void start() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void stop(boolean cancel) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void onIgniteStart() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void onIgniteStop(boolean cancel) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void validate() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void validateRemote(CacheConfiguration locCfg, CacheConfiguration rmtCfg, ClusterNode rmtNode) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Nullable @Override public <T, K2, V2> T unwrapCacheEntry(Cache.Entry<K2, V2> entry, Class<T> cls) {
        return null;
    }
}
