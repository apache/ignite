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

package org.apache.ignite.cdc.conflictplugin;

import org.apache.ignite.internal.processors.cache.CacheConflictResolutionManager;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.version.CacheVersionConflictResolver;
import org.apache.ignite.lang.IgniteFuture;

/**
 * Intermediate component to provide {@link DrIdCacheVersionConflictResolver} for specific cache.
 *
 * @see DrIdCacheVersionConflictResolver
 * @see CacheVersionConflictResolver
 */
public class CDCCacheConflictResolutionManager<K, V> implements CacheConflictResolutionManager<K, V> {
    /**
     * Field for conflict resolve.
     * Value of this field will be used to compare two entries in case of conflicting changes.
     * Note, values of this field must implement {@link Comparable} interface.
     *
     * @see DrIdCacheVersionConflictResolver
     */
    private final String conflictResolveField;

    /** Grid cache context. */
    private GridCacheContext<K, V> cctx;

    /**
     * @param conflictResolveField Field to resolve conflicts.
     */
    public CDCCacheConflictResolutionManager(String conflictResolveField) {
        this.conflictResolveField = conflictResolveField;
    }

    /** {@inheritDoc} */
    @Override public CacheVersionConflictResolver conflictResolver() {
        return new DrIdCacheVersionConflictResolver(
            cctx.versions().dataCenterId(),
            conflictResolveField,
            cctx.logger(DrIdCacheVersionConflictResolver.class)
        );
    }

    /** {@inheritDoc} */
    @Override public void start(GridCacheContext<K, V> cctx) {
        this.cctx = cctx;
    }

    /** {@inheritDoc} */
    @Override public void stop(boolean cancel, boolean destroy) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void onKernalStart() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void onKernalStop(boolean cancel) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void printMemoryStats() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void onDisconnected(IgniteFuture reconnectFut) {
        // No-op.
    }
}
