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

package org.gridgain.grid.kernal.processors.cache.dr.os;

import org.apache.ignite.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.kernal.processors.cache.dr.*;
import org.gridgain.grid.kernal.processors.cache.transactions.*;
import org.gridgain.grid.kernal.processors.dr.*;
import org.jetbrains.annotations.*;

/**
 * No-op implementation for {@link GridCacheDrManager}.
 */
public class GridOsCacheDrManager<K, V> implements GridCacheDrManager<K, V> {
    /** {@inheritDoc} */
    @Override public boolean enabled() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public void start(GridCacheContext<K, V> cctx) throws IgniteCheckedException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void stop(boolean cancel) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void onKernalStart() throws IgniteCheckedException {
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
    @Override public byte dataCenterId() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public void replicate(K key,
        @Nullable byte[] keyBytes,
        @Nullable V val,
        @Nullable byte[] valBytes,
        long ttl,
        long expireTime,
        GridCacheVersion ver,
        GridDrType drType) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public GridDrResolveResult<V> resolveAtomic(GridCacheEntryEx<K, V> e,
        GridCacheOperation op,
        @Nullable Object writeObj,
        @Nullable byte[] valBytes,
        long ttl,
        long drTtl,
        long drExpireTime,
        @Nullable GridCacheVersion drVer) throws IgniteCheckedException, GridCacheEntryRemovedException {
        return null;
    }

    /** {@inheritDoc} */
    @Override public GridDrResolveResult<V> resolveTx(GridCacheEntryEx<K, V> e,
        IgniteTxEntry<K, V> txEntry,
        GridCacheVersion newVer,
        GridCacheOperation op,
        V newVal,
        byte[] newValBytes,
        long newTtl,
        long newDrExpireTime) throws IgniteCheckedException, GridCacheEntryRemovedException {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void beforeExchange(long topVer, boolean left) throws IgniteCheckedException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void partitionEvicted(int part) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void onReceiveCacheEntriesReceived(int entriesCnt) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void resetMetrics() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public boolean receiveEnabled() {
        return false;
    }
}
