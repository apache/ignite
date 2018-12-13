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

package org.apache.ignite.internal.processors.cache.transactions;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTopologyFuture;
import org.apache.ignite.internal.util.GridIntList;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_ASYNC;

/**
 *
 */
public abstract class IgniteTxRemoteStateAdapter implements IgniteTxRemoteState {
    /** Active cache IDs. */
    private GridIntList activeCacheIds = new GridIntList();

    /** {@inheritDoc} */
    @Override public boolean implicitSingle() {
        return false;
    }

    /** {@inheritDoc} */
    @Nullable @Override public Integer firstCacheId() {
        return activeCacheIds.isEmpty() ? null : activeCacheIds.get(0);
    }

    /** {@inheritDoc} */
    @Nullable @Override public GridIntList cacheIds() {
        return activeCacheIds;
    }

    /** {@inheritDoc} */
    @Override public void awaitLastFuture(GridCacheSharedContext cctx) {
        assert false;
    }

    /** {@inheritDoc} */
    @Override public IgniteCheckedException validateTopology(
        GridCacheSharedContext cctx,
        boolean read,
        GridDhtTopologyFuture topFut
    ) {
        assert false;

        return null;
    }

    /** {@inheritDoc} */
    @Override public CacheWriteSynchronizationMode syncMode(GridCacheSharedContext cctx) {
        assert false;

        return FULL_ASYNC;
    }

    /** {@inheritDoc} */
    @Override public void addActiveCache(GridCacheContext cctx, boolean recovery, IgniteTxAdapter tx)
        throws IgniteCheckedException {
        assert !tx.local();

        int cacheId = cctx.cacheId();

        // Check if we can enlist new cache to transaction.
        if (!activeCacheIds.contains(cacheId))
            activeCacheIds.add(cacheId);
    }

    /** {@inheritDoc} */
    @Override public GridDhtTopologyFuture topologyReadLock(GridCacheSharedContext cctx, GridFutureAdapter<?> fut) {
        assert false;

        return null;
    }

    /** {@inheritDoc} */
    @Override public void topologyReadUnlock(GridCacheSharedContext cctx) {
        assert false;
    }

    /** {@inheritDoc} */
    @Override public boolean storeWriteThrough(GridCacheSharedContext cctx) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean hasInterceptor(GridCacheSharedContext cctx) {
        return false;
    }

    /** {@inheritDoc} */
    @Nullable @Override public GridCacheContext singleCacheContext(GridCacheSharedContext cctx) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void onTxEnd(GridCacheSharedContext cctx, IgniteInternalTx tx, boolean commit) {
        assert false;
    }
}
