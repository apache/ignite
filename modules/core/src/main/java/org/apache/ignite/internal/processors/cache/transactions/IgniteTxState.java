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

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTopologyFuture;
import org.apache.ignite.internal.processors.cache.mvcc.MvccCachingManager;
import org.apache.ignite.internal.processors.cache.store.CacheStoreManager;
import org.apache.ignite.internal.util.GridIntList;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public interface IgniteTxState {
    /**
     *
     * @return Flag indicating whether transaction is implicit with only one key.
     */
    public boolean implicitSingle();

    /**
     * @return First tx cache id.
     */
    @Nullable public Integer firstCacheId();

    /**
     * Gets caches ids affected with current tx.
     *
     * @return tx cache ids.
     */
    @Nullable public GridIntList cacheIds();

    /**
     * Unwind evicts for caches involved in this transaction.
     * @param cctx Grid cache shared context.
     */
    public void unwindEvicts(GridCacheSharedContext cctx);

    /**
     * @param cctx Context.
     * @return cctx Non-null cache context if tx has only one active cache.
     */
    @Nullable public GridCacheContext singleCacheContext(GridCacheSharedContext cctx);

    /**
     * @param cctx Awaits for previous async operations on active caches to be completed.
     */
    public void awaitLastFuture(GridCacheSharedContext cctx);

    /**
     * @param cctx Context.
     * @param read {@code True} if validating for a read operation, {@code false} for write.
     * @param topFut Topology future.
     * @return Error if validation failed.
     */
    public IgniteCheckedException validateTopology(
        GridCacheSharedContext cctx,
        boolean read,
        GridDhtTopologyFuture topFut);

    /**
     * @param cctx Context.
     * @return Write synchronization mode.
     */
    public CacheWriteSynchronizationMode syncMode(GridCacheSharedContext cctx);

    /**
     * @param cacheCtx Context.
     * @param tx Transaction.
     * @throws IgniteCheckedException If cache check failed.
     */
    public void addActiveCache(GridCacheContext cacheCtx, boolean recovery, IgniteTxAdapter tx)
        throws IgniteCheckedException;

    /**
     * @param cctx Context.
     * @param fut Future to finish with error if some cache is stopping.
     * @return Topology future.
     */
    public GridDhtTopologyFuture topologyReadLock(GridCacheSharedContext cctx, GridFutureAdapter<?> fut);

    /**
     * @param cctx Context.
     */
    public void topologyReadUnlock(GridCacheSharedContext cctx);

    /**
     * @param cctx Context.
     * @return {@code True} if transaction is allowed to use store and transactions spans one or more caches with
     *      store enabled.
     */
    public boolean storeWriteThrough(GridCacheSharedContext cctx);

    /**
     * @param cctx Context.
     * @return {@code True} if transaction spans one or more caches with configured interceptor.
     */
    public boolean hasInterceptor(GridCacheSharedContext cctx);

    /**
     * @param cctx Context.
     * @return Configured stores for active caches.
     */
    public Collection<CacheStoreManager> stores(GridCacheSharedContext cctx);

    /**
     * @param cctx Context.
     * @param tx Transaction.
     * @param commit Commit flag.
     */
    public void onTxEnd(GridCacheSharedContext cctx, IgniteInternalTx tx, boolean commit);

    /**
     * @param key Key.
     * @return Entry.
     */
    @Nullable public IgniteTxEntry entry(IgniteTxKey key);

    /**
     * @param key Key.
     * @return {@code True} if tx has write key.
     */
    public boolean hasWriteKey(IgniteTxKey key);

    /**
     * @return Read entries keys.
     */
    public Set<IgniteTxKey> readSet();

    /**
     * @return Write entries keys.
     */
    public Set<IgniteTxKey> writeSet();

    /**
     * @return Write entries.
     */
    public Collection<IgniteTxEntry> writeEntries();

    /**
     * @return Read entries.
     */
    public Collection<IgniteTxEntry> readEntries();

    /**
     * @return Write entries map.
     */
    public Map<IgniteTxKey, IgniteTxEntry> writeMap();

    /**
     * @return Read entries map.
     */
    public Map<IgniteTxKey, IgniteTxEntry> readMap();

    /**
     * @return All entries.
     */
    public Collection<IgniteTxEntry> allEntries();

    /**
     * @return Non-null entry if tx has only one write entry.
     */
    @Nullable public IgniteTxEntry singleWrite();

    /**
     * @return {@code True} if transaction is empty.
     */
    public boolean empty();

    /**
     * @return {@code True} if MVCC mode is enabled for transaction.
     */
    public boolean mvccEnabled();

    /**
     * @param cacheId Cache id.
     * @return {@code True} if it is need to store in the heap updates made by the current TX for the given cache.
     * These updates will be used for CQ and DR. See {@link MvccCachingManager}.
     */
    public boolean useMvccCaching(int cacheId);
}
