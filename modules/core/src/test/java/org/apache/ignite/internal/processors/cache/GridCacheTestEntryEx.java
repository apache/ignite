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

import java.util.Collection;
import java.util.Collections;
import java.util.UUID;
import javax.cache.Cache;
import javax.cache.expiry.ExpiryPolicy;
import javax.cache.processor.EntryProcessorResult;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.eviction.EvictableEntry;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxKey;
import org.apache.ignite.internal.processors.cache.version.CacheVersion;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersionedEntryEx;
import org.apache.ignite.internal.processors.dr.GridDrType;
import org.apache.ignite.internal.util.lang.GridMetadataAwareAdapter;
import org.apache.ignite.internal.util.lang.GridTuple3;
import org.apache.ignite.internal.util.typedef.T2;
import org.jetbrains.annotations.Nullable;

/**
 * Test entry.
 */
@SuppressWarnings("unchecked")
public class GridCacheTestEntryEx extends GridMetadataAwareAdapter implements GridCacheEntryEx {
    /** Key. */
    private KeyCacheObject key;

    /** Val. */
    private CacheObject val;

    /** TTL. */
    private long ttl;

    /** Version. */
    private CacheVersion ver = new GridCacheVersion(0, 0, 0, 1, 0);

    /** Obsolete version. */
    private CacheVersion obsoleteVer = ver;

    /** MVCC. */
    private GridCacheMvcc mvcc;

    /**
     * @param ctx Context.
     * @param key Key.
     */
    public GridCacheTestEntryEx(GridCacheContext ctx, Object key) {
        mvcc = new GridCacheMvcc(ctx);

        this.key = ctx.toCacheKeyObject(key);
    }

    /**
     * @param ctx Context.
     * @param key Key.
     * @param val Value.
     */
    public GridCacheTestEntryEx(GridCacheContext ctx, Object key, Object val) {
        mvcc = new GridCacheMvcc(ctx);

        this.key = ctx.toCacheKeyObject(key);
        this.val = ctx.toCacheObject(val);
    }

    /** {@inheritDoc} */
    @Override public int memorySize() throws IgniteCheckedException {
        return 1024;
    }

    /** {@inheritDoc} */
    @Override public boolean isInternal() {
        return key instanceof GridCacheInternal;
    }

    /** {@inheritDoc} */
    @Override public boolean isDht() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean isNear() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean isReplicated() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean isLocal() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean detached() {
        return false;
    }

    /** {@inheritDoc} */
    @Nullable @Override public GridCacheContext context() {
        return null;
    }

    /** {@inheritDoc} */
    @Nullable @Override public EvictableEntry wrapEviction() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public int partition() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public boolean partitionValid() {
        return true;
    }

    /**
     * @param threadId Thread ID.
     * @param ver Lock version.
     * @param timeout Lock acquisition timeout.
     * @param reenter Reentry flag ({@code true} if reentry is allowed).
     * @param tx Transaction flag.
     * @return New lock candidate if lock was added, or current owner if lock was reentered,
     *      or <tt>null</tt> if lock was owned by another thread and timeout is negative.
     */
    @Nullable public GridCacheMvccCandidate addLocal(
        long threadId,
        CacheVersion ver,
        long timeout,
        boolean reenter,
        boolean tx) {
        return mvcc.addLocal(
            this,
            threadId,
            ver,
            timeout,
            reenter,
            tx,
            false
        );
    }

    /**
     * Adds new lock candidate.
     *
     * @param nodeId Node ID.
     * @param threadId Thread ID.
     * @param ver Lock version.
     * @param timeout Lock acquire timeout.
     * @param ec Not used.
     * @param tx Transaction flag.
     * @return Remote candidate.
     */
    public GridCacheMvccCandidate addRemote(UUID nodeId, long threadId, CacheVersion ver, long timeout,
        boolean ec, boolean tx) {
        return mvcc.addRemote(this, nodeId, null, threadId, ver, timeout, tx, true, false);
    }

    /**
     * Adds new lock candidate.
     *
     * @param nodeId Node ID.
     * @param threadId Thread ID.
     * @param ver Lock version.
     * @param timeout Lock acquire timeout.
     * @param tx Transaction flag.
     * @return Remote candidate.
     */
    public GridCacheMvccCandidate addNearLocal(UUID nodeId, long threadId, CacheVersion ver, long timeout,
        boolean tx) {
        return mvcc.addNearLocal(this, nodeId, null, threadId, ver, timeout, tx, true);
    }

    /**
     *
     * @param baseVer Base version.
     */
    public void salvageRemote(GridCacheVersion baseVer) {
        mvcc.salvageRemote(baseVer);
    }

    /**
     * Moves completed candidates right before the base one. Note that
     * if base is not found, then nothing happens and {@code false} is
     * returned.
     *
     * @param baseVer Base version.
     * @param committedVers Committed versions relative to base.
     * @param rolledbackVers Rolled back versions relative to base.
     * @return Lock owner.
     */
    @Nullable public GridCacheMvccCandidate orderCompleted(CacheVersion baseVer,
        Collection<CacheVersion> committedVers, Collection<CacheVersion> rolledbackVers) {
        return mvcc.orderCompleted(baseVer, committedVers, rolledbackVers);
    }

    /**
     * @param ver Version.
     */
    public void doneRemote(CacheVersion ver) {
        mvcc.doneRemote(ver, Collections.<CacheVersion>emptyList(),
            Collections.<CacheVersion>emptyList(), Collections.<CacheVersion>emptyList());
    }

    /**
     * @param baseVer Base version.
     * @param owned Owned.
     */
    public void orderOwned(CacheVersion baseVer, CacheVersion owned) {
        mvcc.markOwned(baseVer, owned);
    }

    /**
     * @param ver Lock version to acquire or set to ready.
     * @return Current owner.
     */
    @Nullable public GridCacheMvccCandidate readyLocal(CacheVersion ver) {
        return mvcc.readyLocal(ver);
    }

    /**
     * @param ver Ready near lock version.
     * @param mapped Mapped version.
     * @param committedVers Committed versions.
     * @param rolledbackVers Rolled back versions.
     * @param pending Pending versions.
     * @return Lock owner.
     */
    @Nullable public GridCacheMvccCandidate readyNearLocal(CacheVersion ver, CacheVersion mapped,
        Collection<CacheVersion> committedVers, Collection<CacheVersion> rolledbackVers,
        Collection<CacheVersion> pending) {
        return mvcc.readyNearLocal(ver, mapped, committedVers, rolledbackVers, pending);
    }

    /**
     * @param cand Candidate to set to ready.
     * @return Current owner.
     */
    @Nullable public GridCacheMvccCandidate readyLocal(GridCacheMvccCandidate cand) {
        return mvcc.readyLocal(cand);
    }

    /**
     * Local local release.
     * @return Removed lock candidate or <tt>null</tt> if candidate was not removed.
     */
    @Nullable public GridCacheMvccCandidate releaseLocal() {
        return releaseLocal(Thread.currentThread().getId());
    }

    /**
     * Local release.
     *
     * @param threadId ID of the thread.
     * @return Current owner.
     */
    @Nullable public GridCacheMvccCandidate releaseLocal(long threadId) {
        return mvcc.releaseLocal(threadId);
    }

    /**
     *
     */
    public void recheckLock() {
        mvcc.recheck();
    }

    /** {@inheritDoc} */
    @Override public GridCacheEntryInfo info() {
        GridCacheEntryInfo info = new GridCacheEntryInfo();

        info.key(key());
        info.value(val);
        info.ttl(ttl());
        info.expireTime(expireTime());
        info.version(version());

        return info;
    }

    /** {@inheritDoc} */
    @Override public boolean valid(AffinityTopologyVersion topVer) {
        return true;
    }

    /** @inheritDoc */
    @Override public KeyCacheObject key() {
        return key;
    }

    /** {@inheritDoc} */
    @Override public IgniteTxKey txKey() {
        return new IgniteTxKey(key, 0);
    }

    /** @inheritDoc */
    @Override public CacheObject rawGet() {
        return val;
    }

    /** {@inheritDoc} */
    @Override public CacheObject rawGetOrUnmarshal(boolean tmp) throws IgniteCheckedException {
        return val;
    }

    /** {@inheritDoc} */
    @Override public boolean hasValue() {
        return val != null;
    }

    /** @inheritDoc */
    @Override public CacheObject rawPut(CacheObject val, long ttl) {
        CacheObject old = this.val;

        this.ttl = ttl;
        this.val = val;

        return old;
    }

    /** @inheritDoc */
    @Override public Cache.Entry wrap() {
        assert false;

        return null;
    }

    /** @inheritDoc */
    @Override public Cache.Entry wrapLazyValue() {
        assert false;

        return null;
    }

    /** {@inheritDoc} */
    @Override public CacheEntryImplEx wrapVersioned() {
        assert false;

        return null;
    }

    /** @inheritDoc */
    @Nullable @Override public CacheObject peekVisibleValue() {
        assert false;

        return null;
    }

    /** @inheritDoc */
    @Override public CacheVersion obsoleteVersion() {
        return obsoleteVer;
    }

    /** @inheritDoc */
    @Override public boolean obsolete() {
        return obsoleteVer != null;
    }

    /** {@inheritDoc} */
    @Override public boolean obsolete(CacheVersion exclude) {
        return obsoleteVer != null && !obsoleteVer.equals(exclude);
    }

    /** @inheritDoc */
    @Override public boolean invalidate(@Nullable CacheVersion curVer, CacheVersion newVer)
        throws IgniteCheckedException {
        assert false;

        return false;
    }

    /** @inheritDoc */
    @Override public boolean invalidate(@Nullable CacheEntryPredicate[] filter)
        throws GridCacheEntryRemovedException, IgniteCheckedException {
        assert false;

        return false;
    }

    /** @inheritDoc */
    @Override public boolean evictInternal(boolean swap, CacheVersion obsoleteVer,
        @Nullable CacheEntryPredicate[] filter) {
        assert false;

        return false;
    }

    /** {@inheritDoc} */
    @Override public GridCacheBatchSwapEntry evictInBatchInternal(CacheVersion obsoleteVer)
        throws IgniteCheckedException {
        assert false;

        return null;
    }

    /** @inheritDoc */
    @Override public boolean isNew() {
        assert false; return false;
    }

    /** {@inheritDoc} */
    @Override public boolean isNewLocked() throws GridCacheEntryRemovedException {
        assert false; return false;
    }

    /** @inheritDoc */
    @Override public CacheObject innerGet(@Nullable IgniteInternalTx tx,
        boolean readSwap,
        boolean readThrough,
        boolean failFast,
        boolean unmarshal,
        boolean updateMetrics,
        boolean evt,
        boolean tmp,
        UUID subjId,
        Object transformClo,
        String taskName,
        @Nullable IgniteCacheExpiryPolicy expiryPlc,
        boolean keepBinary) {
        return val;
    }

    /** @inheritDoc */
    @Nullable @Override public T2<CacheObject, CacheVersion> innerGetVersioned(
        IgniteInternalTx tx,
        boolean readSwap,
        boolean unmarshal,
        boolean updateMetrics,
        boolean evt,
        UUID subjId,
        Object transformClo,
        String taskName,
        @Nullable IgniteCacheExpiryPolicy expiryPlc,
        boolean keepBinary) {
        assert false;

        return null;
    }

    /** @inheritDoc */
    @Override public CacheObject innerReload() {
        return val;
    }

    /** @inheritDoc */
    @Override public GridCacheUpdateTxResult innerSet(@Nullable IgniteInternalTx tx,
        UUID evtNodeId,
        UUID affNodeId,
        @Nullable CacheObject val,
        boolean writeThrough,
        boolean retval,
        long ttl,
        boolean evt,
        boolean metrics,
        boolean keepBinary,
        AffinityTopologyVersion topVer,
        CacheEntryPredicate[] filter,
        GridDrType drType,
        long drExpireTime,
        @Nullable CacheVersion drVer,
        UUID subjId,
        String taskName,
        @Nullable CacheVersion dhtVer,
        @Nullable Long updateCntr)
        throws IgniteCheckedException, GridCacheEntryRemovedException {
        return new GridCacheUpdateTxResult(true, rawPut(val, ttl));
    }

    /** {@inheritDoc} */
    @Override public GridTuple3<Boolean, Object, EntryProcessorResult<Object>> innerUpdateLocal(
        CacheVersion ver,
        GridCacheOperation op,
        @Nullable Object writeObj,
        @Nullable Object[] invokeArgs,
        boolean writeThrough,
        boolean readThrough,
        boolean retval,
        boolean keepBinary,
        @Nullable ExpiryPolicy expiryPlc,
        boolean evt,
        boolean metrics,
        @Nullable CacheEntryPredicate[] filter,
        boolean intercept,
        UUID subjId,
        String taskName)
        throws IgniteCheckedException, GridCacheEntryRemovedException {
        return new GridTuple3<>(false, null, null);
    }

    /** {@inheritDoc} */
    @Override public GridCacheUpdateAtomicResult innerUpdate(
        CacheVersion ver,
        UUID evtNodeId,
        UUID affNodeId,
        GridCacheOperation op,
        @Nullable Object val,
        @Nullable Object[] invokeArgs,
        boolean writeThrough,
        boolean readThrough,
        boolean retval,
        boolean keepBinary,
        @Nullable IgniteCacheExpiryPolicy expiryPlc,
        boolean evt,
        boolean metrics,
        boolean primary,
        boolean checkVer,
        AffinityTopologyVersion topVer,
        @Nullable CacheEntryPredicate[] filter,
        GridDrType drType,
        long conflictTtl,
        long conflictExpireTime,
        @Nullable CacheVersion conflictVer,
        boolean conflictResolve,
        boolean intercept,
        UUID subjId,
        String taskName,
        @Nullable CacheObject prevVal,
        @Nullable Long updateCntr) throws IgniteCheckedException,
        GridCacheEntryRemovedException {
        assert false;

        return null;
    }

    /** @inheritDoc */
    @Override public GridCacheUpdateTxResult innerRemove(
        @Nullable IgniteInternalTx tx,
        UUID evtNodeId,
        UUID affNodeId,
        boolean retval,
        boolean evt,
        boolean metrics,
        boolean keepBinary,
        AffinityTopologyVersion topVer,
        CacheEntryPredicate[] filter,
        GridDrType drType,
        @Nullable CacheVersion drVer,
        UUID subjId,
        String taskName,
        @Nullable CacheVersion dhtVer,
        @Nullable Long updateCntr
        ) throws IgniteCheckedException, GridCacheEntryRemovedException {
        obsoleteVer = ver;

        CacheObject old = val;

        val = null;

        return new GridCacheUpdateTxResult(true, old);
    }

    /** @inheritDoc */
    @Override public boolean clear(CacheVersion ver, boolean readers) throws IgniteCheckedException {
        if (ver == null || ver.equals(this.ver)) {
            val = null;

            return true;
        }

        return false;
    }

    /** @inheritDoc */
    @Override public boolean tmLock(IgniteInternalTx tx,
        long timeout,
        @Nullable CacheVersion serOrder,
        CacheVersion serReadVer,
        boolean keepBinary) {
        assert false; return false;
    }

    /** @inheritDoc */
    @Override public void txUnlock(IgniteInternalTx tx) {
        assert false;
    }

    /** @inheritDoc */
    @Override public boolean removeLock(CacheVersion ver) {
        GridCacheMvccCandidate doomed = mvcc.candidate(ver);

        mvcc.remove(ver);

        return doomed != null;
    }

    /** @inheritDoc */
    @Override public boolean markObsolete(CacheVersion ver) {
        if (ver == null || ver.equals(obsoleteVer)) {
            obsoleteVer = ver;

            val = null;

            return true;
        }

        return false;
    }

    /** {@inheritDoc} */
    @Override public void onMarkedObsolete() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public boolean markObsoleteIfEmpty(CacheVersion ver) {
        if (val == null)
            obsoleteVer = ver;

        return obsoleteVer != null;
    }

    /** {@inheritDoc} */
    @Override public boolean markObsoleteVersion(CacheVersion ver) {
        if (this.ver.equals(ver)) {
            obsoleteVer = ver;

            return true;
        }

        return false;
    }

    /** @inheritDoc */
    @Override public CacheVersion version() {
        return ver;
    }

    /** @inheritDoc */
    @Override public boolean checkSerializableReadVersion(CacheVersion ver) {
        assert false;

        return false;
    }

    /** @inheritDoc */
    @Override public boolean initialValue(
        CacheObject val,
        CacheVersion ver,
        long ttl,
        long expireTime,
        boolean preload,
        AffinityTopologyVersion topVer,
        GridDrType drType
    ) throws IgniteCheckedException, GridCacheEntryRemovedException {
        assert false;

        return false;
    }

    /** @inheritDoc */
    @Override public boolean initialValue(KeyCacheObject key, GridCacheSwapEntry unswapped) {
        assert false;

        return false;
    }

    /** @inheritDoc */
    @Override public GridCacheVersionedEntryEx versionedEntry(final boolean keepBinary) throws IgniteCheckedException {
        return null;
    }

    /** @inheritDoc */
    @Override public CacheVersion versionedValue(CacheObject val,
        CacheVersion curVer,
        CacheVersion newVer) {
        assert false;

        return null;
    }

    /** @inheritDoc */
    @Override public boolean hasLockCandidate(CacheVersion ver) {
        return mvcc.hasCandidate(ver);
    }

    /** @inheritDoc */
    @Override public boolean lockedByAny(CacheVersion... exclude) {
        return !mvcc.isEmpty(exclude);
    }

    /** @inheritDoc */
    @Override public boolean lockedByThread()  {
        return lockedByThread(Thread.currentThread().getId());
    }

    /** @inheritDoc */
    @Override public boolean lockedLocally(CacheVersion lockVer) {
        return mvcc.isLocallyOwned(lockVer);
    }

    /** {@inheritDoc} */
    @Override public boolean lockedLocallyByIdOrThread(CacheVersion lockVer, long threadId)
        throws GridCacheEntryRemovedException {
        return lockedLocally(lockVer) || lockedByThread(threadId);
    }

    /** @inheritDoc */
    @Override public boolean lockedByThread(long threadId, CacheVersion exclude) {
        return mvcc.isLocallyOwnedByThread(threadId, false, exclude);
    }

    /** @inheritDoc */
    @Override public boolean lockedByThread(long threadId) {
        return mvcc.isLocallyOwnedByThread(threadId, true);
    }

    /** @inheritDoc */
    @Override public boolean lockedBy(CacheVersion ver) {
        return mvcc.isOwnedBy(ver);
    }

    /** @inheritDoc */
    @Override public boolean lockedByThreadUnsafe(long threadId) {
        return mvcc.isLocallyOwnedByThread(threadId, true);
    }

    /** @inheritDoc */
    @Override public boolean lockedByUnsafe(CacheVersion ver) {
        return mvcc.isOwnedBy(ver);
    }

    /** @inheritDoc */
    @Override public boolean lockedLocallyUnsafe(CacheVersion lockVer) {
        return mvcc.isLocallyOwned(lockVer);
    }

    /** @inheritDoc */
    @Override public boolean hasLockCandidateUnsafe(CacheVersion ver) {
        return mvcc.hasCandidate(ver);
    }

    /** @inheritDoc */
    @Override public Collection<GridCacheMvccCandidate> localCandidates(CacheVersion... exclude) {
        return mvcc.localCandidates(exclude);
    }

    /** @inheritDoc */
    public Collection<GridCacheMvccCandidate> localCandidates(boolean reentries, CacheVersion... exclude) {
        return mvcc.localCandidates(reentries, exclude);
    }

    /** @inheritDoc */
    @Override public Collection<GridCacheMvccCandidate> remoteMvccSnapshot(CacheVersion... exclude) {
        return mvcc.remoteCandidates(exclude);
    }

    /** {@inheritDoc} */
    @Override public GridCacheMvccCandidate localCandidate(long threadId) throws GridCacheEntryRemovedException {
        return mvcc.localCandidate(threadId);
    }

    /** @inheritDoc */
    @Override public GridCacheMvccCandidate candidate(CacheVersion ver) {
        return mvcc.candidate(ver);
    }

    /** {@inheritDoc} */
    @Override public GridCacheMvccCandidate candidate(UUID nodeId, long threadId)
        throws GridCacheEntryRemovedException {
        return mvcc.remoteCandidate(nodeId, threadId);
    }

    /**
     * @return Any MVCC owner.
     */
    public GridCacheMvccCandidate anyOwner() {
        return mvcc.anyOwner();
    }

    /** @inheritDoc */
    @Override public GridCacheMvccCandidate localOwner() {
        return mvcc.localOwner();
    }

    /** @inheritDoc */
    @Override public CacheObject valueBytes() {
        assert false;

        return null;
    }

    /** @inheritDoc */
    @Override public CacheObject valueBytes(CacheVersion ver) {
        assert false;

        return null;
    }

    /** {@inheritDoc} */
    @Override public long rawExpireTime() {
        return 0;
    }

    /** @inheritDoc */
    @Override public long expireTime() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public long expireTimeUnlocked() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public boolean onTtlExpired(CacheVersion obsoleteVer) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public long rawTtl() {
        return ttl;
    }

    /** @inheritDoc */
    @Override public long ttl() {
        return ttl;
    }

    /** @inheritDoc */
    @Override public void updateTtl(CacheVersion ver, long ttl) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public CacheObject unswap() throws IgniteCheckedException {
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean onOffheapEvict(byte[] vb, CacheVersion evictVer, CacheVersion obsoleteVer)
        throws IgniteCheckedException, GridCacheEntryRemovedException {
        return false;
    }

    /** {@inheritDoc} */
    @Override public CacheObject unswap(boolean needVal) throws IgniteCheckedException {
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean hasLockCandidate(long threadId) throws GridCacheEntryRemovedException {
        return localCandidate(threadId) != null;
    }

    /** {@inheritDoc} */
    @Override public boolean deleted() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean obsoleteOrDeleted() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public long startVersion() {
        return 0;
    }

    /** {@inheritDoc} */
    @Nullable @Override public CacheObject peek(boolean heap,
        boolean offheap,
        boolean swap,
        AffinityTopologyVersion topVer,
        @Nullable IgniteCacheExpiryPolicy plc)
    {
        return null;
    }

    /** {@inheritDoc} */
    @Nullable @Override public CacheObject peek(
        boolean heap,
        boolean offheap,
        boolean swap,
        @Nullable IgniteCacheExpiryPolicy plc)
        throws GridCacheEntryRemovedException, IgniteCheckedException {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void onUnlock() {
        // No-op.
    }
}
