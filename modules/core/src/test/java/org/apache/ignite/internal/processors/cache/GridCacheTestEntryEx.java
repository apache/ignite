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
import java.util.List;
import java.util.UUID;
import javax.cache.Cache;
import javax.cache.expiry.ExpiryPolicy;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorResult;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.eviction.EvictableEntry;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxLocalAdapter;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridDhtAtomicAbstractUpdateFuture;
import org.apache.ignite.internal.processors.cache.mvcc.MvccSnapshot;
import org.apache.ignite.internal.processors.cache.mvcc.MvccVersion;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxKey;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersionedEntryEx;
import org.apache.ignite.internal.processors.dr.GridDrType;
import org.apache.ignite.internal.processors.query.schema.SchemaIndexCacheFilter;
import org.apache.ignite.internal.processors.query.schema.SchemaIndexCacheVisitorClosure;
import org.apache.ignite.internal.util.lang.GridMetadataAwareAdapter;
import org.apache.ignite.internal.util.lang.GridTuple3;
import org.apache.ignite.lang.IgniteUuid;
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
    private GridCacheVersion ver = new GridCacheVersion(0, 0, 1, 0);

    /** Obsolete version. */
    private GridCacheVersion obsoleteVer = ver;

    /** MVCC. */
    private GridCacheMvcc mvcc;

    /**
     * @param ctx Context.
     * @param key Key.
     */
    GridCacheTestEntryEx(GridCacheContext ctx, Object key) {
        mvcc = new GridCacheMvcc(ctx);

        this.key = ctx.toCacheKeyObject(key);
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
    @Override public boolean isMvcc() {
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
    @Nullable GridCacheMvccCandidate addLocal(
        long threadId,
        GridCacheVersion ver,
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
            false,
            false
        );
    }

    /**
     * Adds new lock candidate.
     *
     * @param nodeId Node ID.
     * @param threadId Thread ID.
     * @param ver Lock version.
     * @param tx Transaction flag.
     * @return Remote candidate.
     */
    GridCacheMvccCandidate addRemote(UUID nodeId, long threadId, GridCacheVersion ver,
                                     boolean tx) {
        return mvcc.addRemote(this, nodeId, null, threadId, ver, tx, true, false);
    }

    /**
     * Adds new lock candidate.
     *
     * @param nodeId Node ID.
     * @param threadId Thread ID.
     * @param ver Lock version.
     * @param tx Transaction flag.
     * @return Remote candidate.
     */
    GridCacheMvccCandidate addNearLocal(UUID nodeId, long threadId, GridCacheVersion ver,
        boolean tx) {
        return mvcc.addNearLocal(this, nodeId, null, threadId, ver, tx, true, false);
    }

    /**
     *
     * @param baseVer Base version.
     */
    void salvageRemote(GridCacheVersion baseVer) {
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
     */
    void orderCompleted(GridCacheVersion baseVer,
        Collection<GridCacheVersion> committedVers, Collection<GridCacheVersion> rolledbackVers) {
        mvcc.orderCompleted(baseVer, committedVers, rolledbackVers);
    }

    /**
     * @param ver Version.
     */
    void doneRemote(GridCacheVersion ver) {
        mvcc.doneRemote(ver, Collections.<GridCacheVersion>emptyList(),
            Collections.<GridCacheVersion>emptyList(), Collections.<GridCacheVersion>emptyList());
    }

    /**
     * @param baseVer Base version.
     * @param owned Owned.
     */
    void orderOwned(GridCacheVersion baseVer, GridCacheVersion owned) {
        mvcc.markOwned(baseVer, owned);
    }

    /**
     * @param ver Lock version to acquire or set to ready.
     */
    void readyLocal(GridCacheVersion ver) {
        mvcc.readyLocal(ver);
    }

    /**
     * @param ver Ready near lock version.
     * @param mapped Mapped version.
     * @param committedVers Committed versions.
     * @param rolledbackVers Rolled back versions.
     * @param pending Pending versions.
     */
    void readyNearLocal(GridCacheVersion ver, GridCacheVersion mapped,
        Collection<GridCacheVersion> committedVers, Collection<GridCacheVersion> rolledbackVers,
        Collection<GridCacheVersion> pending) {
        mvcc.readyNearLocal(ver, mapped, committedVers, rolledbackVers, pending);
    }

    /**
     * @param cand Candidate to set to ready.
     */
    void readyLocal(GridCacheMvccCandidate cand) {
        mvcc.readyLocal(cand);
    }

    /**
     * Local release.
     *
     * @param threadId ID of the thread.
     */
    void releaseLocal(long threadId) {
        mvcc.releaseLocal(threadId);
    }

    /**
     *
     */
    void recheckLock() {
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
    @Nullable @Override public List<GridCacheEntryInfo> allVersionsInfo()
        throws IgniteCheckedException {
        return null;
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
    @Override public Cache.Entry wrapLazyValue(boolean keepBinary) {
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
    @Override public GridCacheVersion obsoleteVersion() {
        return obsoleteVer;
    }

    /** @inheritDoc */
    @Override public boolean obsolete() {
        return obsoleteVer != null;
    }

    /** {@inheritDoc} */
    @Override public boolean obsolete(GridCacheVersion exclude) {
        return obsoleteVer != null && !obsoleteVer.equals(exclude);
    }

    /** @inheritDoc */
    @Override public boolean invalidate(GridCacheVersion newVer)
        throws IgniteCheckedException {
        assert false;

        return false;
    }

    /** @inheritDoc */
    @Override public boolean evictInternal(GridCacheVersion obsoleteVer,
        @Nullable CacheEntryPredicate[] filter, boolean evictOffheap) {
        assert false;

        return false;
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
    @Override public CacheObject innerGet(
        @Nullable GridCacheVersion ver,
        @Nullable IgniteInternalTx tx,
        boolean readThrough,
        boolean updateMetrics,
        boolean evt,
        UUID subjId,
        Object transformClo,
        String taskName,
        @Nullable IgniteCacheExpiryPolicy expiryPlc,
        boolean keepBinary,
        MvccSnapshot mvccVer) {
        return val;
    }

    /** @inheritDoc */
    @Override public void clearReserveForLoad(GridCacheVersion ver) {
        assert false;
    }

    /** @inheritDoc */
    @Override public EntryGetResult innerGetAndReserveForLoad(
        boolean updateMetrics,
        boolean evt,
        UUID subjId,
        String taskName,
        @Nullable IgniteCacheExpiryPolicy expiryPlc,
        boolean keepBinary,
        MvccSnapshot mvccVer,
        @Nullable ReaderArguments args) throws IgniteCheckedException, GridCacheEntryRemovedException {
        assert false;

        return null;
    }

    /** @inheritDoc */
    @Nullable @Override public EntryGetResult innerGetVersioned(
        @Nullable GridCacheVersion ver,
        IgniteInternalTx tx,
        boolean updateMetrics,
        boolean evt,
        UUID subjId,
        Object transformClo,
        String taskName,
        @Nullable IgniteCacheExpiryPolicy expiryPlc,
        boolean keepBinary,
        MvccSnapshot mvccVer,
        @Nullable ReaderArguments readerArgs) {
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
        boolean hasOldVal,
        @Nullable CacheObject oldVal,
        AffinityTopologyVersion topVer,
        CacheEntryPredicate[] filter,
        GridDrType drType,
        long drExpireTime,
        @Nullable GridCacheVersion drVer,
        UUID subjId,
        String taskName,
        @Nullable GridCacheVersion dhtVer,
        @Nullable Long updateCntr,
        MvccSnapshot mvccVer
    )
        throws IgniteCheckedException, GridCacheEntryRemovedException
    {
        rawPut(val, ttl);

        return new GridCacheUpdateTxResult(true);
    }

    /** {@inheritDoc} */
    @Override public GridCacheUpdateTxResult mvccSet(@Nullable IgniteInternalTx tx, UUID affNodeId, CacheObject val,
        EntryProcessor entryProc, Object[] invokeArgs, long ttl0, AffinityTopologyVersion topVer, MvccSnapshot mvccVer, GridCacheOperation op, boolean needHistory,
        boolean noCreate, boolean needOldVal, CacheEntryPredicate filter, boolean retVal)
        throws IgniteCheckedException, GridCacheEntryRemovedException {
        rawPut(val, ttl);

        return new GridCacheUpdateTxResult(true);
    }

    /** {@inheritDoc} */
    @Override public GridCacheUpdateTxResult mvccRemove(@Nullable IgniteInternalTx tx, UUID affNodeId,
        AffinityTopologyVersion topVer, MvccSnapshot mvccVer, boolean needHistory, boolean needOldVal,
        CacheEntryPredicate filter, boolean retVal)
        throws IgniteCheckedException, GridCacheEntryRemovedException {
        obsoleteVer = ver;

        val = null;

        return new GridCacheUpdateTxResult(true);
    }

    /** {@inheritDoc} */
    @Override public GridCacheUpdateTxResult mvccLock(GridDhtTxLocalAdapter tx,
        MvccSnapshot mvccVer) throws GridCacheEntryRemovedException, IgniteCheckedException {
        return new GridCacheUpdateTxResult(true);
    }

    /** {@inheritDoc} */
    @Override public GridTuple3<Boolean, Object, EntryProcessorResult<Object>> innerUpdateLocal(
        GridCacheVersion ver,
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
        String taskName,
        boolean transformOp)
        throws IgniteCheckedException, GridCacheEntryRemovedException {
        return new GridTuple3<>(false, null, null);
    }

    /** {@inheritDoc} */
    @Override public GridCacheUpdateAtomicResult innerUpdate(
        GridCacheVersion ver,
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
        @Nullable GridCacheVersion conflictVer,
        boolean conflictResolve,
        boolean intercept,
        UUID subjId,
        String taskName,
        @Nullable CacheObject prevVal,
        @Nullable Long updateCntr,
        @Nullable GridDhtAtomicAbstractUpdateFuture fut,
        boolean transformOp)
        throws IgniteCheckedException, GridCacheEntryRemovedException {
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
        boolean oldValPresent,
        @Nullable CacheObject oldVal,
        AffinityTopologyVersion topVer,
        CacheEntryPredicate[] filter,
        GridDrType drType,
        @Nullable GridCacheVersion drVer,
        UUID subjId,
        String taskName,
        @Nullable GridCacheVersion dhtVer,
        @Nullable Long updateCntr,
        MvccSnapshot mvccVer
        ) throws IgniteCheckedException, GridCacheEntryRemovedException {
        obsoleteVer = ver;

        val = null;

        return new GridCacheUpdateTxResult(true);
    }

    /** @inheritDoc */
    @Override public boolean clear(GridCacheVersion ver, boolean readers) throws IgniteCheckedException {
        if (ver == null || ver.equals(this.ver)) {
            val = null;

            return true;
        }

        return false;
    }

    /** @inheritDoc */
    @Override public boolean tmLock(IgniteInternalTx tx,
        long timeout,
        @Nullable GridCacheVersion serOrder,
        GridCacheVersion serReadVer,
        boolean read) {
        assert false;
        return false;
    }

    /** @inheritDoc */
    @Override public void txUnlock(IgniteInternalTx tx) {
        assert false;
    }

    /** @inheritDoc */
    @Override public boolean removeLock(GridCacheVersion ver) {
        GridCacheMvccCandidate doomed = mvcc.candidate(ver);

        mvcc.remove(ver);

        return doomed != null;
    }

    /** @inheritDoc */
    @Override public boolean markObsolete(GridCacheVersion ver) {
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
    @Override public boolean markObsoleteIfEmpty(GridCacheVersion ver) {
        if (val == null)
            obsoleteVer = ver;

        return obsoleteVer != null;
    }

    /** {@inheritDoc} */
    @Override public boolean markObsoleteVersion(GridCacheVersion ver) {
        if (this.ver.equals(ver)) {
            obsoleteVer = ver;

            return true;
        }

        return false;
    }

    /** @inheritDoc */
    @Override public GridCacheVersion version() {
        return ver;
    }

    /** @inheritDoc */
    @Override public boolean checkSerializableReadVersion(GridCacheVersion ver) {
        assert false;

        return false;
    }

    /** @inheritDoc */
    @Override public boolean initialValue(
        CacheObject val,
        GridCacheVersion ver,
        MvccVersion mvccVer,
        MvccVersion newMvccVer,
        byte mvccTxState,
        byte newMvccTxState,
        long ttl,
        long expireTime,
        boolean preload,
        AffinityTopologyVersion topVer,
        GridDrType drType,
        boolean fromStore
    ) throws IgniteCheckedException, GridCacheEntryRemovedException {
        assert false;

        return false;
    }

    /** @inheritDoc */
    @Override public GridCacheVersionedEntryEx versionedEntry(final boolean keepBinary) throws IgniteCheckedException {
        return null;
    }

    /** @inheritDoc */
    @Override public EntryGetResult versionedValue(CacheObject val,
        GridCacheVersion curVer,
        GridCacheVersion newVer,
        @Nullable IgniteCacheExpiryPolicy loadExpiryPlc,
        @Nullable ReaderArguments readerArgs) {
        assert false;

        return null;
    }

    /** @inheritDoc */
    @Override public boolean hasLockCandidate(GridCacheVersion ver) {
        return mvcc.hasCandidate(ver);
    }

    /** @inheritDoc */
    @Override public boolean lockedByAny(GridCacheVersion... exclude) {
        return !mvcc.isEmpty(exclude);
    }

    /** @inheritDoc */
    @Override public boolean lockedByThread()  {
        return lockedByThread(Thread.currentThread().getId());
    }

    /** @inheritDoc */
    @Override public boolean lockedLocally(GridCacheVersion lockVer) {
        return mvcc.isLocallyOwned(lockVer);
    }

    /** {@inheritDoc} */
    @Override public boolean lockedLocallyByIdOrThread(GridCacheVersion lockVer, long threadId)
        throws GridCacheEntryRemovedException {
        return lockedLocally(lockVer) || lockedByThread(threadId);
    }

    /** @inheritDoc */
    @Override public boolean lockedByThread(long threadId, GridCacheVersion exclude) {
        return mvcc.isLocallyOwnedByThread(threadId, false, exclude);
    }

    /** @inheritDoc */
    @Override public boolean lockedByThread(long threadId) {
        return mvcc.isLocallyOwnedByThread(threadId, true);
    }

    /** @inheritDoc */
    @Override public boolean lockedBy(GridCacheVersion ver) {
        return mvcc.isOwnedBy(ver);
    }

    /** @inheritDoc */
    @Override public boolean lockedByThreadUnsafe(long threadId) {
        return mvcc.isLocallyOwnedByThread(threadId, true);
    }

    /** @inheritDoc */
    @Override public boolean lockedByUnsafe(GridCacheVersion ver) {
        return mvcc.isOwnedBy(ver);
    }

    /** @inheritDoc */
    @Override public boolean lockedLocallyUnsafe(GridCacheVersion lockVer) {
        return mvcc.isLocallyOwned(lockVer);
    }

    /** @inheritDoc */
    @Override public boolean hasLockCandidateUnsafe(GridCacheVersion ver) {
        return mvcc.hasCandidate(ver);
    }

    /** @inheritDoc */
    @Override public Collection<GridCacheMvccCandidate> localCandidates(GridCacheVersion... exclude) {
        return mvcc.localCandidates(exclude);
    }

    /** @inheritDoc */
    Collection<GridCacheMvccCandidate> localCandidates(boolean reentries, GridCacheVersion... exclude) {
        return mvcc.localCandidates(reentries, exclude);
    }

    /** @inheritDoc */
    @Override public Collection<GridCacheMvccCandidate> remoteMvccSnapshot(GridCacheVersion... exclude) {
        return mvcc.remoteCandidates(exclude);
    }

    /** {@inheritDoc} */
    @Override public GridCacheMvccCandidate localCandidate(long threadId) throws GridCacheEntryRemovedException {
        return mvcc.localCandidate(threadId);
    }

    /** @inheritDoc */
    @Override public GridCacheMvccCandidate candidate(GridCacheVersion ver) {
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
    GridCacheMvccCandidate anyOwner() {
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
    @Override public CacheObject valueBytes(GridCacheVersion ver) {
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
    @Override public boolean onTtlExpired(GridCacheVersion obsoleteVer) {
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
    @Override public void updateTtl(GridCacheVersion ver, long ttl) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public CacheObject unswap() throws IgniteCheckedException {
        return null;
    }

    /** {@inheritDoc} */
    @Override public CacheObject unswap(boolean needVal) throws IgniteCheckedException {
        return null;
    }

    /** {@inheritDoc} */
    @Override public CacheObject unswap(CacheDataRow row) throws IgniteCheckedException {
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean hasLockCandidate(long threadId) throws GridCacheEntryRemovedException {
        return localCandidate(threadId) != null;
    }

    /** {@inheritDoc} */
    @Override public void updateIndex(SchemaIndexCacheFilter filter, SchemaIndexCacheVisitorClosure clo)
        throws IgniteCheckedException, GridCacheEntryRemovedException {
        // No-op.
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
    @Nullable @Override public CacheObject mvccPeek(boolean onheapOnly) {
        return null;
    }

    /** {@inheritDoc} */
    @Nullable @Override public CacheObject peek(boolean heap,
        boolean offheap,
        AffinityTopologyVersion topVer,
        @Nullable IgniteCacheExpiryPolicy plc) {
        return null;
    }

    /** {@inheritDoc} */
    @Nullable @Override public CacheObject peek()
        throws GridCacheEntryRemovedException, IgniteCheckedException {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void onUnlock() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void lockEntry() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void unlockEntry() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public boolean lockedByCurrentThread() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public GridCacheUpdateTxResult mvccUpdateRowsWithPreloadInfo(IgniteInternalTx tx,
        UUID affNodeId,
        AffinityTopologyVersion topVer,
        List<GridCacheEntryInfo> entries,
        GridCacheOperation op,
        MvccSnapshot mvccVer,
        IgniteUuid futId,
        int batchNum) throws IgniteCheckedException, GridCacheEntryRemovedException {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void touch(AffinityTopologyVersion topVer) {
        context().evicts().touch(this, topVer);
    }
}
