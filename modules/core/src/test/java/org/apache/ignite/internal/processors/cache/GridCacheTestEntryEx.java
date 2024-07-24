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
import java.util.UUID;
import javax.cache.Cache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.eviction.EvictableEntry;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridDhtAtomicAbstractUpdateFuture;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxKey;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersionedEntryEx;
import org.apache.ignite.internal.processors.dr.GridDrType;
import org.apache.ignite.internal.processors.query.schema.SchemaIndexCacheVisitorClosure;
import org.apache.ignite.internal.util.lang.GridMetadataAwareAdapter;
import org.jetbrains.annotations.Nullable;

/**
 * Test entry.
 */
@SuppressWarnings("unchecked")
public class GridCacheTestEntryEx extends GridMetadataAwareAdapter implements GridCacheEntryEx {
    /** Context */
    private final GridCacheContext context;

    /** Key. */
    private final KeyCacheObject key;

    /** Val. */
    private final CacheObject val;

    /** Version. */
    private final GridCacheVersion ver;

    /** TTL. */
    private final long ttl;

    /** */
    public GridCacheTestEntryEx(GridCacheContext context, KeyCacheObject key, CacheObject val, GridCacheVersion ver, long ttl) {
        this.context = context;
        this.key = key;
        this.val = val;
        this.ver = ver;
        this.ttl = ttl;
    }

    /** {@inheritDoc} */
    @Override public boolean initialValue(CacheObject val, GridCacheVersion ver, long ttl, long expireTime,
        boolean preload, AffinityTopologyVersion topVer, GridDrType drType, boolean fromStore, boolean primary) {
        assert false;

        return false;
    }

    /** {@inheritDoc} */
    @Override
    public boolean initialValue(CacheObject val, GridCacheVersion ver, long ttl, long expireTime, boolean preload, AffinityTopologyVersion topVer, GridDrType drType, boolean fromStore, boolean primary, @Nullable CacheDataRow row) throws IgniteCheckedException, GridCacheEntryRemovedException {
        return false;
    }

    /** {@inheritDoc} */
    @Override
    public int memorySize() throws IgniteCheckedException {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public boolean isInternal() {
        return false;
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
    @Override public boolean deleted() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public <K, V> GridCacheContext<K, V> context() {
        return context;
    }

    /** {@inheritDoc} */
    @Override public int partition() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public KeyCacheObject key() {
        return key;
    }

    /** {@inheritDoc} */
    @Override public IgniteTxKey txKey() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public CacheObject rawGet() {
        return val;
    }

    /** {@inheritDoc} */
    @Override public boolean hasValue() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public CacheObject rawPut(CacheObject val, long ttl) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public <K, V> Cache.Entry<K, V> wrap() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public <K, V> Cache.Entry<K, V> wrapLazyValue(boolean keepBinary) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public @Nullable CacheObject peekVisibleValue() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public <K, V> EvictableEntry<K, V> wrapEviction() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public <K, V> CacheEntryImplEx<K, V> wrapVersioned() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public GridCacheVersion obsoleteVersion() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean obsolete() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean obsoleteOrDeleted() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean obsolete(GridCacheVersion exclude) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public @Nullable GridCacheEntryInfo info() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean invalidate(GridCacheVersion newVer) throws IgniteCheckedException {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean evictInternal(GridCacheVersion obsoleteVer, @Nullable CacheEntryPredicate[] filter,
        boolean evictOffheap) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public void onMarkedObsolete() {

    }

    /** {@inheritDoc} */
    @Override public boolean isNew() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean isNewLocked() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean valid(AffinityTopologyVersion topVer) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean partitionValid() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public CacheObject innerGet(@Nullable GridCacheVersion ver, @Nullable IgniteInternalTx tx, boolean readThrough,
        boolean updateMetrics, boolean evt, Object transformClo, String taskName,
        @Nullable IgniteCacheExpiryPolicy expiryPlc,
        boolean keepBinary) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public EntryGetResult innerGetVersioned(@Nullable GridCacheVersion ver, IgniteInternalTx tx, boolean updateMetrics,
        boolean evt, Object transformClo, String taskName, @Nullable IgniteCacheExpiryPolicy expiryPlc,
        boolean keepBinary,
        @Nullable ReaderArguments readerArgs) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public EntryGetResult innerGetAndReserveForLoad(boolean updateMetrics, boolean evt, String taskName,
        @Nullable IgniteCacheExpiryPolicy expiryPlc, boolean keepBinary,
        @Nullable ReaderArguments readerArgs) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void clearReserveForLoad(GridCacheVersion ver) {

    }

    /** {@inheritDoc} */
    @Override public @Nullable CacheObject innerReload() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public GridCacheUpdateTxResult innerSet(@Nullable IgniteInternalTx tx, UUID evtNodeId, UUID affNodeId,
        @Nullable CacheObject val, boolean writeThrough, boolean retval, long ttl, boolean evt, boolean metrics,
        boolean keepBinary, boolean oldValPresent, @Nullable CacheObject oldVal, AffinityTopologyVersion topVer,
        CacheEntryPredicate[] filter, GridDrType drType, long drExpireTime, @Nullable GridCacheVersion explicitVer,
        String taskName, @Nullable GridCacheVersion dhtVer,
        @Nullable Long updateCntr) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public GridCacheUpdateTxResult innerRemove(@Nullable IgniteInternalTx tx, UUID evtNodeId, UUID affNodeId,
        boolean retval, boolean evt, boolean metrics, boolean keepBinary, boolean oldValPresent,
        @Nullable CacheObject oldVal, AffinityTopologyVersion topVer, CacheEntryPredicate[] filter, GridDrType drType,
        @Nullable GridCacheVersion explicitVer, String taskName, @Nullable GridCacheVersion dhtVer,
        @Nullable Long updateCntr) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public GridCacheUpdateAtomicResult innerUpdate(GridCacheVersion ver, UUID evtNodeId, UUID affNodeId,
        GridCacheOperation op, @Nullable Object val, @Nullable Object[] invokeArgs, boolean writeThrough,
        boolean readThrough, boolean retval, boolean keepBinary, @Nullable IgniteCacheExpiryPolicy expiryPlc,
        boolean evt, boolean metrics, boolean primary, boolean checkVer, boolean readRepairRecovery,
        AffinityTopologyVersion topVer, @Nullable CacheEntryPredicate[] filter, GridDrType drType, long conflictTtl,
        long conflictExpireTime, @Nullable GridCacheVersion conflictVer, CacheObject prevStateMeta,
        boolean conflictResolve, boolean intercept, String taskName, @Nullable CacheObject prevVal,
        @Nullable Long updateCntr, @Nullable GridDhtAtomicAbstractUpdateFuture fut,
        boolean transformOp) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean clear(GridCacheVersion ver, boolean readers) throws IgniteCheckedException {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean tmLock(IgniteInternalTx tx, long timeout, @Nullable GridCacheVersion serOrder,
        @Nullable GridCacheVersion serReadVer,
        boolean read) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public void txUnlock(IgniteInternalTx tx) {

    }

    /** {@inheritDoc} */
    @Override public boolean removeLock(GridCacheVersion ver) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean markObsolete(GridCacheVersion ver) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean markObsoleteIfEmpty(@Nullable GridCacheVersion ver) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean markObsoleteVersion(GridCacheVersion ver) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public GridCacheVersion version() throws GridCacheEntryRemovedException {
        return ver;
    }

    /** {@inheritDoc} */
    @Override public boolean checkSerializableReadVersion(GridCacheVersion serReadVer) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public @Nullable CacheObject peek(boolean heap, boolean offheap, AffinityTopologyVersion topVer,
        @Nullable IgniteCacheExpiryPolicy plc) throws GridCacheEntryRemovedException, IgniteCheckedException {
        return null;
    }

    /** {@inheritDoc} */
    @Override public @Nullable CacheObject peek() throws GridCacheEntryRemovedException, IgniteCheckedException {
        return null;
    }

    /** {@inheritDoc} */
    @Override public <K, V> GridCacheVersionedEntryEx<K, V> versionedEntry(boolean keepBinary) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public EntryGetResult versionedValue(CacheObject val, @Nullable GridCacheVersion curVer,
        @Nullable GridCacheVersion newVer, @Nullable IgniteCacheExpiryPolicy loadExpiryPlc,
        @Nullable ReaderArguments readerArgs) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean hasLockCandidate(GridCacheVersion ver) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean hasLockCandidate(long threadId) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean lockedByAny(GridCacheVersion... exclude) throws GridCacheEntryRemovedException {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean lockedByThread() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean lockedLocallyByIdOrThread(GridCacheVersion lockVer, long threadId) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean lockedLocally(GridCacheVersion lockVer) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean lockedByThread(long threadId, GridCacheVersion exclude) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean lockedByThread(long threadId) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean lockedBy(GridCacheVersion ver) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean lockedByThreadUnsafe(long threadId) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean lockedByUnsafe(GridCacheVersion ver) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean lockedLocallyUnsafe(GridCacheVersion lockVer) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean hasLockCandidateUnsafe(GridCacheVersion ver) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public @Nullable GridCacheMvccCandidate localCandidate(long threadId) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public Collection<GridCacheMvccCandidate> localCandidates(@Nullable GridCacheVersion... exclude) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public Collection<GridCacheMvccCandidate> remoteMvccSnapshot(GridCacheVersion... exclude) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public @Nullable GridCacheMvccCandidate candidate(GridCacheVersion ver) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public @Nullable GridCacheMvccCandidate candidate(UUID nodeId, long threadId) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public @Nullable GridCacheMvccCandidate localOwner() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public CacheObject valueBytes() throws GridCacheEntryRemovedException {
        return null;
    }

    /** {@inheritDoc} */
    @Override public @Nullable CacheObject valueBytes(
        @Nullable GridCacheVersion ver) throws IgniteCheckedException, GridCacheEntryRemovedException {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void updateIndex(SchemaIndexCacheVisitorClosure clo) {

    }

    /** {@inheritDoc} */
    @Override public long rawExpireTime() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public long expireTime() throws GridCacheEntryRemovedException {
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

    /** {@inheritDoc} */
    @Override public long ttl() throws GridCacheEntryRemovedException {
        return ttl;
    }

    /** {@inheritDoc} */
    @Override public void updateTtl(GridCacheVersion ver, IgniteCacheExpiryPolicy expiryPlc) {

    }

    /** {@inheritDoc} */
    @Override public void updateTtl(@Nullable GridCacheVersion ver, long ttl) {

    }

    /** {@inheritDoc} */
    @Override public @Nullable CacheObject unswap() throws IgniteCheckedException, GridCacheEntryRemovedException {
        return null;
    }

    /** {@inheritDoc} */
    @Override public @Nullable CacheObject unswap(
        CacheDataRow row) throws IgniteCheckedException, GridCacheEntryRemovedException {
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable CacheObject unswap(boolean needVal) throws IgniteCheckedException, GridCacheEntryRemovedException {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void onUnlock() {

    }

    /** {@inheritDoc} */
    @Override public void lockEntry() {

    }

    /** {@inheritDoc} */
    @Override public void unlockEntry() {

    }

    /** {@inheritDoc} */
    @Override public boolean tryLockEntry(long timeout) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean lockedByCurrentThread() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public void touch() {

    }
}
