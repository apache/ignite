/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache;

import org.apache.ignite.*;
import org.apache.ignite.lang.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.kernal.processors.dr.*;
import org.gridgain.grid.util.lang.*;
import org.gridgain.grid.util.typedef.*;
import org.jetbrains.annotations.*;

import javax.cache.expiry.*;
import java.util.*;

/**
 * Test entry.
 */
public class GridCacheTestEntryEx<K, V> extends GridMetadataAwareAdapter implements GridCacheEntryEx<K, V> {
    /** Key. */
    private K key;

    /** Val. */
    private V val;

    /** TTL. */
    private long ttl;

    /** Version. */
    private GridCacheVersion ver = new GridCacheVersion(0, 0, 0, 1, 0);

    /** Obsolete version. */
    private GridCacheVersion obsoleteVer = ver;

    /** MVCC. */
    private GridCacheMvcc<K> mvcc;

    /**
     * @param ctx Context.
     * @param key Key.
     */
    public GridCacheTestEntryEx(GridCacheContext<K, V> ctx, K key) {
        mvcc = new GridCacheMvcc<>(ctx);

        this.key = key;
    }

    /**
     * @param ctx Context.
     * @param key Key.
     * @param val Value.
     */
    public GridCacheTestEntryEx(GridCacheContext<K, V> ctx, K key, V val) {
        mvcc = new GridCacheMvcc<>(ctx);

        this.key = key;
        this.val = val;
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
    @Nullable @Override public GridCacheContext<K, V> context() {
        return null;
    }

    /** {@inheritDoc} */
    @Nullable @Override public GridCacheEntry<K, V> evictWrap() {
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
    @Nullable public GridCacheMvccCandidate<K> addLocal(
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
    public GridCacheMvccCandidate<K> addRemote(UUID nodeId, long threadId, GridCacheVersion ver, long timeout,
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
    public GridCacheMvccCandidate<K> addNearLocal(UUID nodeId, long threadId, GridCacheVersion ver, long timeout,
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
    @Nullable public GridCacheMvccCandidate<K> orderCompleted(GridCacheVersion baseVer,
        Collection<GridCacheVersion> committedVers, Collection<GridCacheVersion> rolledbackVers) {
        return mvcc.orderCompleted(baseVer, committedVers, rolledbackVers);
    }

    /**
     * @param ver Version.
     */
    public void doneRemote(GridCacheVersion ver) {
        mvcc.doneRemote(ver, Collections.<GridCacheVersion>emptyList(),
            Collections.<GridCacheVersion>emptyList(), Collections.<GridCacheVersion>emptyList());
    }

    /**
     * @param baseVer Base version.
     * @param owned Owned.
     */
    public void orderOwned(GridCacheVersion baseVer, GridCacheVersion owned) {
        mvcc.markOwned(baseVer, owned);
    }

    /**
     * @param ver Lock version to acquire or set to ready.
     * @return Current owner.
     */
    @Nullable public GridCacheMvccCandidate<K> readyLocal(GridCacheVersion ver) {
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
    @Nullable public GridCacheMvccCandidate<K> readyNearLocal(GridCacheVersion ver, GridCacheVersion mapped,
        Collection<GridCacheVersion> committedVers, Collection<GridCacheVersion> rolledbackVers,
        Collection<GridCacheVersion> pending) {
        return mvcc.readyNearLocal(ver, mapped, committedVers, rolledbackVers, pending);
    }

    /**
     * @param cand Candidate to set to ready.
     * @return Current owner.
     */
    @Nullable public GridCacheMvccCandidate<K> readyLocal(GridCacheMvccCandidate<K> cand) {
        return mvcc.readyLocal(cand);
    }

    /**
     * Local local release.
     * @return Removed lock candidate or <tt>null</tt> if candidate was not removed.
     */
    @Nullable public GridCacheMvccCandidate<K> releaseLocal() {
        return releaseLocal(Thread.currentThread().getId());
    }

    /**
     * Local release.
     *
     * @param threadId ID of the thread.
     * @return Current owner.
     */
    @Nullable public GridCacheMvccCandidate<K> releaseLocal(long threadId) {
        return mvcc.releaseLocal(threadId);
    }

    /**
     *
     */
    public void recheckLock() {
        mvcc.recheck();
    }

    /** {@inheritDoc} */
    @Override public GridCacheEntryInfo<K, V> info() {
        GridCacheEntryInfo<K, V> info = new GridCacheEntryInfo<>();

        info.key(key());
        info.value(val);
        info.ttl(ttl());
        info.expireTime(expireTime());
        info.keyBytes(keyBytes());
        info.valueBytes(valueBytes().getIfMarshaled());
        info.version(version());

        return info;
    }

    /** {@inheritDoc} */
    @Override public boolean valid(long topVer) {
        return true;
    }

    /** @inheritDoc */
    @Override public K key() {
        return key;
    }

    /** {@inheritDoc} */
    @Override public GridCacheTxKey<K> txKey() {
        return new GridCacheTxKey<>(key, 0);
    }

    /** @inheritDoc */
    @Override public V rawGet() {
        return val;
    }

    /** {@inheritDoc} */
    @Override public V rawGetOrUnmarshal(boolean tmp) throws IgniteCheckedException {
        return val;
    }

    /** {@inheritDoc} */
    @Override public boolean hasValue() {
        return val != null;
    }

    /** @inheritDoc */
    @Override public V rawPut(V val, long ttl) {
        V old = this.val;

        this.ttl = ttl;
        this.val = val;

        return old;
    }

    /** @inheritDoc */
    @Override public GridCacheEntry<K, V> wrap(boolean prjAware) {
        assert false; return null;
    }

    /** {@inheritDoc} */
    @Override public GridCacheEntry<K, V> wrapFilterLocked() throws IgniteCheckedException {
        assert false; return null;
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
    @Override public boolean invalidate(@Nullable GridCacheVersion curVer, GridCacheVersion newVer)
        throws IgniteCheckedException {
        assert false; return false;
    }

    /** @inheritDoc */
    @Override public boolean invalidate(@Nullable IgnitePredicate<GridCacheEntry<K, V>>[] filter)
        throws GridCacheEntryRemovedException, IgniteCheckedException {
        assert false; return false;
    }

    /** @inheritDoc */
    @Override public boolean compact(@Nullable IgnitePredicate<GridCacheEntry<K, V>>[] filter)
        throws GridCacheEntryRemovedException, IgniteCheckedException {
        assert false;  return false;
    }

    /** @inheritDoc */
    @Override public boolean evictInternal(boolean swap, GridCacheVersion obsoleteVer,
        @Nullable IgnitePredicate<GridCacheEntry<K, V>>[] filter) {
        assert false; return false;
    }

    /** {@inheritDoc} */
    @Override public GridCacheBatchSwapEntry<K, V> evictInBatchInternal(GridCacheVersion obsoleteVer)
        throws IgniteCheckedException {
        assert false; return null;
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
    @Override public V innerGet(@Nullable GridCacheTxEx<K, V> tx,
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
        IgnitePredicate<GridCacheEntry<K, V>>[] filter) {
        return val;
    }

    /** @inheritDoc */
    @Override public V innerReload(IgnitePredicate<GridCacheEntry<K, V>>[] filter) {
        return val;
    }

    /** @inheritDoc */
    @Override public GridCacheUpdateTxResult<V> innerSet(@Nullable GridCacheTxEx<K, V> tx, UUID evtNodeId, UUID affNodeId,
        @Nullable V val, @Nullable byte[] valBytes, boolean writeThrough, boolean retval, long ttl,
        boolean evt, boolean metrics, long topVer, IgnitePredicate<GridCacheEntry<K, V>>[] filter, GridDrType drType,
        long drExpireTime, @Nullable GridCacheVersion drVer, UUID subjId, String taskName) throws IgniteCheckedException,
        GridCacheEntryRemovedException {
        return new GridCacheUpdateTxResult<>(true, rawPut(val, ttl));
    }

    /** {@inheritDoc} */
    @Override public IgniteBiTuple<Boolean, V> innerUpdateLocal(GridCacheVersion ver, GridCacheOperation op,
        @Nullable Object writeObj, boolean writeThrough, boolean retval, long ttl, boolean evt, boolean metrics,
        @Nullable IgnitePredicate<GridCacheEntry<K, V>>[] filter, boolean intercept, UUID subjId, String taskName)
        throws IgniteCheckedException, GridCacheEntryRemovedException {
        return new IgniteBiTuple<>(false, null);
    }

    /** {@inheritDoc} */
    @Override public GridCacheUpdateAtomicResult<K, V> innerUpdate(
        GridCacheVersion ver,
        UUID evtNodeId,
        UUID affNodeId,
        GridCacheOperation op,
        @Nullable Object val,
        @Nullable byte[] valBytes,
        boolean writeThrough,
        boolean retval,
        ExpiryPolicy expiryPlc,
        boolean evt,
        boolean metrics,
        boolean primary,
        boolean checkVer,
        @Nullable IgnitePredicate<GridCacheEntry<K, V>>[] filter,
        GridDrType drType,
        long drTtl,
        long drExpireTime,
        @Nullable GridCacheVersion drVer,
        boolean drResolve,
        boolean intercept,
        UUID subjId,
        String taskName) throws IgniteCheckedException,
        GridCacheEntryRemovedException {
        return new GridCacheUpdateAtomicResult<>(true, rawPut((V)val, 0), (V)val, 0L, 0L, null, null, true);
    }

    /** @inheritDoc */
    @Override public GridCacheUpdateTxResult<V> innerRemove(@Nullable GridCacheTxEx<K, V> tx, UUID evtNodeId,
        UUID affNodeId, boolean writeThrough, boolean retval, boolean evt, boolean metrics, long topVer,
        IgnitePredicate<GridCacheEntry<K, V>>[] filter, GridDrType drType, @Nullable GridCacheVersion drVer, UUID subjId,
        String taskName)
        throws IgniteCheckedException, GridCacheEntryRemovedException {
        obsoleteVer = ver;

        V old = val;

        val = null;

        return new GridCacheUpdateTxResult<>(true, old);
    }

    /** @inheritDoc */
    @Override public boolean clear(GridCacheVersion ver, boolean readers,
        @Nullable IgnitePredicate<GridCacheEntry<K, V>>[] filter) throws IgniteCheckedException {
        if (ver == null || ver.equals(this.ver)) {
            val = null;

            return true;
        }

        return false;
    }

    /** @inheritDoc */
    @Override public boolean tmLock(GridCacheTxEx<K, V> tx, long timeout) {
        assert false; return false;
    }

    /** @inheritDoc */
    @Override public void txUnlock(GridCacheTxEx<K, V> tx) {
        assert false;
    }

    /** @inheritDoc */
    @Override public boolean removeLock(GridCacheVersion ver) {
        GridCacheMvccCandidate<K> doomed = mvcc.candidate(ver);

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
    @Override public byte[] keyBytes() {
        assert false; return null;
    }

    /** @inheritDoc */
    @Override public byte[] getOrMarshalKeyBytes() {
        assert false; return null;
    }

    /** @inheritDoc */
    @Override public GridCacheVersion version() {
        return ver;
    }

    /** @inheritDoc */
    @Override public V peek(GridCachePeekMode mode, IgnitePredicate<GridCacheEntry<K, V>>[] filter) {
        return val;
    }

    /** @inheritDoc */
    @Override public GridTuple<V> peek0(boolean failFast, GridCachePeekMode mode,
        IgnitePredicate<GridCacheEntry<K, V>>[] filter, GridCacheTxEx<K, V> tx)
        throws GridCacheEntryRemovedException, GridCacheFilterFailedException, IgniteCheckedException {
        return F.t(val);
    }

    /** @inheritDoc */
    @Override public V peek(Collection<GridCachePeekMode> modes, IgnitePredicate<GridCacheEntry<K, V>>[] filter)
        throws GridCacheEntryRemovedException {
        return val;
    }

    /** @inheritDoc */
    @Override public V peekFailFast(GridCachePeekMode mode,
        IgnitePredicate<GridCacheEntry<K, V>>[] filter) {
        assert false; return null;
    }

    /** {@inheritDoc} */
    @Override public V poke(V val) throws GridCacheEntryRemovedException, IgniteCheckedException {
        V old = this.val;

        this.val = val;

        return old;
    }

    /** @inheritDoc */
    @Override public boolean initialValue(V val, @Nullable byte[] valBytes, GridCacheVersion ver, long ttl,
        long expireTime, boolean preload, long topVer, GridDrType drType) throws IgniteCheckedException,
        GridCacheEntryRemovedException {
        assert false; return false;
    }

    /** @inheritDoc */
    @Override public boolean initialValue(K key, GridCacheSwapEntry<V> unswapped) {
        assert false; return false;
    }

    /** @inheritDoc */
    @Override public boolean versionedValue(V val, GridCacheVersion curVer, GridCacheVersion newVer) {
        assert false; return false;
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
    @Override public Collection<GridCacheMvccCandidate<K>> localCandidates(GridCacheVersion... exclude) {
        return mvcc.localCandidates(exclude);
    }

    /** @inheritDoc */
    public Collection<GridCacheMvccCandidate<K>> localCandidates(boolean reentries, GridCacheVersion... exclude) {
        return mvcc.localCandidates(reentries, exclude);
    }

    /** @inheritDoc */
    @Override public Collection<GridCacheMvccCandidate<K>> remoteMvccSnapshot(GridCacheVersion... exclude) {
        return mvcc.remoteCandidates(exclude);
    }

    /** {@inheritDoc} */
    @Override public GridCacheMvccCandidate<K> localCandidate(long threadId) throws GridCacheEntryRemovedException {
        return mvcc.localCandidate(threadId);
    }

    /** @inheritDoc */
    @Override public GridCacheMvccCandidate<K> candidate(GridCacheVersion ver) {
        return mvcc.candidate(ver);
    }

    /** {@inheritDoc} */
    @Override public GridCacheMvccCandidate<K> candidate(UUID nodeId, long threadId)
        throws GridCacheEntryRemovedException {
        return mvcc.remoteCandidate(nodeId, threadId);
    }

    /**
     * @return Any MVCC owner.
     */
    public GridCacheMvccCandidate<K> anyOwner() {
        return mvcc.anyOwner();
    }

    /** @inheritDoc */
    @Override public GridCacheMvccCandidate<K> localOwner() {
        return mvcc.localOwner();
    }

    /** @inheritDoc */
    @Override public void keyBytes(byte[] keyBytes) {
        assert false;
    }

    /** @inheritDoc */
    @Override public GridCacheValueBytes valueBytes() {
        assert false; return GridCacheValueBytes.nil();
    }

    /** @inheritDoc */
    @Override public GridCacheValueBytes valueBytes(GridCacheVersion ver) {
        assert false; return GridCacheValueBytes.nil();
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

    /** {@inheritDoc} */
    @Override public V unswap() throws IgniteCheckedException {
        return null;
    }

    /** {@inheritDoc} */
    @Override public V unswap(boolean ignoreFlags, boolean needVal) throws IgniteCheckedException {
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
}
