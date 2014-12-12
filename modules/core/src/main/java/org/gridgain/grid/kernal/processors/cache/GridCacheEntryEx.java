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
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.kernal.processors.cache.distributed.*;
import org.gridgain.grid.kernal.processors.dr.*;
import org.gridgain.grid.util.lang.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Internal API for cache entry ({@code 'Ex'} stands for extended).
 */
public interface GridCacheEntryEx<K, V> extends GridMetadataAware {
    /**
     * @return Memory size.
     * @throws IgniteCheckedException If failed.
     */
    public int memorySize() throws IgniteCheckedException;

    /**
     * @return {@code True} if entry is internal cache entry.
     */
    public boolean isInternal();

    /**
     * @return {@code True} if DHT.
     */
    public boolean isDht();

    /**
     * @return {@code True} if near.
     */
    public boolean isNear();

    /**
     * @return {@code True} if replicated.
     */
    public boolean isReplicated();

    /**
     * @return {@code True} if local.
     */
    public boolean isLocal();

    /**
     * @return {@code False} if entry belongs to cache map, {@code true} if this entry was created in colocated
     *      cache and node is not primary for this key.
     */
    public boolean detached();

    /**
     * Note: this method works only for cache configured in ATOMIC mode or for cache that is
     * data center replication target.
     *
     * @return {@code True} if entry has been already deleted.
     */
    public boolean deleted();

    /**
     * @return Context.
     */
    public GridCacheContext<K, V> context();

    /**
     * @return Partition ID.
     */
    public int partition();

    /**
     * @return Start version.
     */
    public long startVersion();

    /**
     * @return Key.
     */
    public K key();

    /**
     * @return Transaction key.
     */
    public GridCacheTxKey<K> txKey();

    /**
     * @return Value.
     */
    public V rawGet();

    /**
     * @param tmp If {@code true} can return temporary instance which is valid while entry lock is held,
     *        temporary object can used for filter evaluation or transform closure execution and
     *        should not be returned to user.
     * @return Value (unmarshalled if needed).
     * @throws IgniteCheckedException If failed.
     */
    public V rawGetOrUnmarshal(boolean tmp) throws IgniteCheckedException;

    /**
     * @return {@code True} if has value or value bytes.
     */
    public boolean hasValue();

    /**
     * @param val New value.
     * @param ttl Time to live.
     * @return Old value.
     */
    public V rawPut(V val, long ttl);

    /**
     * Wraps this map entry into cache entry.

     * @param prjAware {@code true} if entry should inherit projection properties.
     * @return Wrapped entry.
     */
    public GridCacheEntry<K, V> wrap(boolean prjAware);

    /**
     * Wraps this map entry into cache entry for filter evaluation inside entry lock.
     *
     * @return Wrapped entry.
     * @throws IgniteCheckedException If failed.
     */
    public GridCacheEntry<K, V> wrapFilterLocked() throws IgniteCheckedException;

    /**
     * @return Entry which is safe to pass into eviction policy.
     */
    public GridCacheEntry<K, V> evictWrap();

    /**
     * @return Not-null version if entry is obsolete.
     */
    public GridCacheVersion obsoleteVersion();

    /**
     * @return {@code True} if entry is obsolete.
     */
    public boolean obsolete();

    /**
     * @return {@code True} if entry is obsolete or deleted.
     * @see #deleted()
     */
    public boolean obsoleteOrDeleted();

    /**
     * @param exclude Obsolete version to ignore.
     * @return {@code True} if obsolete version is not {@code null} and is not the
     *      passed in version.
     */
    public boolean obsolete(GridCacheVersion exclude);

    /**
     * @return Entry info.
     */
    @Nullable public GridCacheEntryInfo<K, V> info();

    /**
     * Invalidates this entry.
     *
     * @param curVer Current version to match ({@code null} means always match).
     * @param newVer New version to set.
     * @return {@code true} if entry is obsolete.
     * @throws IgniteCheckedException If swap could not be released.
     */
    public boolean invalidate(@Nullable GridCacheVersion curVer, GridCacheVersion newVer) throws IgniteCheckedException;

    /**
     * Invalidates this entry if it passes given filter.
     *
     * @param filter Optional filter that entry should pass before invalidation.
     * @return {@code true} if entry was actually invalidated.
     * @throws IgniteCheckedException If swap could not be released.
     * @throws GridCacheEntryRemovedException If entry was removed.
     */
    public boolean invalidate(@Nullable IgnitePredicate<GridCacheEntry<K, V>>[] filter)
        throws GridCacheEntryRemovedException, IgniteCheckedException;

    /**
     * Optimizes the size of this entry.
     *
     * @param filter Optional filter that entry should pass before invalidation.
     * @throws GridCacheEntryRemovedException If entry was removed.
     * @throws IgniteCheckedException If operation failed.
     * @return {@code true} if entry was not being used and could be removed.
     */
    public boolean compact(@Nullable IgnitePredicate<GridCacheEntry<K, V>>[] filter)
        throws GridCacheEntryRemovedException, IgniteCheckedException;

    /**
     * @param swap Swap flag.
     * @param obsoleteVer Version for eviction.
     * @param filter Optional filter.
     * @return {@code True} if entry could be evicted.
     * @throws IgniteCheckedException In case of error.
     */
    public boolean evictInternal(boolean swap, GridCacheVersion obsoleteVer,
        @Nullable IgnitePredicate<GridCacheEntry<K, V>>[] filter) throws IgniteCheckedException;

    /**
     * Evicts entry when batch evict is performed. When called, does not write entry data to swap, but instead
     * returns batch swap entry if entry was marked obsolete.
     *
     * @param obsoleteVer Version to mark obsolete with.
     * @return Swap entry if this entry was marked obsolete, {@code null} if entry was not evicted.
     * @throws IgniteCheckedException If failed.
     */
    public GridCacheBatchSwapEntry<K, V> evictInBatchInternal(GridCacheVersion obsoleteVer) throws IgniteCheckedException;

    /**
     * This method should be called each time entry is marked obsolete
     * other than by calling {@link #markObsolete(GridCacheVersion)}.
     */
    public void onMarkedObsolete();

    /**
     * Checks if entry is new assuming lock is held externally.
     *
     * @return {@code True} if entry is new.
     * @throws GridCacheEntryRemovedException If entry was removed.
     */
    public boolean isNew() throws GridCacheEntryRemovedException;

    /**
     * Checks if entry is new while holding lock.
     *
     * @return {@code True} if entry is new.
     * @throws GridCacheEntryRemovedException If entry was removed.
     */
    public boolean isNewLocked() throws GridCacheEntryRemovedException;

    /**
     * @param topVer Topology version where validation should be performed.
     *  When negative the latest available topology should be used.
     *
     * @return Checks if value is valid.
     */
    public boolean valid(long topVer);

    /**
     * @return {@code True} if partition is in valid.
     */
    public boolean partitionValid();

    /**
     * @param tx Ongoing transaction (possibly null).
     * @param readSwap Flag indicating whether to check swap memory.
     * @param readThrough Flag indicating whether to read through.
     * @param failFast If {@code true}, then throw {@link GridCacheFilterFailedException} if
     *      filter didn't pass.
     * @param unmarshal Unmarshal flag.
     * @param updateMetrics If {@code true} then metrics should be updated.
     * @param evt Flag to signal event notification.
     * @param tmp If {@code true} can return temporary instance which is valid while entry lock is held,
     *        temporary object can used for filter evaluation or transform closure execution and
     *        should not be returned to user.
     * @param subjId Subject ID initiated this read.
     * @param taskName Task name.
     * @param filter Filter to check prior to getting the value. Note that filter check
     *      together with getting the value is an atomic operation.
     * @param transformClo Transform closure to record event.
     * @return Cached value.
     * @throws IgniteCheckedException If loading value failed.
     * @throws GridCacheEntryRemovedException If entry was removed.
     * @throws GridCacheFilterFailedException If filter failed.
     */
    @Nullable public V innerGet(@Nullable GridCacheTxEx<K, V> tx,
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
        IgnitePredicate<GridCacheEntry<K, V>>[] filter)
        throws IgniteCheckedException, GridCacheEntryRemovedException, GridCacheFilterFailedException;

    /**
     * Reloads entry from underlying storage.
     *
     * @param filter Filter for entries.
     * @return Reloaded value.
     * @throws IgniteCheckedException If reload failed.
     * @throws GridCacheEntryRemovedException If entry has been removed.
     */
    @Nullable public V innerReload(IgnitePredicate<GridCacheEntry<K, V>>... filter) throws IgniteCheckedException,
        GridCacheEntryRemovedException;

    /**
     * @param tx Cache transaction.
     * @param evtNodeId ID of node responsible for this change.
     * @param affNodeId Partitioned node iD.
     * @param val Value to set.
     * @param valBytes Value bytes to set.
     * @param writeThrough If {@code true} then persist to storage.
     * @param retval {@code True} if value should be returned (and unmarshalled if needed).
     * @param ttl Time to live.
     * @param evt Flag to signal event notification.
     * @param metrics Flag to signal metrics update.
     * @param topVer Topology version.
     * @param filter Filter.
     * @param drType DR type.
     * @param drExpireTime DR expire time (if any).
     * @param explicitVer Explicit version (if any).
     * @param subjId Subject ID initiated this update.
     * @param taskName Task name.
     * @return Tuple containing success flag and old value. If success is {@code false},
     *      then value is {@code null}.
     * @throws IgniteCheckedException If storing value failed.
     * @throws GridCacheEntryRemovedException If entry has been removed.
     */
    public GridCacheUpdateTxResult<V> innerSet(
        @Nullable GridCacheTxEx<K, V> tx,
        UUID evtNodeId,
        UUID affNodeId,
        @Nullable V val,
        @Nullable byte[] valBytes,
        boolean writeThrough,
        boolean retval,
        long ttl,
        boolean evt,
        boolean metrics,
        long topVer,
        IgnitePredicate<GridCacheEntry<K, V>>[] filter,
        GridDrType drType,
        long drExpireTime,
        @Nullable GridCacheVersion explicitVer,
        @Nullable UUID subjId,
        String taskName
    ) throws IgniteCheckedException, GridCacheEntryRemovedException;

    /**
     * @param tx Cache transaction.
     * @param evtNodeId ID of node responsible for this change.
     * @param affNodeId Partitioned node iD.
     * @param writeThrough If {@code true}, persist to the storage.
     * @param retval {@code True} if value should be returned (and unmarshalled if needed).
     * @param evt Flag to signal event notification.
     * @param metrics Flag to signal metrics notification.
     * @param topVer Topology version.
     * @param filter Filter.
     * @param drType DR type.
     * @param explicitVer Explicit version (if any).
     * @param subjId Subject ID initiated this update.
     * @param taskName Task name.
     * @return Tuple containing success flag and old value. If success is {@code false},
     *      then value is {@code null}.
     * @throws IgniteCheckedException If remove failed.
     * @throws GridCacheEntryRemovedException If entry has been removed.
     */
    public GridCacheUpdateTxResult<V> innerRemove(
        @Nullable GridCacheTxEx<K, V> tx,
        UUID evtNodeId,
        UUID affNodeId,
        boolean writeThrough,
        boolean retval,
        boolean evt,
        boolean metrics,
        long topVer,
        IgnitePredicate<GridCacheEntry<K, V>>[] filter,
        GridDrType drType,
        @Nullable GridCacheVersion explicitVer,
        @Nullable UUID subjId,
        String taskName
    ) throws IgniteCheckedException, GridCacheEntryRemovedException;

    /**
     * @param ver Cache version to set. Entry will be updated only if current version is less then passed version.
     * @param evtNodeId Event node ID.
     * @param affNodeId Affinity node ID.
     * @param op Update operation.
     * @param val Value. Type depends on operation.
     * @param valBytes Value bytes. Can be non-null only if operation is UPDATE.
     * @param writeThrough Write through flag.
     * @param retval Return value flag.
     * @param ttl Time to live.
     * @param evt Event flag.
     * @param metrics Metrics update flag.
     * @param primary If update is performed on primary node (the one which assigns version).
     * @param checkVer Whether update should check current version and ignore update if current version is
     *      greater than passed in.
     * @param filter Optional filter to check.
     * @param drType DR type.
     * @param drTtl DR TTL (if any).
     * @param drExpireTime DR expire time (if any).
     * @param drVer DR version (if any).
     * @param drResolve If {@code true} then performs DR conflicts resolution.
     * @param intercept If {@code true} then calls cache interceptor.
     * @param subjId Subject ID initiated this update.
     * @param taskName Task name.
     * @return Tuple where first value is flag showing whether operation succeeded,
     *      second value is old entry value if return value is requested, third is updated entry value,
     *      fourth is the version to enqueue for deferred delete the fifth is DR conflict context
     *      or {@code null} if conflict resolution was not performed, the last boolean - whether update should be
     *      propagated to backups or not.
     * @throws IgniteCheckedException If update failed.
     * @throws GridCacheEntryRemovedException If entry is obsolete.
     */
    public GridCacheUpdateAtomicResult<K, V> innerUpdate(
        GridCacheVersion ver,
        UUID evtNodeId,
        UUID affNodeId,
        GridCacheOperation op,
        @Nullable Object val,
        @Nullable byte[] valBytes,
        boolean writeThrough,
        boolean retval,
        long ttl,
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
        @Nullable UUID subjId,
        String taskName
    ) throws IgniteCheckedException, GridCacheEntryRemovedException;

    /**
     * Update method for local cache in atomic mode.
     *
     * @param ver Cache version.
     * @param op Operation.
     * @param writeObj Value. Type depends on operation.
     * @param writeThrough Write through flag.
     * @param retval Return value flag.
     * @param ttl Time to live.
     * @param evt Event flag.
     * @param metrics Metrics update flag.
     * @param filter Optional filter to check.
     * @param intercept If {@code true} then calls cache interceptor.
     * @param subjId Subject ID initiated this update.
     * @param taskName Task name.
     * @return Tuple containing success flag and old value.
     * @throws IgniteCheckedException If update failed.
     * @throws GridCacheEntryRemovedException If entry is obsolete.
     */
    public IgniteBiTuple<Boolean, V> innerUpdateLocal(
        GridCacheVersion ver,
        GridCacheOperation op,
        @Nullable Object writeObj,
        boolean writeThrough,
        boolean retval,
        long ttl,
        boolean evt,
        boolean metrics,
        @Nullable IgnitePredicate<GridCacheEntry<K, V>>[] filter,
        boolean intercept,
        @Nullable UUID subjId,
        String taskName
    ) throws IgniteCheckedException, GridCacheEntryRemovedException;


    /**
     * Marks entry as obsolete and, if possible or required, removes it
     * from swap storage.
     *
     * @param ver Obsolete version.
     * @param readers Flag to clear readers as well.
     * @param filter Optional entry filter.
     * @throws IgniteCheckedException If failed to remove from swap.
     * @return {@code True} if entry was not being used, passed the filter and could be removed.
     */
    public boolean clear(GridCacheVersion ver, boolean readers,
        @Nullable IgnitePredicate<GridCacheEntry<K, V>>[] filter) throws IgniteCheckedException;

    /**
     * This locks is called by transaction manager during prepare step
     * for optimistic transactions.
     *
     * @param tx Cache transaction.
     * @param timeout Timeout for lock acquisition.
     * @return {@code True} if lock was acquired, {@code false} otherwise.
     * @throws GridCacheEntryRemovedException If this entry is obsolete.
     * @throws GridDistributedLockCancelledException If lock has been cancelled.
     */
    public boolean tmLock(GridCacheTxEx<K, V> tx, long timeout) throws GridCacheEntryRemovedException,
        GridDistributedLockCancelledException;

    /**
     * Unlocks acquired lock.
     *
     * @param tx Cache transaction.
     * @throws GridCacheEntryRemovedException If this entry has been removed from cache.
     */
    public abstract void txUnlock(GridCacheTxEx<K, V> tx) throws GridCacheEntryRemovedException;

    /**
     * @param ver Removes lock.
     * @return {@code True} If lock has been removed.
     * @throws GridCacheEntryRemovedException If this entry has been removed from cache.
     */
    public boolean removeLock(GridCacheVersion ver) throws GridCacheEntryRemovedException;

    /**
     * Sets obsolete flag if possible.
     *
     * @param ver Version to set as obsolete.
     * @return {@code True} if entry is obsolete, {@code false} if
     *      entry is still used by other threads or nodes.
     */
    public boolean markObsolete(GridCacheVersion ver);

    /**
     * Sets obsolete flag if entry value is {@code null} or entry is expired and no
     * locks are held.
     *
     * @param ver Version to set as obsolete.
     * @return {@code True} if entry was marked obsolete.
     * @throws IgniteCheckedException If failed.
     */
    public boolean markObsoleteIfEmpty(@Nullable GridCacheVersion ver) throws IgniteCheckedException;

    /**
     * Sets obsolete flag if entry version equals to {@code ver}.
     *
     * @param ver Version to compare with.
     * @return {@code True} if marked obsolete.
     */
    public boolean markObsoleteVersion(GridCacheVersion ver);

    /**
     * @return Key bytes.
     */
    public byte[] keyBytes();

    /**
     * @return Key bytes.
     * @throws IgniteCheckedException If marshalling failed.
     */
    public byte[] getOrMarshalKeyBytes() throws IgniteCheckedException;

    /**
     * @return Version.
     * @throws GridCacheEntryRemovedException If entry has been removed.
     */
    public GridCacheVersion version() throws GridCacheEntryRemovedException;

    /**
     * Peeks into entry without loading value or updating statistics.
     *
     * @param mode Peek mode.
     * @param filter Optional filter.
     * @return Value.
     * @throws GridCacheEntryRemovedException If entry has been removed.
     */
    @Nullable public V peek(GridCachePeekMode mode, IgnitePredicate<GridCacheEntry<K, V>>... filter)
        throws GridCacheEntryRemovedException;

    /**
     * Peeks into entry without loading value or updating statistics.
     *
     * @param modes Peek modes.
     * @param filter Optional filter.
     * @return Value.
     * @throws GridCacheEntryRemovedException If entry has been removed.
     */
    @Nullable public V peek(Collection<GridCachePeekMode> modes, IgnitePredicate<GridCacheEntry<K, V>>... filter)
        throws GridCacheEntryRemovedException;

    /**
     * Peeks into entry without loading value or updating statistics.
     *
     * @param mode Peek mode.
     * @param filter Optional filter.
     * @return Value.
     * @throws GridCacheEntryRemovedException If entry has been removed.
     * @throws GridCacheFilterFailedException If {@code failFast} is {@code true} and
     *      filter didn't pass.
     */
    @Nullable public V peekFailFast(GridCachePeekMode mode, IgnitePredicate<GridCacheEntry<K, V>>... filter)
        throws GridCacheEntryRemovedException, GridCacheFilterFailedException;

    /**
     * @param failFast Fail-fast flag.
     * @param mode Peek mode.
     * @param filter Filter.
     * @param tx Transaction to peek value at (if mode is TX value).
     * @return Peeked value.
     * @throws IgniteCheckedException In case of error.
     * @throws GridCacheEntryRemovedException If removed.
     * @throws GridCacheFilterFailedException If filter failed.
     */
    @SuppressWarnings({"RedundantTypeArguments"})
    @Nullable public GridTuple<V> peek0(boolean failFast, GridCachePeekMode mode,
        @Nullable IgnitePredicate<GridCacheEntry<K, V>>[] filter, @Nullable GridCacheTxEx<K, V> tx)
        throws GridCacheEntryRemovedException, GridCacheFilterFailedException, IgniteCheckedException;

    /**
     * This method overwrites current in-memory value with new value.
     * <p>
     * Note that this method is non-transactional and non-distributed and should almost
     * never be used. It is meant to be used when fixing some heurisitic error state.
     *
     * @param val Value to set.
     * @return Previous value.
     * @throws IgniteCheckedException If poke operation failed.
     * @throws GridCacheEntryRemovedException if entry was unexpectedly removed.
     */
    public V poke(V val) throws GridCacheEntryRemovedException, IgniteCheckedException;

    /**
     * Sets new value if current version is <tt>0</tt>
     *
     * @param val New value.
     * @param valBytes Value bytes.
     * @param ver Version to use.
     * @param ttl Time to live.
     * @param expireTime Expiration time.
     * @param preload Flag indicating whether entry is being preloaded.
     * @param topVer Topology version.
     * @param drType DR type.
     * @return {@code True} if initial value was set.
     * @throws IgniteCheckedException In case of error.
     * @throws GridCacheEntryRemovedException If entry was removed.
     */
    public boolean initialValue(V val, @Nullable byte[] valBytes, GridCacheVersion ver, long ttl, long expireTime,
        boolean preload, long topVer, GridDrType drType) throws IgniteCheckedException, GridCacheEntryRemovedException;

    /**
     * Sets new value if current version is <tt>0</tt> using swap entry data.
     * Note that this method does not update cache index.
     *
     * @param key Key.
     * @param unswapped Swap entry to set entry state from.
     * @return {@code True} if  initial value was set.
     * @throws IgniteCheckedException In case of error.
     * @throws GridCacheEntryRemovedException If entry was removed.
     */
    public boolean initialValue(K key, GridCacheSwapEntry<V> unswapped)
        throws IgniteCheckedException, GridCacheEntryRemovedException;

    /**
     * Sets new value if passed in version matches the current version
     * (used for read-through only).
     *
     * @param val New value.
     * @param curVer Version to match or {@code null} if match is not required.
     * @param newVer Version to set.
     * @return {@code True} if versioned matched.
     * @throws IgniteCheckedException If index could not be updated.
     * @throws GridCacheEntryRemovedException If entry was removed.
     */
    public boolean versionedValue(V val, @Nullable GridCacheVersion curVer, @Nullable GridCacheVersion newVer)
        throws IgniteCheckedException, GridCacheEntryRemovedException;

    /**
     * Checks if the candidate is either owner or pending.
     *
     * @param ver Candidate version to check.
     * @return {@code True} if the candidate is either owner or pending.
     * @throws GridCacheEntryRemovedException If entry was removed.
     */
    public boolean hasLockCandidate(GridCacheVersion ver) throws GridCacheEntryRemovedException;

    /**
     * Checks if the candidate is either owner or pending.
     *
     * @param threadId ThreadId.
     * @return {@code True} if the candidate is either owner or pending.
     * @throws GridCacheEntryRemovedException If entry was removed.
     */
    public boolean hasLockCandidate(long threadId) throws GridCacheEntryRemovedException;

    /**
     * @param exclude Exclude versions.
     * @return {@code True} if lock is owned by any thread or node.
     * @throws GridCacheEntryRemovedException If entry was removed.
     */
    public boolean lockedByAny(GridCacheVersion... exclude) throws GridCacheEntryRemovedException;

    /**
     * @return {@code True} if lock is owned by current thread.
     * @throws GridCacheEntryRemovedException If entry was removed.
     */
    public boolean lockedByThread() throws GridCacheEntryRemovedException;

    /**
     * @param lockVer Lock ID.
     * @param threadId Thread ID.
     * @return {@code True} if locked either locally or by thread.
     * @throws GridCacheEntryRemovedException If removed.
     */
    public boolean lockedLocallyByIdOrThread(GridCacheVersion lockVer, long threadId) throws GridCacheEntryRemovedException;

    /**
     *
     * @param lockVer Lock ID to check.
     * @return {@code True} if lock is owned by candidate.
     * @throws GridCacheEntryRemovedException If entry was removed.
     */
    public boolean lockedLocally(GridCacheVersion lockVer) throws GridCacheEntryRemovedException;

    /**
     * @param threadId Thread ID to check.
     * @param exclude Version to exclude from check.
     * @return {@code True} if lock is owned by given thread.
     * @throws GridCacheEntryRemovedException If entry was removed.
     */
    public boolean lockedByThread(long threadId, GridCacheVersion exclude) throws GridCacheEntryRemovedException;

    /**
     * @param threadId Thread ID to check.
     * @return {@code True} if lock is owned by given thread.
     * @throws GridCacheEntryRemovedException If entry was removed.
     */
    public boolean lockedByThread(long threadId) throws GridCacheEntryRemovedException;

    /**
     * @param ver Version to check for ownership.
     * @return {@code True} if owner has the specified version.
     * @throws GridCacheEntryRemovedException If entry was removed.
     */
    public boolean lockedBy(GridCacheVersion ver) throws GridCacheEntryRemovedException;

    /**
     * Will not fail for removed entries.
     *
     * @param threadId Thread ID to check.
     * @return {@code True} if lock is owned by given thread.
     */
    public boolean lockedByThreadUnsafe(long threadId);

    /**
     * @param ver Version to check for ownership.
     * @return {@code True} if owner has the specified version.
     */
    public boolean lockedByUnsafe(GridCacheVersion ver);

    /**
     *
     * @param lockVer Lock ID to check.
     * @return {@code True} if lock is owned by candidate.
     */
    public boolean lockedLocallyUnsafe(GridCacheVersion lockVer);

    /**
     * @param ver Lock version to check.
     * @return {@code True} if has candidate with given lock ID.
     */
    public boolean hasLockCandidateUnsafe(GridCacheVersion ver);

    /**
     * @param threadId Thread ID.
     * @return Local candidate.
     * @throws GridCacheEntryRemovedException If entry was removed.
     */
    @Nullable public GridCacheMvccCandidate<K> localCandidate(long threadId) throws GridCacheEntryRemovedException;

    /**
     * Gets all local candidates.
     *
     * @param exclude Versions to exclude from check.
     * @return All local candidates.
     * @throws GridCacheEntryRemovedException If entry was removed.
     */
    public Collection<GridCacheMvccCandidate<K>> localCandidates(@Nullable GridCacheVersion... exclude)
        throws GridCacheEntryRemovedException;

    /**
     * Gets all remote versions.
     *
     * @param exclude Exclude version.
     * @return All remote versions minus the excluded ones, if any.
     */
    public Collection<GridCacheMvccCandidate<K>> remoteMvccSnapshot(GridCacheVersion... exclude);

    /**
     * Gets lock candidate for given lock ID.
     *
     * @param ver Lock version.
     * @return Lock candidate for given ID.
     * @throws GridCacheEntryRemovedException If entry was removed.
     */
    @Nullable public GridCacheMvccCandidate<K> candidate(GridCacheVersion ver) throws GridCacheEntryRemovedException;

    /**
     * @param nodeId Node ID.
     * @param threadId Thread ID.
     * @return Candidate.
     * @throws GridCacheEntryRemovedException If entry was removed.
     */
    @Nullable public GridCacheMvccCandidate<K> candidate(UUID nodeId, long threadId)
        throws GridCacheEntryRemovedException;

    /**
     * @return Local owner.
     * @throws GridCacheEntryRemovedException If entry was removed.
     */
    @Nullable public GridCacheMvccCandidate<K> localOwner() throws GridCacheEntryRemovedException;

    /**
     * @param keyBytes Key bytes.
     * @throws GridCacheEntryRemovedException If entry was removed.
     */
    public void keyBytes(byte[] keyBytes) throws GridCacheEntryRemovedException;

    /**
     * @return Value bytes.
     * @throws GridCacheEntryRemovedException If entry was removed.
     */
    public GridCacheValueBytes valueBytes() throws GridCacheEntryRemovedException;

    /**
     * Gets cached serialized value bytes.
     *
     * @param ver Version for which to get value bytes.
     * @return Serialized value bytes.
     * @throws IgniteCheckedException If serialization failed.
     * @throws GridCacheEntryRemovedException If entry was removed.
     */
    @Nullable public GridCacheValueBytes valueBytes(@Nullable GridCacheVersion ver)
        throws IgniteCheckedException, GridCacheEntryRemovedException;

    /**
     * @return Expire time, without accounting for transactions or removals.
     */
    public long rawExpireTime();

    /**
     * @return Expiration time.
     * @throws GridCacheEntryRemovedException If entry was removed.
     */
    public long expireTime() throws GridCacheEntryRemovedException;

    /**
     * @return Expiration time. Does not check for entry obsolete flag.
     */
    public long expireTimeUnlocked();

    /**
     * Callback from ttl processor to cache entry indicating that entry is expired.
     *
     * @param obsoleteVer Version to set obsolete if entry is expired.
     * @return {@code True} if this entry was obsolete or became obsolete as a result of this call.
     */
    public boolean onTtlExpired(GridCacheVersion obsoleteVer);

    /**
     * @return Time to live, without accounting for transactions or removals.
     */
    public long rawTtl();

    /**
     * @return Time to live.
     * @throws GridCacheEntryRemovedException If entry was removed.
     */
    public long ttl() throws GridCacheEntryRemovedException;

    /**
     * @return Value.
     * @throws IgniteCheckedException If failed to read from swap storage.
     */
    @Nullable public V unswap() throws IgniteCheckedException;

    /**
     * Unswap ignoring flags.
     *
     * @param ignoreFlags Whether to ignore swap flags.
     * @param needVal If {@code false} then do not need to deserialize value during unswap.
     * @return Value.
     * @throws IgniteCheckedException If failed.
     */
    @Nullable public V unswap(boolean ignoreFlags, boolean needVal) throws IgniteCheckedException;
}
