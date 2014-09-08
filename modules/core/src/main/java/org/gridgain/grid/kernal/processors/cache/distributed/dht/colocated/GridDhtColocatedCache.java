/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.distributed.dht.colocated;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.kernal.processors.cache.distributed.*;
import org.gridgain.grid.kernal.processors.cache.distributed.dht.*;
import org.gridgain.grid.kernal.processors.cache.distributed.near.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.portables.*;
import org.gridgain.grid.security.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.grid.util.future.*;
import org.gridgain.grid.util.lang.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;

import static org.gridgain.grid.cache.GridCacheFlag.*;
import static org.gridgain.grid.cache.GridCachePeekMode.*;
import static org.gridgain.grid.kernal.processors.cache.GridCacheTxEx.FinalizationStatus.*;
import static org.gridgain.grid.kernal.processors.cache.GridCacheUtils.*;

/**
 * Colocated cache.
 */
public class GridDhtColocatedCache<K, V> extends GridDhtTransactionalCacheAdapter<K, V> {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Empty constructor required for {@link Externalizable}
     */
    public GridDhtColocatedCache() {
        // No-op.
    }

    /**
     * @param ctx Cache context.
     */
    public GridDhtColocatedCache(GridCacheContext<K, V> ctx) {
        super(ctx);
    }

    /**
     * Creates colocated cache with specified map.
     *
     * @param ctx Cache context.
     * @param map Cache map.
     */
    public GridDhtColocatedCache(GridCacheContext<K, V> ctx, GridCacheConcurrentMap<K, V> map) {
        super(ctx, map);
    }

    /** {@inheritDoc} */
    @Override public boolean isColocated() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public GridCacheTxLocalAdapter<K, V> newTx(
        boolean implicit,
        boolean implicitSingle,
        GridCacheTxConcurrency concurrency,
        GridCacheTxIsolation isolation,
        long timeout,
        boolean invalidate,
        boolean syncCommit,
        boolean syncRollback,
        boolean swapOrOffheapEnabled,
        boolean storeEnabled,
        int txSize,
        @Nullable Object grpLockKey,
        boolean partLock
    ) {
        assert !isNearEnabled(cacheCfg);

        // Use null as subject ID for transactions if subject per call is not set.
        GridCacheProjectionImpl<K, V> prj = ctx.projectionPerCall();

        UUID subjId = prj == null ? null : prj.subjectId();

        int taskNameHash = ctx.kernalContext().job().currentTaskNameHash();

        return new GridDhtColocatedTxLocal<>(implicit, implicitSingle, ctx, concurrency, isolation, timeout,
            invalidate, syncCommit, syncRollback, swapOrOffheapEnabled, storeEnabled, txSize, grpLockKey, partLock,
            subjId, taskNameHash);
    }

    /** {@inheritDoc} */
    @Override protected void init() {
        map.setEntryFactory(new GridCacheMapEntryFactory<K, V>() {
            /** {@inheritDoc} */
            @Override public GridCacheMapEntry<K, V> create(GridCacheContext<K, V> ctx, long topVer, K key, int hash,
                V val, GridCacheMapEntry<K, V> next, long ttl, int hdrId) {
                return new GridDhtColocatedCacheEntry<>(ctx, topVer, key, hash, val, next, ttl, hdrId);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public void start() throws GridException {
        super.start();

        ctx.io().addHandler(GridNearGetResponse.class, new CI2<UUID, GridNearGetResponse<K, V>>() {
            @Override public void apply(UUID nodeId, GridNearGetResponse<K, V> res) {
                processGetResponse(nodeId, res);
            }
        });

        ctx.io().addHandler(GridNearTxPrepareResponse.class, new CI2<UUID, GridNearTxPrepareResponse<K, V>>() {
            @Override public void apply(UUID nodeId, GridNearTxPrepareResponse<K, V> res) {
                processPrepareResponse(nodeId, res);
            }
        });

        ctx.io().addHandler(GridNearTxFinishResponse.class, new CI2<UUID, GridNearTxFinishResponse<K, V>>() {
            @Override public void apply(UUID nodeId, GridNearTxFinishResponse<K, V> res) {
                processFinishResponse(nodeId, res);
            }
        });

        ctx.io().addHandler(GridNearLockResponse.class, new CI2<UUID, GridNearLockResponse<K, V>>() {
            @Override public void apply(UUID nodeId, GridNearLockResponse<K, V> res) {
                processLockResponse(nodeId, res);
            }
        });
    }

    /**
     * Gets or creates entry for given key and given topology version.
     *
     * @param key Key for entry.
     * @param topVer Topology version.
     * @param allowDetached Whether to allow detached entries. If {@code true} and node is not primary
     *      for given key, a new detached entry will be created. Otherwise, entry will be obtained from
     *      dht cache map.
     * @return Cache entry.
     * @throws GridDhtInvalidPartitionException If {@code allowDetached} is false and node is not primary
     *      for given key.
     */
    GridDistributedCacheEntry<K, V> entryExx(K key, long topVer, boolean allowDetached) {
        return allowDetached && !ctx.affinity().primary(ctx.localNode(), key, topVer) ?
            new GridDhtDetachedCacheEntry<>(ctx, key, key.hashCode(), null, null, 0, 0) : entryExx(key, topVer);
    }

    /** {@inheritDoc} */
    @Override public V peek(K key, @Nullable Collection<GridCachePeekMode> modes) throws GridException {
        GridTuple<V> val = null;

        if (ctx.isReplicated() || !modes.contains(NEAR_ONLY)) {
            try {
                val = peek0(true, key, modes, ctx.tm().txx());
            }
            catch (GridCacheFilterFailedException ignored) {
                if (log.isDebugEnabled())
                    log.debug("Filter validation failed for key: " + key);

                return null;
            }
        }

        return val != null ? val.get() : null;
    }

    /** {@inheritDoc} */
    @Override public boolean isLocked(K key) {
        return ctx.mvcc().isLockedByThread(key, -1);
    }

    /** {@inheritDoc} */
    @Override public boolean isLockedByThread(K key) {
        return ctx.mvcc().isLockedByThread(key, Thread.currentThread().getId());
    }

    /** {@inheritDoc} */
    @Override public GridFuture<Map<K, V>> getAllAsync(
        @Nullable final Collection<? extends K> keys,
        boolean forcePrimary,
        boolean skipTx,
        @Nullable final GridCacheEntryEx<K, V> entry,
        @Nullable UUID subjId,
        String taskName,
        final boolean deserializePortable,
        @Nullable final GridPredicate<GridCacheEntry<K, V>>[] filter
    ) {
        ctx.denyOnFlag(LOCAL);
        ctx.checkSecurity(GridSecurityPermission.CACHE_READ);

        if (F.isEmpty(keys))
            return new GridFinishedFuture<>(ctx.kernalContext(), Collections.<K, V>emptyMap());

        GridCacheTxLocalAdapter<K, V> tx = ctx.tm().threadLocalTx();

        if (tx != null && !tx.implicit() && !skipTx) {
            return asyncOp(tx, new AsyncOp<Map<K, V>>(keys) {
                @Override public GridFuture<Map<K, V>> op(GridCacheTxLocalAdapter<K, V> tx) {
                    return ctx.wrapCloneMap(tx.getAllAsync(keys, entry, deserializePortable, filter));
                }
            });
        }

        long topVer = tx == null ? ctx.affinity().affinityTopologyVersion() : tx.topologyVersion();

        subjId = ctx.subjectIdPerCall(subjId);

        return loadAsync(keys, false, forcePrimary, topVer, subjId, taskName, deserializePortable, filter);
    }

    /** {@inheritDoc} */
    @Override protected GridCacheEntryEx<K, V> entryExSafe(K key, long topVer) {
        try {
            return ctx.affinity().localNode(key, topVer) ? entryEx(key) : null;
        }
        catch (GridDhtInvalidPartitionException ignored) {
            return null;
        }
    }

    /** {@inheritDoc} */
    @Override public boolean containsKey(K key, @Nullable GridPredicate<GridCacheEntry<K, V>> filter) {
        A.notNull(key, "key");

        // We need detached entry here because if there is an ongoing transaction,
        // we should see this entry and apply filter.
        GridCacheEntryEx<K, V> e = entryExx(key, ctx.affinity().affinityTopologyVersion(), true, true);

        try {
            return e != null && e.peek(SMART, filter) != null;
        }
        catch (GridCacheEntryRemovedException ignore) {
            if (log.isDebugEnabled())
                log.debug("Got removed entry during peek (will ignore): " + e);

            return false;
        }
    }

    /**
     * @param keys Keys to load.
     * @param reload Reload flag.
     * @param forcePrimary Force get from primary node flag.
     * @param filter Filter.
     * @return Loaded values.
     */
    public GridFuture<Map<K, V>> loadAsync(@Nullable Collection<? extends K> keys, boolean reload,
        boolean forcePrimary, long topVer, @Nullable UUID subjId, String taskName, boolean deserializePortable,
        @Nullable GridPredicate<GridCacheEntry<K, V>>[] filter) {
        if (F.isEmpty(keys))
            return new GridFinishedFuture<>(ctx.kernalContext(), Collections.<K, V>emptyMap());

        validateCacheKeys(keys);

        // Optimisation: try to resolve value locally and escape 'get future' creation.
        if (!reload && !forcePrimary) {
            Map<K, V> locVals = new HashMap<>(keys.size(), 1.0f);

            GridCacheVersion obsoleteVer = null;

            boolean success = true;

            // Optimistically expect that all keys are available locally (avoid creation of get future).
            for (K key : keys) {
                GridCacheEntryEx<K, V> entry = null;

                while (true) {
                    try {
                        entry = ctx.isSwapOrOffheapEnabled() ? entryEx(key) : peekEx(key);

                        // If our DHT cache do has value, then we peek it.
                        if (entry != null) {
                            boolean isNew = entry.isNewLocked();

                            V v = entry.innerGet(null,
                                /*swap*/true,
                                /*read-through*/false,
                                /*fail-fast*/true,
                                /*unmarshal*/true,
                                /**update-metrics*/true,
                                /*event*/true,
                                subjId,
                                null,
                                taskName,
                                filter);

                            // Entry was not in memory or in swap, so we remove it from cache.
                            if (v == null) {
                                if (obsoleteVer == null)
                                    obsoleteVer = context().versions().next();

                                if (isNew && entry.markObsoleteIfEmpty(obsoleteVer))
                                    removeIfObsolete(key);

                                success = false;
                            }
                            else {
                                if (ctx.portableEnabled() && deserializePortable && v instanceof GridPortableObject)
                                    v = ((GridPortableObject)v).deserialize();

                                locVals.put(key, v);
                            }
                        }
                        else
                            success = false;

                        break; // While.
                    }
                    catch (GridCacheEntryRemovedException ignored) {
                        // No-op, retry.
                    }
                    catch (GridCacheFilterFailedException ignored) {
                        // No-op, skip the key.
                        break;
                    }
                    catch (GridDhtInvalidPartitionException ignored) {
                        success = false;

                        break; // While.
                    }
                    catch (GridException e) {
                        return new GridFinishedFuture<>(ctx.kernalContext(), e);
                    }
                    finally {
                        if (entry != null)
                            context().evicts().touch(entry, topVer);
                    }
                }

                if (!success)
                    break;
            }

            if (success)
                return ctx.wrapCloneMap(new GridFinishedFuture<>(ctx.kernalContext(), locVals));
        }

        // Either reload or not all values are available locally.
        GridPartitionedGetFuture<K, V> fut = new GridPartitionedGetFuture<>(ctx, keys, topVer, reload, forcePrimary,
            filter, subjId, taskName, deserializePortable);

        fut.init();

        return ctx.wrapCloneMap(fut);
    }

    /**
     * This is an entry point to pessimistic locking within transaction.
     *
     * {@inheritDoc}
     */
    @Override public GridFuture<Boolean> lockAllAsync(Collection<? extends K> keys, long timeout,
        @Nullable GridCacheTxLocalEx<K, V> tx, boolean isInvalidate, boolean isRead, boolean retval,
        @Nullable GridCacheTxIsolation isolation, GridPredicate<GridCacheEntry<K, V>>[] filter) {
        assert tx == null || tx instanceof GridDhtColocatedTxLocal;

        GridDhtColocatedTxLocal<K, V> txx = (GridDhtColocatedTxLocal<K, V>)tx;

        GridDhtColocatedLockFuture<K, V> fut = new GridDhtColocatedLockFuture<>(ctx, keys, txx, isRead, retval,
            timeout, filter);

        // Future will be added to mvcc only if it was mapped to remote nodes.
        fut.map();

        return fut;
    }

    /** {@inheritDoc} */
    @Override public GridNearTransactionalCache<K, V> near() {
        assert false : "Near cache is not available in colocated mode.";

        return null;
    }

    /** {@inheritDoc} */
    @Override public GridCacheEntry<K, V> entry(K key) throws GridDhtInvalidPartitionException {
        return new GridDhtCacheEntryImpl<>(ctx.projectionPerCall(), ctx, key, null);
    }

    /** {@inheritDoc} */
    @Override public void unlockAll(Collection<? extends K> keys,
        GridPredicate<GridCacheEntry<K, V>>[] filter) {
        if (keys.isEmpty())
            return;

        try {
            GridCacheVersion ver = null;

            int keyCnt = -1;

            Map<GridNode, GridNearUnlockRequest<K, V>> map = null;

            Collection<K> locKeys = new LinkedList<>();

            for (K key : keys) {
                GridDistributedCacheEntry<K, V> entry = peekExx(key);

                GridCacheEntry<K, V> cacheEntry = entry == null ? entry(key) : entry.wrap(false);

                if (!ctx.isAll(cacheEntry, filter))
                    break; // While.

                GridCacheMvccCandidate lock = ctx.mvcc().removeExplicitLock(Thread.currentThread().getId(), key, null);

                if (lock != null) {
                    final long topVer = lock.topologyVersion();

                    assert topVer > 0;

                    if (map == null) {
                        Collection<GridNode> affNodes = CU.allNodes(ctx, topVer);

                        keyCnt = (int)Math.ceil((double)keys.size() / affNodes.size());

                        map = new HashMap<>(affNodes.size());
                    }

                    if (ver == null)
                        ver = lock.version();

                    // Send request to remove from remote nodes.
                    GridNode primary = ctx.affinity().primary(key, topVer);

                    if (!lock.reentry()) {
                        if (!ver.equals(lock.version()))
                            throw new GridException("Failed to unlock (if keys were locked separately, " +
                                "then they need to be unlocked separately): " + keys);

                        if (!primary.isLocal()) {
                            GridNearUnlockRequest<K, V> req = map.get(primary);

                            if (req == null) {
                                map.put(primary, req = new GridNearUnlockRequest<>(keyCnt));

                                req.version(ver);
                            }

                            byte[] keyBytes = entry != null ? entry.getOrMarshalKeyBytes() : CU.marshal(ctx, key);

                            req.addKey(key, keyBytes, ctx);
                        }
                        else
                            locKeys.add(key);

                        if (log.isDebugEnabled())
                            log.debug("Removed lock (will distribute): " + lock);
                    }
                    else if (log.isDebugEnabled())
                        log.debug("Current thread still owns lock (or there are no other nodes)" +
                            " [lock=" + lock + ", curThreadId=" + Thread.currentThread().getId() + ']');
                }
            }

            if (ver == null)
                return;

            if (!locKeys.isEmpty())
                removeLocks(ctx.localNodeId(), ver, locKeys, true);

            for (Map.Entry<GridNode, GridNearUnlockRequest<K, V>> mapping : map.entrySet()) {
                GridNode n = mapping.getKey();

                GridDistributedUnlockRequest<K, V> req = mapping.getValue();

                assert !n.isLocal();

                if (!F.isEmpty(req.keyBytes()) || !F.isEmpty(req.keys()))
                    // We don't wait for reply to this message.
                    ctx.io().send(n, req);
            }
        }
        catch (GridException ex) {
            U.error(log, "Failed to unlock the lock for keys: " + keys, ex);
        }
    }

    /**
     * Removes locks regardless of whether they are owned or not for given
     * version and keys.
     *
     * @param threadId Thread ID.
     * @param ver Lock version.
     * @param keys Keys.
     */
    public void removeLocks(long threadId, GridCacheVersion ver, Collection<? extends K> keys) {
        if (keys.isEmpty())
            return;

        try {
            int keyCnt = -1;

            Map<GridNode, GridNearUnlockRequest<K, V>> map = null;

            Collection<K> locKeys = new LinkedList<>();

            for (K key : keys) {
                GridCacheMvccCandidate<K> lock = ctx.mvcc().removeExplicitLock(threadId, key, ver);

                if (lock != null) {
                    long topVer = lock.topologyVersion();

                    if (map == null) {
                        Collection<GridNode> affNodes = CU.allNodes(ctx, topVer);

                        keyCnt = (int)Math.ceil((double)keys.size() / affNodes.size());

                        map = new HashMap<>(affNodes.size());
                    }

                    GridNode primary = ctx.affinity().primary(key, topVer);

                    if (!primary.isLocal()) {
                        // Send request to remove from remote nodes.
                        GridNearUnlockRequest<K, V> req = map.get(primary);

                        if (req == null) {
                            map.put(primary, req = new GridNearUnlockRequest<>(keyCnt));

                            req.version(ver);
                        }

                        GridCacheEntryEx<K, V> entry = peekEx(key);

                        byte[] keyBytes = entry != null ? entry.getOrMarshalKeyBytes() : CU.marshal(ctx, key);

                        req.addKey(key, keyBytes, ctx);
                    }
                    else
                        locKeys.add(key);
                }
            }

            if (!locKeys.isEmpty())
                removeLocks(ctx.localNodeId(), ver, locKeys, true);

            if (map == null || map.isEmpty())
                return;

            Collection<GridCacheVersion> committed = ctx.tm().committedVersions(ver);
            Collection<GridCacheVersion> rolledback = ctx.tm().rolledbackVersions(ver);

            for (Map.Entry<GridNode, GridNearUnlockRequest<K, V>> mapping : map.entrySet()) {
                GridNode n = mapping.getKey();

                GridDistributedUnlockRequest<K, V> req = mapping.getValue();

                if (!F.isEmpty(req.keyBytes()) || !F.isEmpty(req.keys())) {
                    req.completedVersions(committed, rolledback);

                    // We don't wait for reply to this message.
                    ctx.io().send(n, req);
                }
            }
        }
        catch (GridException ex) {
            U.error(log, "Failed to unlock the lock for keys: " + keys, ex);
        }
    }

    /**
     * @param tx Started colocated transaction to prepare.
     * @param reads Read collection.
     * @param writes Write collection.
     * @param txNodes Transaction nodes mapping.
     * @param last {@code True} if this is last prepare operation.
     * @param lastBackups IDs of backup nodes receiving last prepare request.
     * @return Future for transaction.
     */
    public GridFuture<GridCacheTxEx<K, V>> prepareTxLocally(final GridDhtColocatedTxLocal<K, V> tx,
        final Collection<GridCacheTxEntry<K, V>> reads, final Collection<GridCacheTxEntry<K, V>> writes,
        final Map<UUID, Collection<UUID>> txNodes, final boolean last, final Collection<UUID> lastBackups) {
        assert tx != null;

        GridFuture<Object> fut = ctx.preloader().request(
            F.viewReadOnly(F.concat(false, reads, writes), CU.<K, V>tx2key()), tx.topologyVersion());

        return new GridEmbeddedFuture<>(
            ctx.kernalContext(),
            fut,
            new C2<Object, Exception, GridFuture<GridCacheTxEx<K, V>>>() {
                @Override public GridFuture<GridCacheTxEx<K, V>> apply(Object o, Exception ex) {
                    if (ex != null)
                        throw new GridClosureException(ex);

                    GridFuture<GridCacheTxEx<K, V>> fut = tx.prepareAsyncLocal(reads, writes, txNodes, last,
                        lastBackups);

                    if (tx.isRollbackOnly())
                        tx.rollbackAsync();

                    return fut;
                }
            },
            new C2<GridCacheTxEx<K, V>, Exception, GridCacheTxEx<K, V>>() {
                @Nullable @Override public GridCacheTxEx<K, V> apply(GridCacheTxEx<K, V> tx, Exception e) {
                    if (e != null) {
                        // tx can be null of exception occurred.
                        if (tx != null)
                            tx.setRollbackOnly(); // Just in case.

                        if (!(e instanceof GridCacheTxOptimisticException))
                            U.error(log, "Failed to prepare DHT transaction: " + tx, e);
                    }

                    return tx;
                }
            }
        );
    }

    /**
     * @param tx Started colocated transaction (if any).
     * @param threadId Thread ID.
     * @param ver Lock version.
     * @param topVer Topology version.
     * @param keys Mapped keys.
     * @param txRead Tx read.
     * @param timeout Lock timeout.
     * @param filter filter Optional filter.
     * @return Lock future.
     */
    GridFuture<Exception> lockAllAsync(
        @Nullable final GridDhtColocatedTxLocal<K, V> tx,
        final long threadId,
        final GridCacheVersion ver,
        final long topVer,
        final Collection<K> keys,
        final boolean txRead,
        final long timeout,
        @Nullable final GridPredicate<GridCacheEntry<K, V>>[] filter
    ) {
        assert keys != null;

        GridFuture<Object> keyFut = ctx.dht().dhtPreloader().request(keys, topVer);

        // Prevent embedded future creation if possible.
        if (keyFut.isDone()) {
            try {
                // Check for exception.
                keyFut.get();

                return lockAllAsync0(tx, threadId, ver, topVer, keys, txRead, timeout, filter);
            }
            catch (GridException e) {
                return new GridFinishedFuture<>(ctx.kernalContext(), e);
            }
        }
        else {
            return new GridEmbeddedFuture<>(true, keyFut,
                new C2<Object, Exception, GridFuture<Exception>>() {
                    @Override public GridFuture<Exception> apply(Object o, Exception exx) {
                        if (exx != null)
                            return new GridDhtFinishedFuture<>(ctx.kernalContext(), exx);

                        return lockAllAsync0(tx, threadId, ver, topVer, keys, txRead, timeout, filter);
                    }
                },
                ctx.kernalContext());
        }
    }

    /**
     * @param tx Started colocated transaction (if any).
     * @param threadId Thread ID.
     * @param ver Lock version.
     * @param topVer Topology version.
     * @param keys Mapped keys.
     * @param txRead Tx read.
     * @param timeout Lock timeout.
     * @param filter filter Optional filter.
     * @return Lock future.
     */
    private GridFuture<Exception> lockAllAsync0(@Nullable final GridDhtColocatedTxLocal<K, V> tx, long threadId,
        final GridCacheVersion ver, final long topVer, final Collection<K> keys, final boolean txRead,
        final long timeout, @Nullable final GridPredicate<GridCacheEntry<K, V>>[] filter) {
        int cnt = keys.size();

        if (tx == null) {
            GridDhtLockFuture<K, V> fut = new GridDhtLockFuture<>(ctx, ctx.localNodeId(), ver, topVer, cnt, txRead,
                timeout, tx, threadId, filter);

            // Add before mapping.
            if (!ctx.mvcc().addFuture(fut))
                throw new IllegalStateException("Duplicate future ID: " + fut);

            boolean timedout = false;

            for (K key : keys) {
                if (timedout)
                    break;

                while (true) {
                    GridDhtCacheEntry<K, V> entry = entryExx(key, topVer);

                    try {
                        fut.addEntry(key == null ? null : entry);

                        if (fut.isDone())
                            timedout = true;

                        break;
                    }
                    catch (GridCacheEntryRemovedException ignore) {
                        if (log.isDebugEnabled())
                            log.debug("Got removed entry when adding lock (will retry): " + entry);
                    }
                    catch (GridDistributedLockCancelledException e) {
                        if (log.isDebugEnabled())
                            log.debug("Got lock request for cancelled lock (will ignore): " +
                                entry);

                        fut.onError(e);

                        return new GridDhtFinishedFuture<>(ctx.kernalContext(), e);
                    }
                }
            }

            // This will send remote messages.
            fut.map();

            return new GridDhtEmbeddedFuture<>(
                ctx.kernalContext(),
                fut,
                new C2<Boolean, Exception, Exception>() {
                    @Override public Exception apply(Boolean b, Exception e) {
                        if (e != null)
                            e = U.unwrap(e);
                        else if (!b)
                            e = new GridCacheLockTimeoutException(ver);

                        return e;
                    }
                });
        }
        else {
            // Handle implicit locks for pessimistic transactions.
            ctx.tm().txContext(tx);

            if (log.isDebugEnabled())
                log.debug("Performing colocated lock [tx=" + tx + ", keys=" + keys + ']');

            GridFuture<GridCacheReturn<V>> txFut = tx.lockAllAsync(keys, tx.implicit(), txRead);

            return new GridDhtEmbeddedFuture<>(
                ctx.kernalContext(),
                txFut,
                new C2<GridCacheReturn<V>, Exception, Exception>() {
                    @Override public Exception apply(GridCacheReturn<V> ret,
                        Exception e) {
                        if (e != null)
                            e = U.unwrap(e);

                        assert !tx.empty();

                        return e;
                    }
                });
        }
    }

    /**
     * @param commit Commit flag (rollback if {@code false}).
     * @param explicitLock Explicit lock flag.
     * @param tx Transaction to commit.
     * @return Future.
     */
    GridFuture<GridCacheTx> finishLocal(boolean commit, boolean explicitLock, GridDhtColocatedTxLocal<K, V> tx) {
        try {
            if (commit) {
                if (!tx.markFinalizing(USER_FINISH)) {
                    if (log.isDebugEnabled())
                        log.debug("Will not finish transaction (it is handled by another thread): " + tx);

                    return null;
                }

                return tx.commitAsyncLocal();
            }
            else
                return tx.rollbackAsyncLocal();
        }
        catch (Throwable e) {
            U.error(log, "Failed completing transaction [commit=" + commit + ", tx=" + tx + ']', e);

            if (tx != null)
                return tx.rollbackAsync();

            return new GridFinishedFuture<>(ctx.kernalContext(), e);
        }
    }

    /**
     * @param nodeId Sender ID.
     * @param res Response.
     */
    private void processGetResponse(UUID nodeId, GridNearGetResponse<K, V> res) {
        GridPartitionedGetFuture<K, V> fut = (GridPartitionedGetFuture<K, V>)ctx.mvcc().<Map<K, V>>future(
            res.version(), res.futureId());

        if (fut == null) {
            if (log.isDebugEnabled())
                log.debug("Failed to find future for get response [sender=" + nodeId + ", res=" + res + ']');

            return;
        }

        fut.onResult(nodeId, res);
    }

    /**
     * @param nodeId Node ID.
     * @param res Response.
     */
    private void processPrepareResponse(UUID nodeId, GridNearTxPrepareResponse<K, V> res) {
        GridDhtColocatedTxPrepareFuture<K, V> fut = (GridDhtColocatedTxPrepareFuture<K, V>)ctx.mvcc().
            <GridCacheTxEx<K, V>>future(res.version(), res.futureId());

        if (fut == null) {
            if (log.isDebugEnabled())
                log.debug("Failed to find future for prepare response [sender=" + nodeId + ", res=" + res + ']');

            return;
        }

        fut.onResult(nodeId, res);
    }

    /**
     * @param nodeId Node ID.
     * @param res Response.
     */
    private void processFinishResponse(UUID nodeId, GridNearTxFinishResponse<K, V> res) {
        ctx.tm().onFinishedRemote(nodeId, res.threadId());

        GridDhtColocatedTxFinishFuture<K, V> fut = (GridDhtColocatedTxFinishFuture<K, V>)ctx.mvcc()
            .<GridCacheTx>future(res.xid(), res.futureId());

        if (fut == null) {
            if (log.isDebugEnabled())
                log.debug("Failed to find future for finish response [sender=" + nodeId + ", res=" + res + ']');

            return;
        }

        fut.onResult(nodeId, res);
    }

    /**
     * @param nodeId Node ID.
     * @param res Response.
     */
    private void processLockResponse(UUID nodeId, GridNearLockResponse<K, V> res) {
        assert nodeId != null;
        assert res != null;

        GridDhtColocatedLockFuture<K, V> fut = (GridDhtColocatedLockFuture<K, V>)ctx.mvcc().
            <Boolean>future(res.version(), res.futureId());

        if (fut != null)
            fut.onResult(nodeId, res);
    }
}
