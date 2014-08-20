/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.distributed.near;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.kernal.processors.cache.distributed.*;
import org.gridgain.grid.kernal.processors.cache.distributed.dht.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.security.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.grid.util.future.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;

import static org.gridgain.grid.kernal.processors.cache.GridCacheTxEx.FinalizationStatus.*;
import static org.gridgain.grid.cache.GridCacheFlag.*;
import static org.gridgain.grid.cache.GridCacheTxConcurrency.*;

/**
 * Near cache for transactional cache.
 */
public class GridNearTransactionalCache<K, V> extends GridNearCacheAdapter<K, V> {
    /** */
    private static final long serialVersionUID = 0L;

    /** DHT cache. */
    private GridDhtCache<K, V> dht;

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    public GridNearTransactionalCache() {
        // No-op.
    }

    /**
     * @param ctx Context.
     */
    public GridNearTransactionalCache(GridCacheContext<K, V> ctx) {
        super(ctx);
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

    /** {@inheritDoc} */
    @Override public void dgc() {
        ctx.dgc().dgc();
    }

    /** {@inheritDoc} */
    @Override public void dgc(long suspectLockTimeout, boolean global, boolean rmvLocks) {
        ctx.dgc().dgc(suspectLockTimeout, global, rmvLocks);
    }

    /**
     * @param dht DHT cache.
     */
    public void dht(GridDhtCache<K, V> dht) {
        this.dht = dht;
    }

    /** {@inheritDoc} */
    @Override public GridDhtCache<K, V> dht() {
        return dht;
    }

    /** {@inheritDoc} */
    @Override public GridFuture<Map<K, V>> getAllAsync(
        @Nullable final Collection<? extends K> keys,
        boolean forcePrimary,
        boolean skipTx,
        @Nullable final GridCacheEntryEx<K, V> entry,
        @Nullable UUID subjId,
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

        subjId = ctx.subjectIdPerCall(subjId);

        return loadAsync(null, keys, false, forcePrimary, filter, subjId, deserializePortable);
    }

    /**
     * @param tx Transaction.
     * @param keys Keys to load.
     * @param filter Filter.
     * @return Future.
     */
    GridFuture<Map<K, V>> txLoadAsync(GridNearTxLocal<K, V> tx, @Nullable Collection<? extends K> keys,
        @Nullable GridPredicate<GridCacheEntry<K, V>>[] filter, boolean deserializePortable) {
        assert tx != null;

        GridNearGetFuture<K, V> fut = new GridNearGetFuture<>(ctx, keys, false, false, tx, filter,
            CU.subjectId(tx, ctx), deserializePortable);

        // init() will register future for responses if it has remote mappings.
        fut.init();

        return fut;
    }

    /**
     * @param nodeId Node ID.
     * @param req Request.
     */
    @SuppressWarnings({"RedundantTypeArguments"})
    public void clearLocks(UUID nodeId, GridDhtUnlockRequest<K, V> req) {
        assert nodeId != null;

        GridCacheVersion obsoleteVer = ctx.versions().next();

        List<K> keys = req.nearKeys();

        if (keys != null) {
            long topVer = ctx.affinity().affinityTopologyVersion();

            for (K key : keys) {
                while (true) {
                    GridDistributedCacheEntry<K, V> entry = peekExx(key);

                    try {
                        if (entry != null) {
                            entry.doneRemote(
                                req.version(),
                                req.version(),
                                null,
                                req.committedVersions(),
                                req.rolledbackVersions(),
                                /*system invalidate*/false);

                            // Note that we don't reorder completed versions here,
                            // as there is no point to reorder relative to the version
                            // we are about to remove.
                            if (entry.removeLock(req.version())) {
                                if (log.isDebugEnabled())
                                    log.debug("Removed lock [lockId=" + req.version() + ", key=" + key + ']');

                                // Try to evict near entry dht-mapped locally.
                                evictNearEntry(entry, obsoleteVer, topVer);
                            }
                            else {
                                if (log.isDebugEnabled())
                                    log.debug("Received unlock request for unknown candidate " +
                                        "(added to cancelled locks set): " + req);
                            }
                        }
                        else if (log.isDebugEnabled())
                            log.debug("Received unlock request for entry that could not be found: " + req);

                        ctx.evicts().touch(entry, topVer);

                        break;
                    }
                    catch (GridCacheEntryRemovedException ignored) {
                        if (log.isDebugEnabled())
                            log.debug("Received remove lock request for removed entry (will retry) [entry=" + entry +
                                ", req=" + req + ']');
                    }
                }
            }
        }
    }

    /**
     * Called while processing dht tx prepare request.
     *
     * @param ldr Loader.
     * @param nodeId Sender node ID.
     * @param req Request.
     * @return Remote transaction.
     * @throws GridException If failed.
     */
    @Nullable public GridNearTxRemote<K, V> startRemoteTx(ClassLoader ldr, UUID nodeId,
        GridDhtTxPrepareRequest<K, V> req) throws GridException {
        assert F.isEmpty(req.candidatesByKey());

        if (!F.isEmpty(req.nearWrites())) {
            GridNearTxRemote<K, V> tx = ctx.tm().tx(req.version());

            if (tx == null) {
                tx = new GridNearTxRemote<>(
                    ldr,
                    nodeId,
                    req.nearNodeId(),
                    req.threadId(),
                    req.version(),
                    req.commitVersion(),
                    req.concurrency(),
                    req.isolation(),
                    req.isInvalidate(),
                    req.timeout(),
                    req.nearWrites(),
                    ctx,
                    req.txSize(),
                    req.groupLockKey(),
                    req.subjectId()
                );

                if (!tx.empty()) {
                    tx = ctx.tm().onCreated(tx);

                    if (tx == null || !ctx.tm().onStarted(tx))
                        throw new GridCacheTxRollbackException("Attempt to start a completed transaction: " + tx);
                }
            }
            else
                tx.addEntries(ldr, req.nearWrites());

            tx.ownedVersions(req.owned());

            // Prepare prior to reordering, so the pending locks added
            // in prepare phase will get properly ordered as well.
            tx.prepare();

            return tx;
        }

        return null;
    }

    /**
     * @param nodeId Primary node ID.
     * @param req Request.
     * @return Remote transaction.
     * @throws GridException If failed.
     * @throws GridDistributedLockCancelledException If lock has been cancelled.
     */
    @SuppressWarnings({"RedundantTypeArguments"})
    @Nullable public GridNearTxRemote<K, V> startRemoteTx(UUID nodeId, GridDhtLockRequest<K, V> req)
        throws GridException, GridDistributedLockCancelledException {
        List<K> nearKeys = req.nearKeys();
        List<byte[]> keyBytes = req.nearKeyBytes();

        assert keyBytes != null;

        GridNearTxRemote<K, V> tx = null;

        ClassLoader ldr = ctx.deploy().globalLoader();

        if (ldr != null) {
            Collection<T2<K, byte[]>> evicted = null;

            for (int i = 0; i < nearKeys.size(); i++) {
                K key = nearKeys.get(i);

                if (key == null)
                    continue;

                byte[] bytes = !keyBytes.isEmpty() ? keyBytes.get(i) : null;

                Collection<GridCacheMvccCandidate<K>> cands = req.candidatesByIndex(i);
                GridCacheVersion drVer = req.drVersionByIndex(i);

                if (log.isDebugEnabled())
                    log.debug("Unmarshalled key: " + key);

                GridNearCacheEntry<K, V> entry = null;

                while (true) {
                    try {
                        entry = peekExx(key);

                        if (entry != null) {
                            entry.keyBytes(bytes);

                            // Handle implicit locks for pessimistic transactions.
                            if (req.inTx()) {
                                tx = ctx.tm().tx(req.version());

                                if (tx != null)
                                    tx.addWrite(key, bytes, null/*Value.*/, null/*Value bytes.*/, drVer);
                                else {
                                    tx = new GridNearTxRemote<>(
                                        nodeId,
                                        req.nearNodeId(),
                                        req.nearXidVersion(),
                                        req.threadId(),
                                        req.version(),
                                        null,
                                        PESSIMISTIC,
                                        req.isolation(),
                                        req.isInvalidate(),
                                        req.timeout(),
                                        key,
                                        bytes,
                                        null, // Value.
                                        null, // Value bytes.
                                        drVer,
                                        ctx,
                                        req.txSize(),
                                        req.groupLockKey(),
                                        req.subjectId()
                                    );

                                    if (req.groupLock())
                                        tx.groupLockKey(key);

                                    if (tx.empty()) {
                                        if (evicted == null)
                                            evicted = new LinkedList<>();

                                        evicted.add(new T2<>(key, bytes));

                                        tx = null;

                                        break; // While.
                                    }

                                    tx = ctx.tm().onCreated(tx);

                                    if (tx == null || !ctx.tm().onStarted(tx))
                                        throw new GridCacheTxRollbackException("Failed to acquire lock " +
                                            "(transaction has been completed): " + req.version());
                                }
                            }

                            // Add remote candidate before reordering.
                            // Owned candidates should be reordered inside entry lock.
                            entry.addRemote(
                                req.nodeId(),
                                nodeId,
                                req.threadId(),
                                req.version(),
                                req.timeout(),
                                tx != null,
                                tx != null && tx.implicitSingle(),
                                req.owned(entry.key())
                            );

                            assert cands.isEmpty() : "Received non-empty candidates in dht lock request: " + cands;

                            if (!req.inTx())
                                ctx.evicts().touch(entry, req.topologyVersion());
                        }
                        else {
                            if (evicted == null)
                                evicted = new LinkedList<>();

                            evicted.add(new T2<>(key, bytes));
                        }

                        // Double-check in case if sender node left the grid.
                        if (ctx.discovery().node(req.nodeId()) == null) {
                            if (log.isDebugEnabled())
                                log.debug("Node requesting lock left grid (lock request will be ignored): " + req);

                            if (tx != null)
                                tx.rollback();

                            return null;
                        }

                        // Entry is legit.
                        break;
                    }
                    catch (GridCacheEntryRemovedException ignored) {
                        assert entry.obsoleteVersion() != null : "Obsolete flag not set on removed entry: " +
                            entry;

                        if (log.isDebugEnabled())
                            log.debug("Received entry removed exception (will retry on renewed entry): " + entry);

                        if (tx != null) {
                            tx.clearEntry(entry.key());

                            if (log.isDebugEnabled())
                                log.debug("Cleared removed entry from remote transaction (will retry) [entry=" +
                                    entry + ", tx=" + tx + ']');
                        }
                    }
                }
            }

            if (tx != null && evicted != null) {
                assert !evicted.isEmpty();

                for (T2<K, byte[]> evict : evicted)
                    tx.addEvicted(evict.get1(), evict.get2());
            }
        }
        else {
            String err = "Failed to acquire deployment class loader for message: " + req;

            U.warn(log, err);

            throw new GridException(err);
        }

        return tx;
    }

    /**
     * @param nodeId Primary node ID.
     * @param req Request.
     * @return Remote transaction.
     * @throws GridException If failed.
     * @throws GridDistributedLockCancelledException If lock has been cancelled.
     */
    @SuppressWarnings({"RedundantTypeArguments"})
    @Nullable public GridNearTxRemote<K, V> startRemoteTxForFinish(UUID nodeId, GridDhtTxFinishRequest<K, V> req)
        throws GridException, GridDistributedLockCancelledException {
        assert req.groupLock();

        GridNearTxRemote<K, V> tx = null;

        ClassLoader ldr = ctx.deploy().globalLoader();

        if (ldr != null) {
            boolean marked = false;

            for (GridCacheTxEntry<K, V> txEntry : req.nearWrites()) {
                GridDistributedCacheEntry<K, V> entry = null;

                while (true) {
                    try {
                        entry = peekExx(txEntry.key());

                        if (entry != null) {
                            entry.keyBytes(txEntry.keyBytes());

                            // Handle implicit locks for pessimistic transactions.
                            tx = ctx.tm().tx(req.version());

                            if (tx != null) {
                                if (tx.local())
                                    return null;

                                if (!marked)
                                    marked = tx.markFinalizing(USER_FINISH);

                                if (marked)
                                    tx.addWrite(txEntry.key(), txEntry.keyBytes(), txEntry.op(), txEntry.value(),
                                        txEntry.valueBytes(), txEntry.drVersion());
                                else
                                    return null;
                            }
                            else {
                                tx = new GridNearTxRemote<>(
                                    nodeId,
                                    req.nearNodeId(),
                                    // We can pass null as nearXidVer as transaction will be committed right away.
                                    null,
                                    req.threadId(),
                                    req.version(),
                                    null,
                                    PESSIMISTIC,
                                    req.isolation(),
                                    req.isInvalidate(),
                                    0,
                                    txEntry.key(),
                                    txEntry.keyBytes(),
                                    txEntry.value(),
                                    txEntry.valueBytes(),
                                    txEntry.drVersion(),
                                    ctx,
                                    req.txSize(),
                                    req.groupLockKey(),
                                    req.subjectId());

                                if (tx.empty())
                                    return tx;

                                tx = ctx.tm().onCreated(tx);

                                if (tx == null || !ctx.tm().onStarted(tx))
                                    throw new GridCacheTxRollbackException("Failed to acquire lock " +
                                        "(transaction has been completed): " + req.version());

                                if (!marked)
                                    marked = tx.markFinalizing(USER_FINISH);

                                if (!marked)
                                    return null;
                            }

                            if (req.groupLock()) {
                                tx.markGroupLock();

                                if (!txEntry.groupLockEntry())
                                    tx.groupLockKey(txEntry.key());
                            }

                            // Add remote candidate before reordering.
                            if (txEntry.explicitVersion() == null && !txEntry.groupLockEntry())
                                entry.addRemote(
                                    req.nearNodeId(),
                                    nodeId,
                                    req.threadId(),
                                    req.version(),
                                    0,
                                    /*tx*/true,
                                    tx.implicitSingle(),
                                    null
                                );
                        }

                        // Double-check in case if sender node left the grid.
                        if (ctx.discovery().node(req.nearNodeId()) == null) {
                            if (log.isDebugEnabled())
                                log.debug("Node requesting lock left grid (lock request will be ignored): " + req);

                            if (tx != null)
                                tx.rollback();

                            return null;
                        }

                        // Entry is legit.
                        break;
                    }
                    catch (GridCacheEntryRemovedException ignored) {
                        assert entry.obsoleteVersion() != null : "Obsolete flag not set on removed entry: " +
                            entry;

                        if (log.isDebugEnabled())
                            log.debug("Received entry removed exception (will retry on renewed entry): " + entry);

                        if (tx != null) {
                            tx.clearEntry(entry.key());

                            if (log.isDebugEnabled())
                                log.debug("Cleared removed entry from remote transaction (will retry) [entry=" +
                                    entry + ", tx=" + tx + ']');
                        }
                    }
                }
            }
        }
        else {
            String err = "Failed to acquire deployment class loader for message: " + req;

            U.warn(log, err);

            throw new GridException(err);
        }

        return tx;
    }

    /**
     * @param nodeId Node ID.
     * @param res Response.
     */
    private void processPrepareResponse(UUID nodeId, GridNearTxPrepareResponse<K, V> res) {
        GridNearTxPrepareFuture<K, V> fut = (GridNearTxPrepareFuture<K, V>)ctx.mvcc().<GridCacheTxEx<K, V>>future(
            res.version(), res.futureId());

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

        GridNearTxFinishFuture<K, V> fut = (GridNearTxFinishFuture<K, V>)ctx.mvcc().<GridCacheTx>future(
            res.xid(), res.futureId());

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

        GridNearLockFuture<K, V> fut = (GridNearLockFuture<K, V>)ctx.mvcc().<Boolean>future(res.version(),
            res.futureId());

        if (fut != null)
            fut.onResult(nodeId, res);
    }

    /** {@inheritDoc} */
    @Override public GridCacheTxLocalAdapter<K, V> newTx(boolean implicit, boolean implicitSingle,
        GridCacheTxConcurrency concurrency, GridCacheTxIsolation isolation, long timeout, boolean invalidate,
        boolean syncCommit, boolean syncRollback, boolean swapOrOffheapEnabled, boolean storeEnabled, int txSize,
        @Nullable Object grpLockKey, boolean partLock) {
        // Use null as subject ID for transactions if subject per call is not set.
        GridCacheProjectionImpl<K, V> prj = ctx.projectionPerCall();

        UUID subjId = prj == null ? null : prj.subjectId();

        return new GridNearTxLocal<>(ctx, implicit, implicitSingle, concurrency, isolation, timeout,
            invalidate, syncCommit, syncRollback, swapOrOffheapEnabled, storeEnabled, txSize, grpLockKey, partLock, subjId);
    }

    /** {@inheritDoc} */
    @Override protected GridFuture<Boolean> lockAllAsync(Collection<? extends K> keys, long timeout,
        GridCacheTxLocalEx<K, V> tx, boolean isInvalidate, boolean isRead, boolean retval,
        GridCacheTxIsolation isolation, GridPredicate<GridCacheEntry<K, V>>[] filter) {
        GridNearLockFuture<K, V> fut = new GridNearLockFuture<>(ctx, keys, (GridNearTxLocal<K, V>)tx, isRead,
            retval, timeout, filter);

        if (!ctx.mvcc().addFuture(fut))
            throw new IllegalStateException("Duplicate future ID: " + fut);

        fut.map();

        return fut;
    }

    /**
     * @param e Transaction entry.
     * @return {@code True} if entry is locally mapped as a primary or back up node.
     */
    protected boolean isNearLocallyMapped(GridCacheEntryEx<K, V> e, long topVer) {
        return F.contains(ctx.affinity().nodes(e.key(), topVer), ctx.localNode());
    }

    /**
     *
     * @param e Entry to evict if it qualifies for eviction.
     * @param obsoleteVer Obsolete version.
     * @param topVer Topology version.
     * @return {@code True} if attempt was made to evict the entry.
     */
    protected boolean evictNearEntry(GridCacheEntryEx<K, V> e, GridCacheVersion obsoleteVer, long topVer) {
        assert e != null;
        assert obsoleteVer != null;

        if (isNearLocallyMapped(e, topVer)) {
            if (log.isDebugEnabled())
                log.debug("Evicting dht-local entry from near cache [entry=" + e + ", tx=" + this + ']');

            if (e.markObsolete(obsoleteVer))
                return true;
        }

        return false;
    }

    /** {@inheritDoc} */
    @Override public void unlockAll(Collection<? extends K> keys, GridPredicate<GridCacheEntry<K, V>>[] filter) {
        if (keys.isEmpty())
            return;

        try {
            GridCacheVersion ver = null;

            int keyCnt = -1;

            Map<GridNode, GridNearUnlockRequest<K, V>> map = null;

            Collection<K> locKeys = new LinkedList<>();

            for (K key : keys) {
                while (true) {
                    GridDistributedCacheEntry<K, V> entry = peekExx(key);

                    if (entry == null || !ctx.isAll(entry.wrap(false), filter))
                        break; // While.

                    try {
                        GridCacheMvccCandidate<K> cand = entry.candidate(ctx.nodeId(), Thread.currentThread().getId());

                        long topVer = -1;

                        if (cand != null) {
                            assert cand.nearLocal() : "Got non-near-local candidate in near cache: " + cand;

                            ver = cand.version();

                            if (map == null) {
                                Collection<GridNode> affNodes = CU.allNodes(ctx, cand.topologyVersion());

                                if (F.isEmpty(affNodes))
                                    return;

                                keyCnt = (int)Math.ceil((double)keys.size() / affNodes.size());

                                map = new HashMap<>(affNodes.size());
                            }

                            topVer = cand.topologyVersion();

                            // Send request to remove from remote nodes.
                            GridNode primary = ctx.affinity().primary(key, topVer);

                            GridNearUnlockRequest<K, V> req = map.get(primary);

                            if (req == null) {
                                map.put(primary, req = new GridNearUnlockRequest<>(keyCnt));

                                req.version(ver);
                            }

                            // Remove candidate from local node first.
                            GridCacheMvccCandidate<K> rmv = entry.removeLock();

                            if (rmv != null) {
                                if (!rmv.reentry()) {
                                    if (ver != null && !ver.equals(rmv.version()))
                                        throw new GridException("Failed to unlock (if keys were locked separately, " +
                                            "then they need to be unlocked separately): " + keys);

                                    if (!primary.isLocal()) {
                                        assert req != null;

                                        req.addKey(
                                            entry.key(),
                                            entry.getOrMarshalKeyBytes(),
                                            ctx);
                                    }
                                    else
                                        locKeys.add(key);

                                    if (log.isDebugEnabled())
                                        log.debug("Removed lock (will distribute): " + rmv);
                                }
                                else if (log.isDebugEnabled())
                                    log.debug("Current thread still owns lock (or there are no other nodes)" +
                                        " [lock=" + rmv + ", curThreadId=" + Thread.currentThread().getId() + ']');
                            }
                        }

                        assert topVer != -1 || cand == null;

                        if (topVer == -1)
                            topVer = ctx.affinity().affinityTopologyVersion();

                        ctx.evicts().touch(entry, topVer);

                        break;
                    }
                    catch (GridCacheEntryRemovedException ignore) {
                        if (log.isDebugEnabled())
                            log.debug("Attempted to unlock removed entry (will retry): " + entry);
                    }
                }
            }

            if (ver == null)
                return;

            for (Map.Entry<GridNode, GridNearUnlockRequest<K, V>> mapping : map.entrySet()) {
                GridNode n = mapping.getKey();

                GridDistributedUnlockRequest<K, V> req = mapping.getValue();

                if (n.isLocal())
                    dht.removeLocks(ctx.nodeId(), req.version(), locKeys, true);
                else if (!F.isEmpty(req.keyBytes()) || !F.isEmpty(req.keys()))
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
     * @param ver Lock version.
     * @param keys Keys.
     */
    @SuppressWarnings({"unchecked"})
    public void removeLocks(GridCacheVersion ver, Collection<? extends K> keys) {
        if (keys.isEmpty())
            return;

        try {
            int keyCnt = -1;

            Map<GridNode, GridNearUnlockRequest<K, V>> map = null;

            for (K key : keys) {
                // Send request to remove from remote nodes.
                GridNearUnlockRequest<K, V> req = null;

                while (true) {
                    GridDistributedCacheEntry<K, V> entry = peekExx(key);

                    try {
                        if (entry != null) {
                            GridCacheMvccCandidate<K> cand = entry.candidate(ver);

                            if (cand != null) {
                                if (map == null) {
                                    Collection<GridNode> affNodes = CU.allNodes(ctx, cand.topologyVersion());

                                    if (F.isEmpty(affNodes))
                                        return;

                                    keyCnt = (int)Math.ceil((double)keys.size() / affNodes.size());

                                    map = new HashMap<>(affNodes.size());
                                }

                                GridNode primary = ctx.affinity().primary(key, cand.topologyVersion());

                                if (!primary.isLocal()) {
                                    req = map.get(primary);

                                    if (req == null) {
                                        map.put(primary, req = new GridNearUnlockRequest<>(keyCnt));

                                        req.version(ver);
                                    }
                                }

                                // Remove candidate from local node first.
                                if (entry.removeLock(cand.version())) {
                                    if (primary.isLocal()) {
                                        dht.removeLocks(primary.id(), ver, F.asList(key), true);

                                        assert req == null;

                                        continue;
                                    }

                                    req.addKey(
                                        entry.key(),
                                        entry.getOrMarshalKeyBytes(),
                                        ctx);
                                }
                            }
                        }

                        break;
                    }
                    catch (GridCacheEntryRemovedException ignored) {
                        if (log.isDebugEnabled())
                            log.debug("Attempted to remove lock from removed entry (will retry) [rmvVer=" +
                                ver + ", entry=" + entry + ']');
                    }
                }
            }

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

    /** {@inheritDoc} */
    @Override public void onDeferredDelete(GridCacheEntryEx<K, V> entry, GridCacheVersion ver) {
        assert false : "Should not be called";
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridNearTransactionalCache.class, this);
    }
}
