/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.distributed.dht;

import org.gridgain.grid.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.kernal.processors.cache.distributed.*;
import org.gridgain.grid.kernal.processors.cache.distributed.near.*;
import org.gridgain.grid.kernal.processors.dr.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.grid.util.*;
import org.gridgain.grid.util.future.*;
import org.gridgain.grid.util.tostring.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.*;

import static org.gridgain.grid.cache.GridCacheTxState.*;
import static org.gridgain.grid.events.GridEventType.*;

/**
 *
 */
public final class GridDhtTxPrepareFuture<K, V> extends GridCompoundIdentityFuture<GridCacheTxEx<K, V>>
    implements GridCacheMvccFuture<K, V, GridCacheTxEx<K, V>> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Logger reference. */
    private static final AtomicReference<GridLogger> logRef = new AtomicReference<>();

    /** Context. */
    private GridCacheSharedContext<K, V> cctx;

    /** Future ID. */
    private GridUuid futId;

    /** Transaction. */
    @GridToStringExclude
    private GridDhtTxLocalAdapter<K, V> tx;

    /** Near mappings. */
    private Map<UUID, GridDistributedTxMapping<K, V>> nearMap;

    /** DHT mappings. */
    private Map<UUID, GridDistributedTxMapping<K, V>> dhtMap;

    /** Logger. */
    private GridLogger log;

    /** Error. */
    private AtomicReference<Throwable> err = new AtomicReference<>(null);

    /** Replied flag. */
    private AtomicBoolean replied = new AtomicBoolean(false);

    /** All replies flag. */
    private AtomicBoolean mapped = new AtomicBoolean(false);

    /** Prepare reads. */
    private Iterable<GridCacheTxEntry<K, V>> reads;

    /** Prepare writes. */
    private Iterable<GridCacheTxEntry<K, V>> writes;

    /** Tx nodes. */
    private Map<UUID, Collection<UUID>> txNodes;

    /** Trackable flag. */
    private boolean trackable = true;

    /** Near mini future id. */
    private GridUuid nearMiniId;

    /** DHT versions map. */
    private Map<K, GridCacheVersion> dhtVerMap;

    /** {@code True} if this is last prepare operation for node. */
    private boolean last;

    /** IDs of backup nodes receiving last prepare request during this prepare. */
    private Collection<UUID> lastBackups;

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    public GridDhtTxPrepareFuture() {
        // No-op.
    }

    /**
     * @param cctx Context.
     * @param tx Transaction.
     * @param nearMiniId Near mini future id.
     * @param dhtVerMap DHT versions map.
     * @param last {@code True} if this is last prepare operation for node.
     * @param lastBackups IDs of backup nodes receiving last prepare request during this prepare.
     */
    public GridDhtTxPrepareFuture(GridCacheSharedContext<K, V> cctx, final GridDhtTxLocalAdapter<K, V> tx,
        GridUuid nearMiniId, Map<K, GridCacheVersion> dhtVerMap, boolean last, Collection<UUID> lastBackups) {
        super(cctx.kernalContext(), new GridReducer<GridCacheTxEx<K, V>, GridCacheTxEx<K, V>>() {
            @Override public boolean collect(GridCacheTxEx<K, V> e) {
                return true;
            }

            @Override public GridCacheTxEx<K, V> reduce() {
                // Nothing to aggregate.
                return tx;
            }
        });

        assert cctx != null;

        this.cctx = cctx;
        this.tx = tx;
        this.dhtVerMap = dhtVerMap;
        this.last = last;
        this.lastBackups = lastBackups;

        futId = GridUuid.randomUuid();

        this.nearMiniId = nearMiniId;

        log = U.logger(ctx, logRef, GridDhtTxPrepareFuture.class);

        dhtMap = tx.dhtMap();
        nearMap = tx.nearMap();

        assert dhtMap != null;
        assert nearMap != null;
    }

    /** {@inheritDoc} */
    @Override public GridUuid futureId() {
        return futId;
    }

    /**
     * @return Near mini future id.
     */
    public GridUuid nearMiniId() {
        return nearMiniId;
    }

    /** {@inheritDoc} */
    @Override public GridCacheVersion version() {
        return tx.xidVersion();
    }

    /**
     * @return Involved nodes.
     */
    @Override public Collection<? extends GridNode> nodes() {
        return
            F.viewReadOnly(futures(), new GridClosure<GridFuture<?>, GridNode>() {
                @Nullable @Override public GridNode apply(GridFuture<?> f) {
                    if (isMini(f))
                        return ((MiniFuture)f).node();

                    return cctx.discovery().localNode();
                }
            });
    }

    /** {@inheritDoc} */
    @Override public boolean onOwnerChanged(GridCacheEntryEx<K, V> entry, GridCacheMvccCandidate<K> owner) {
        if (log.isDebugEnabled())
            log.debug("Transaction future received owner changed callback: " + entry);

        boolean ret = tx.hasWriteKey(entry.txKey());

        return ret && mapIfLocked();
    }

    /** {@inheritDoc} */
    @Override public boolean trackable() {
        return trackable;
    }

    /** {@inheritDoc} */
    @Override public void markNotTrackable() {
        trackable = false;
    }

    /**
     * @return Transaction.
     */
    GridDhtTxLocalAdapter<K, V> tx() {
        return tx;
    }

    /**
     * @return {@code True} if all locks are owned.
     */
    private boolean checkLocks() {
        for (GridCacheTxEntry<K, V> txEntry : tx.optimisticLockEntries()) {
            while (true) {
                GridCacheEntryEx<K, V> cached = txEntry.cached();

                try {
                    // Don't compare entry against itself.
                    if (!cached.lockedLocally(tx.xidVersion())) {
                        if (log.isDebugEnabled())
                            log.debug("Transaction entry is not locked by transaction (will wait) [entry=" + cached +
                                ", tx=" + tx + ']');

                        return false;
                    }

                    break; // While.
                }
                // Possible if entry cached within transaction is obsolete.
                catch (GridCacheEntryRemovedException ignored) {
                    if (log.isDebugEnabled())
                        log.debug("Got removed entry in future onAllReplies method (will retry): " + txEntry);

                    txEntry.cached(txEntry.context().cache().entryEx(txEntry.key().key()), txEntry.keyBytes());
                }
            }
        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean onNodeLeft(UUID nodeId) {
        for (GridFuture<?> fut : futures())
            if (isMini(fut)) {
                MiniFuture f = (MiniFuture)fut;

                if (f.node().id().equals(nodeId)) {
                    f.onResult(new GridTopologyException("Remote node left grid (will retry): " + nodeId));

                    return true;
                }
            }

        return false;
    }

    /**
     * @param t Error.
     */
    public void onError(Throwable t) {
        if (err.compareAndSet(null, t)) {
            tx.setRollbackOnly();

            // TODO: GG-4005:
            // TODO: as an improvement, at some point we must rollback right away.
            // TODO: However, in this case need to make sure that reply is sent back
            // TODO: even for non-existing transactions whenever finish request comes in.
//            try {
//                tx.rollback();
//            }
//            catch (GridException ex) {
//                U.error(log, "Failed to automatically rollback transaction: " + tx, ex);
//            }
//
            // If not local node.
            if (!tx.nearNodeId().equals(cctx.localNodeId())) {
                // Send reply back to near node.
                GridCacheMessage<K, V> res = new GridNearTxPrepareResponse<>(tx.nearXidVersion(), tx.nearFutureId(),
                    nearMiniId, tx.xidVersion(), Collections.<Integer>emptySet(), t);

                try {
                    cctx.io().send(tx.nearNodeId(), res);
                }
                catch (GridException e) {
                    U.error(log, "Failed to send reply to originating near node (will rollback): " + tx.nearNodeId(), e);

                    tx.rollbackAsync();
                }
            }

            onComplete();
        }
    }

    /**
     * @param nodeId Sender.
     * @param res Result.
     */
    void onResult(UUID nodeId, GridDhtTxPrepareResponse<K, V> res) {
        if (!isDone()) {
            for (GridFuture<GridCacheTxEx<K, V>> fut : pending()) {
                if (isMini(fut)) {
                    MiniFuture f = (MiniFuture)fut;

                    if (f.futureId().equals(res.miniId())) {
                        assert f.node().id().equals(nodeId);

                        f.onResult(res);

                        break;
                    }
                }
            }
        }
    }

    /**
     * Marks all locks as ready for local transaction.
     */
    private void readyLocks() {
        // Ready all locks.
        if (log.isDebugEnabled())
            log.debug("Marking all local candidates as ready: " + this);

        Collection<GridCacheTxEntry<K, V>> checkEntries = tx.groupLock() ?
            Collections.singletonList(tx.groupLockEntry()) : tx.writeEntries();

        for (GridCacheTxEntry<K, V> txEntry : checkEntries) {
            while (true) {
                GridDistributedCacheEntry<K, V> entry = (GridDistributedCacheEntry<K, V>)txEntry.cached();

                try {
                    GridCacheMvccCandidate<K> c = entry.readyLock(tx.xidVersion());

                    if (log.isDebugEnabled())
                        log.debug("Current lock owner for entry [owner=" + c + ", entry=" + entry + ']');

                    break; // While.
                }
                // Possible if entry cached within transaction is obsolete.
                catch (GridCacheEntryRemovedException ignored) {
                    if (log.isDebugEnabled())
                        log.debug("Got removed entry in future onAllReplies method (will retry): " + txEntry);

                    txEntry.cached(txEntry.context().cache().entryEx(txEntry.key().key()), txEntry.keyBytes());
                }
            }
        }
    }

    /**
     * Checks if all ready locks are acquired and sends requests to remote nodes in this case.
     *
     * @return {@code True} if all locks are acquired, {@code false} otherwise.
     */
    private boolean mapIfLocked() {
        if (checkLocks()) {
            prepare0();

            return true;
        }

        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean onDone(GridCacheTxEx<K, V> tx0, Throwable err) {
        assert err != null || (initialized() && !hasPending()) : "On done called for prepare future that has " +
            "pending mini futures: " + this;

        this.err.compareAndSet(null, err);

        if (replied.compareAndSet(false, true)) {
            try {
                // Must clear prepare future before response is sent or listeners are notified.
                if (tx.optimistic())
                    tx.clearPrepareFuture(this);

                if (!tx.nearNodeId().equals(cctx.localNodeId())) {
                    // Send reply back to originating near node.
                    GridNearTxPrepareResponse<K, V> res = new GridNearTxPrepareResponse<>(tx.nearXidVersion(),
                        tx.nearFutureId(), nearMiniId, tx.xidVersion(), tx.invalidPartitions(), this.err.get());

                    addDhtValues(res);

                    GridCacheVersion min = tx.minVersion();

                    res.completedVersions(cctx.tm().committedVersions(min), cctx.tm().rolledbackVersions(min));

                    res.pending(localDhtPendingVersions(tx.writeEntries(), min));

                    cctx.io().send(tx.nearNodeId(), res);
                }

                return true;
            }
            catch (GridException e) {
                onError(e);

                return true;
            }
            finally {
                // Will call super.onDone().
                onComplete();
            }
        }
        else {
            // Other thread is completing future. Wait for it to complete.
            try {
                get();
            }
            catch (GridInterruptedException e) {
                onError(new GridException("Got interrupted while waiting for replies to be sent.", e));
            }
            catch (GridException ignored) {
                // No-op, get() was just synchronization.
            }

            return false;
        }
    }

    /**
     * @param res Response being sent.
     */
    private void addDhtValues(GridNearTxPrepareResponse<K, V> res) {
        // Interceptor on near node needs old values to execute callbacks.
        if (cctx.config().getInterceptor() != null && !F.isEmpty(writes)) {
            for (GridCacheTxEntry<K, V> e : writes) {
                GridCacheTxEntry<K, V> txEntry = tx.entry(e.key());

                while (true) {
                    try {
                        GridCacheEntryEx<K, V> entry = txEntry.cached();

                        GridCacheVersion dhtVer = entry.version();

                        V val0 = null;
                        byte[] valBytes0 = null;

                        GridCacheValueBytes valBytesTuple = entry.valueBytes();

                        if (!valBytesTuple.isNull()) {
                            if (valBytesTuple.isPlain())
                                val0 = (V) valBytesTuple.get();
                            else
                                valBytes0 = valBytesTuple.get();
                        }
                        else
                            val0 = entry.rawGet();

                        res.addOwnedValue(txEntry.key(), dhtVer, val0, valBytes0);

                        break;
                    }
                    catch (GridCacheEntryRemovedException ignored) {
                        // Retry.
                    }
                }
            }
        }

        for (Map.Entry<K, GridCacheVersion> ver : dhtVerMap.entrySet()) {
            GridCacheTxEntry<K, V> txEntry = tx.entry(ver.getKey());

            if (res.hasOwnedValue(ver.getKey()))
                continue;

            while (true) {
                try {
                    GridCacheEntryEx<K, V> entry = txEntry.cached();

                    GridCacheVersion dhtVer = entry.version();

                    if (ver.getValue() == null || !ver.getValue().equals(dhtVer)) {
                        V val0 = null;
                        byte[] valBytes0 = null;

                        GridCacheValueBytes valBytesTuple = entry.valueBytes();

                        if (!valBytesTuple.isNull()) {
                            if (valBytesTuple.isPlain())
                                val0 = (V)valBytesTuple.get();
                            else
                                valBytes0 = valBytesTuple.get();
                        }
                        else
                            val0 = entry.rawGet();

                        res.addOwnedValue(txEntry.key(), dhtVer, val0, valBytes0);
                    }

                    break;
                }
                catch (GridCacheEntryRemovedException ignored) {
                    // Retry.
                }
            }
        }
    }

    /**
     * @param f Future.
     * @return {@code True} if mini-future.
     */
    private boolean isMini(GridFuture<?> f) {
        return f.getClass().equals(MiniFuture.class);
    }

    /**
     * Completeness callback.
     *
     * @return {@code True} if {@code done} flag was changed as a result of this call.
     */
    private boolean onComplete() {
        if (last || tx.isSystemInvalidate())
            tx.state(PREPARED);

        if (super.onDone(tx, err.get())) {
            // Don't forget to clean up.
            cctx.mvcc().removeFuture(this);

            return true;
        }

        return false;
    }

    /**
     * Completes this future.
     */
    public void complete() {
        onComplete();
    }

    /**
     * Initializes future.
     *
     * @param reads Read entries.
     * @param writes Write entries.
     * @param txNodes Transaction nodes mapping.
     */
    public void prepare(Iterable<GridCacheTxEntry<K, V>> reads, Iterable<GridCacheTxEntry<K, V>> writes,
        Map<UUID, Collection<UUID>> txNodes) {
        if (tx.empty()) {
            tx.setRollbackOnly();

            onDone(tx);
        }

        this.reads = reads;
        this.writes = writes;
        this.txNodes = txNodes;

        readyLocks();

        mapIfLocked();
    }

    /**
     * @param backupId Backup node ID.
     * @return {@code True} if backup node receives last prepare request for this transaction.
     */
    private boolean lastBackup(UUID backupId) {
        return lastBackups != null && lastBackups.contains(backupId);
    }

    /**
     *
     */
    private void prepare0() {
        if (!mapped.compareAndSet(false, true))
            return;

        try {
            Map<UUID, GridDistributedTxMapping<K, V>> futDhtMap = new HashMap<>();
            Map<UUID, GridDistributedTxMapping<K, V>> futNearMap = new HashMap<>();

            boolean hasRemoteNodes = false;

            // Assign keys to primary nodes.
            if (!F.isEmpty(reads)) {
                for (GridCacheTxEntry<K, V> read : reads)
                    hasRemoteNodes |= map(tx.entry(read.key()), futDhtMap, futNearMap);
            }

            if (!F.isEmpty(writes)) {
                for (GridCacheTxEntry<K, V> write : writes)
                    hasRemoteNodes |= map(tx.entry(write.key()), futDhtMap, futNearMap);
            }

            if (isDone())
                return;

            tx.needsCompletedVersions(hasRemoteNodes);

            // Create mini futures.
            for (GridDistributedTxMapping<K, V> dhtMapping : futDhtMap.values()) {
                assert !dhtMapping.empty();

                GridNode n = dhtMapping.node();

                assert !n.isLocal();

                GridDistributedTxMapping<K, V> nearMapping = futNearMap.get(n.id());

                MiniFuture fut = new MiniFuture(n.id(), dhtMap.get(n.id()), nearMap.get(n.id()));

                add(fut); // Append new future.

                Collection<GridCacheTxEntry<K, V>> nearWrites = nearMapping == null ? null : nearMapping.writes();

                GridDhtTxPrepareRequest<K, V> req = new GridDhtTxPrepareRequest<>(
                    futId,
                    fut.futureId(),
                    tx.topologyVersion(),
                    tx,
                    dhtMapping.writes(),
                    nearWrites,
                    tx.groupLockKey(),
                    tx.partitionLock(),
                    txNodes,
                    tx.nearXidVersion(),
                    lastBackup(n.id()),
                    tx.subjectId(),
                    tx.taskNameHash());

                int idx = 0;

                for (GridCacheTxEntry<K, V> entry : dhtMapping.writes()) {
                    try {
                        GridDhtCacheEntry<K, V> cached = (GridDhtCacheEntry<K, V>)entry.cached();

                        GridCacheMvccCandidate<K> added = cached.candidate(version());

                        assert added != null || entry.groupLockEntry() : "Null candidate for non-group-lock entry " +
                            "[added=" + added + ", entry=" + entry + ']';
                        assert added == null || added.dhtLocal() : "Got non-dht-local candidate for prepare future" +
                            "[added=" + added + ", entry=" + entry + ']';

                        if (added.ownerVersion() != null)
                            req.owned(entry.key(), added.ownerVersion());

                        req.invalidateNearEntry(idx, cached.readerId(n.id()) != null);

                        if (cached.isNewLocked())
                            req.markKeyForPreload(idx);

                        break;
                    }
                    catch (GridCacheEntryRemovedException ignore) {
                        assert false : "Got removed exception on entry with dht local candidate: " + entry;
                    }

                    idx++;
                }

                if (!F.isEmpty(nearWrites)) {
                    for (GridCacheTxEntry<K, V> entry : nearWrites) {
                        try {
                            GridCacheMvccCandidate<K> added = entry.cached().candidate(version());

                            assert added != null;
                            assert added.dhtLocal();

                            if (added.ownerVersion() != null)
                                req.owned(entry.key(), added.ownerVersion());

                            break;
                        }
                        catch (GridCacheEntryRemovedException ignore) {
                            assert false : "Got removed exception on entry with dht local candidate: " + entry;
                        }
                    }
                }

                //noinspection TryWithIdenticalCatches
                try {
                    cctx.io().send(n, req);
                }
                catch (GridTopologyException e) {
                    fut.onResult(e);
                }
                catch (GridException e) {
                    fut.onResult(e);
                }
            }

            for (GridDistributedTxMapping<K, V> nearMapping : futNearMap.values()) {
                if (!futDhtMap.containsKey(nearMapping.node().id())) {
                    assert nearMapping.writes() != null;

                    MiniFuture fut = new MiniFuture(nearMapping.node().id(), null, nearMapping);

                    add(fut); // Append new future.

                    GridDhtTxPrepareRequest<K, V> req = new GridDhtTxPrepareRequest<>(
                        futId,
                        fut.futureId(),
                        tx.topologyVersion(),
                        tx,
                        null,
                        nearMapping.writes(),
                        tx.groupLockKey(),
                        tx.partitionLock(),
                        null,
                        tx.nearXidVersion(),
                        false,
                        tx.subjectId(),
                        tx.taskNameHash());

                    for (GridCacheTxEntry<K, V> entry : nearMapping.writes()) {
                        try {
                            GridCacheMvccCandidate<K> added = entry.cached().candidate(version());

                            assert added != null || entry.groupLockEntry() : "Null candidate for non-group-lock entry " +
                                "[added=" + added + ", entry=" + entry + ']';
                            assert added == null || added.dhtLocal() : "Got non-dht-local candidate for prepare future" +
                                "[added=" + added + ", entry=" + entry + ']';

                            if (added != null && added.ownerVersion() != null)
                                req.owned(entry.key(), added.ownerVersion());

                            break;
                        }
                        catch (GridCacheEntryRemovedException ignore) {
                            assert false : "Got removed exception on entry with dht local candidate: " + entry;
                        }
                    }

                    //noinspection TryWithIdenticalCatches
                    try {
                        cctx.io().send(nearMapping.node(), req);
                    }
                    catch (GridTopologyException e) {
                        fut.onResult(e);
                    }
                    catch (GridException e) {
                        fut.onResult(e);
                    }
                }
            }
        }
        finally {
            markInitialized();
        }
    }

    /**
     * @param entry Transaction entry.
     * @param futDhtMap DHT mapping.
     * @param futNearMap Near mapping.
     * @return {@code True} if mapped.
     */
    private boolean map(
        GridCacheTxEntry<K, V> entry,
        Map<UUID, GridDistributedTxMapping<K, V>> futDhtMap,
        Map<UUID, GridDistributedTxMapping<K, V>> futNearMap) {
        GridDhtCacheEntry<K, V> cached = (GridDhtCacheEntry<K, V>)entry.cached();

        boolean ret;

        GridCacheContext<K, V> cacheCtx = entry.context();

        while (true) {
            try {
                Collection<GridNode> dhtNodes = cacheCtx.dht().topology().nodes(cached.partition(), tx.topologyVersion());

                if (log.isDebugEnabled())
                    log.debug("Mapping entry to DHT nodes [nodes=" + U.toShortString(dhtNodes) +
                        ", entry=" + entry + ']');

                Collection<UUID> readers = cached.readers();

                Collection<GridNode> nearNodes = null;

                if (!F.isEmpty(readers)) {
                    nearNodes = cctx.discovery().nodes(readers, F0.not(F.idForNodeId(tx.nearNodeId())));

                    if (log.isDebugEnabled())
                        log.debug("Mapping entry to near nodes [nodes=" + U.toShortString(nearNodes) +
                            ", entry=" + entry + ']');
                }
                else if (log.isDebugEnabled())
                    log.debug("Entry has no near readers: " + entry);

                // Exclude local node.
                ret = map(entry, F.view(dhtNodes, F.remoteNodes(cctx.localNodeId())), dhtMap, futDhtMap);

                // Exclude DHT nodes.
                ret |= map(entry, F.view(nearNodes, F0.notIn(dhtNodes)), nearMap, futNearMap);

                break;
            }
            catch (GridCacheEntryRemovedException ignore) {
                cached = cacheCtx.dht().entryExx(entry.key().key());

                entry.cached(cached, cached.keyBytes());
            }
        }

        return ret;
    }

    /**
     * @param entry Entry.
     * @param nodes Nodes.
     * @param globalMap Map.
     * @param locMap Exclude map.
     * @return {@code True} if mapped.
     */
    private boolean map(GridCacheTxEntry<K, V> entry, Iterable<GridNode> nodes,
        Map<UUID, GridDistributedTxMapping<K, V>> globalMap, Map<UUID, GridDistributedTxMapping<K, V>> locMap) {
        boolean ret = false;

        if (nodes != null) {
            for (GridNode n : nodes) {
                GridDistributedTxMapping<K, V> global = globalMap.get(n.id());

                if (global == null)
                    globalMap.put(n.id(), global = new GridDistributedTxMapping<>(n));

                global.add(entry);

                GridDistributedTxMapping<K, V> loc = locMap.get(n.id());

                if (loc == null)
                    locMap.put(n.id(), loc = new GridDistributedTxMapping<>(n));

                loc.add(entry);

                ret = true;
            }
        }

        return ret;
    }

    /**
     * Collects versions of pending candidates versions less than base.
     *
     * @param entries Tx entries to process.
     * @param baseVer Base version.
     * @return Collection of pending candidates versions.
     */
    private Collection<GridCacheVersion> localDhtPendingVersions(Iterable<GridCacheTxEntry<K, V>> entries,
        GridCacheVersion baseVer) {
        Collection<GridCacheVersion> lessPending = new GridLeanSet<>(5);

        for (GridCacheTxEntry<K, V> entry : entries) {
            try {
                for (GridCacheMvccCandidate cand : entry.cached().localCandidates()) {
                    if (cand.version().isLess(baseVer))
                        lessPending.add(cand.version());
                }
            }
            catch (GridCacheEntryRemovedException ignored) {
                // No-op, no candidates.
            }
        }

        return lessPending;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDhtTxPrepareFuture.class, this, "super", super.toString());
    }

    /**
     * Mini-future for get operations. Mini-futures are only waiting on a single
     * node as opposed to multiple nodes.
     */
    private class MiniFuture extends GridFutureAdapter<GridCacheTxEx<K, V>> {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private final GridUuid futId = GridUuid.randomUuid();

        /** Node ID. */
        private UUID nodeId;

        /** DHT mapping. */
        @GridToStringInclude
        private GridDistributedTxMapping<K, V> dhtMapping;

        /** Near mapping. */
        @GridToStringInclude
        private GridDistributedTxMapping<K, V> nearMapping;

        /**
         * Empty constructor required for {@link Externalizable}.
         */
        public MiniFuture() {
            super(cctx.kernalContext());
        }

        /**
         * @param nodeId Node ID.
         * @param dhtMapping Mapping.
         * @param nearMapping nearMapping.
         */
        MiniFuture(UUID nodeId, GridDistributedTxMapping<K, V> dhtMapping, GridDistributedTxMapping<K, V> nearMapping) {
            super(cctx.kernalContext());

            assert dhtMapping == null || nearMapping == null || dhtMapping.node() == nearMapping.node();

            this.nodeId = nodeId;
            this.dhtMapping = dhtMapping;
            this.nearMapping = nearMapping;
        }

        /**
         * @return Future ID.
         */
        GridUuid futureId() {
            return futId;
        }

        /**
         * @return Node ID.
         */
        public GridNode node() {
            return dhtMapping != null ? dhtMapping.node() : nearMapping.node();
        }

        /**
         * @param e Error.
         */
        void onResult(Throwable e) {
            if (log.isDebugEnabled())
                log.debug("Failed to get future result [fut=" + this + ", err=" + e + ']');

            // Fail.
            onDone(e);
        }

        /**
         * @param e Node failure.
         */
        void onResult(GridTopologyException e) {
            if (log.isDebugEnabled())
                log.debug("Remote node left grid while sending or waiting for reply (will ignore): " + this);

            if (tx != null)
                tx.removeMapping(nodeId);

            onDone(tx);
        }

        /**
         * @param res Result callback.
         */
        void onResult(GridDhtTxPrepareResponse<K, V> res) {
            if (res.error() != null)
                // Fail the whole compound future.
                onError(res.error());
            else {
                // Process evicted readers (no need to remap).
                if (nearMapping != null && !F.isEmpty(res.nearEvicted())) {
                    nearMapping.evictReaders(res.nearEvicted());

                    for (GridCacheTxEntry<K, V> entry : nearMapping.entries()) {
                        if (res.nearEvicted().contains(entry.key())) {
                            while (true) {
                                try {
                                    GridDhtCacheEntry<K, V> cached = (GridDhtCacheEntry<K, V>)entry.cached();

                                    cached.removeReader(nearMapping.node().id(), res.messageId());

                                    break;
                                }
                                catch (GridCacheEntryRemovedException ignore) {
                                    GridCacheEntryEx<K, V> e = entry.context().cache().peekEx(entry.key().key());

                                    if (e == null)
                                        break;

                                    entry.cached(e, entry.keyBytes());
                                }
                            }
                        }
                    }
                }

                // Process invalid partitions (no need to remap).
                if (!F.isEmpty(res.invalidPartitions())) {
                    for (Iterator<GridCacheTxEntry<K, V>> it = dhtMapping.entries().iterator(); it.hasNext();) {
                        GridCacheTxEntry<K, V> entry  = it.next();

                        if (res.invalidPartitions().contains(entry.cached().partition())) {
                            it.remove();

                            if (log.isDebugEnabled())
                                log.debug("Removed mapping for entry from dht mapping [key=" + entry.key() +
                                    ", tx=" + tx + ", dhtMapping=" + dhtMapping + ']');
                        }
                    }

                    if (dhtMapping.empty()) {
                        dhtMap.remove(nodeId);

                        if (log.isDebugEnabled())
                            log.debug("Removed mapping for node entirely because all partitions are invalid [nodeId=" +
                                nodeId + ", tx=" + tx + ']');
                    }
                }

                long topVer = tx.topologyVersion();

                boolean rec = cctx.events().isRecordable(EVT_CACHE_PRELOAD_OBJECT_LOADED);

                for (GridCacheEntryInfo<K, V> info : res.preloadEntries()) {
                    GridCacheContext<K, V> cacheCtx = cctx.cacheContext(info.cacheId());

                    while (true) {
                        GridCacheEntryEx<K, V> entry = cacheCtx.cache().entryEx(info.key());

                        GridDrType drType = cacheCtx.isDrEnabled() ? GridDrType.DR_PRELOAD : GridDrType.DR_NONE;

                        try {
                            if (entry.initialValue(info.value(), info.valueBytes(), info.version(),
                                info.ttl(), info.expireTime(), true, topVer, drType)) {
                                if (rec && !entry.isInternal())
                                    cctx.events().addEvent(entry.partition(), entry.key(), cctx.localNodeId(),
                                        (GridUuid)null, null, EVT_CACHE_PRELOAD_OBJECT_LOADED, info.value(), true, null,
                                        false, null, null, null);
                            }

                            break;
                        }
                        catch (GridException e) {
                            // Fail the whole thing.
                            onDone(e);

                            return;
                        }
                        catch (GridCacheEntryRemovedException ignore) {
                            if (log.isDebugEnabled())
                                log.debug("Failed to set entry initial value (entry is obsolete, " +
                                    "will retry): " + entry);
                        }
                    }
                }

                // Finish mini future.
                onDone(tx);
            }
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(MiniFuture.class, this, "done", isDone(), "cancelled", isCancelled(), "err", error());
        }
    }
}
