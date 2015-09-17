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

package org.apache.ignite.internal.processors.cache.distributed.dht.colocated;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.cluster.ClusterTopologyServerNotFoundException;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheEntryPredicate;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheEntryRemovedException;
import org.apache.ignite.internal.processors.cache.GridCacheFuture;
import org.apache.ignite.internal.processors.cache.GridCacheLockTimeoutException;
import org.apache.ignite.internal.processors.cache.GridCacheMvccCandidate;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.distributed.GridDistributedCacheEntry;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtCacheEntry;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTopologyFuture;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearLockMapping;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearLockRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearLockResponse;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxLocal;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxEntry;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxKey;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.timeout.GridTimeoutObjectAdapter;
import org.apache.ignite.internal.util.future.GridCompoundIdentityFuture;
import org.apache.ignite.internal.util.future.GridEmbeddedFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.C2;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.transactions.TransactionIsolation;
import org.jetbrains.annotations.Nullable;
import org.jsr166.ConcurrentHashMap8;
import org.jsr166.ConcurrentLinkedDeque8;

import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_READ;

/**
 * Colocated cache lock future.
 */
public final class GridDhtColocatedLockFuture extends GridCompoundIdentityFuture<Boolean>
    implements GridCacheFuture<Boolean> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Logger reference. */
    private static final AtomicReference<IgniteLogger> logRef = new AtomicReference<>();

    /** Logger. */
    private static IgniteLogger log;

    /** Cache registry. */
    @GridToStringExclude
    private GridCacheContext<?, ?> cctx;

    /** Lock owner thread. */
    @GridToStringInclude
    private long threadId;

    /** Keys to lock. */
    private Collection<KeyCacheObject> keys;

    /** Future ID. */
    private IgniteUuid futId;

    /** Lock version. */
    private GridCacheVersion lockVer;

    /** Read flag. */
    private boolean read;

    /** Flag to return value. */
    private boolean retval;

    /** Error. */
    private AtomicReference<Throwable> err = new AtomicReference<>(null);

    /** Timeout object. */
    @GridToStringExclude
    private LockTimeoutObject timeoutObj;

    /** Lock timeout. */
    private long timeout;

    /** Filter. */
    private CacheEntryPredicate[] filter;

    /** Transaction. */
    @GridToStringExclude
    private GridNearTxLocal tx;

    /** Topology snapshot to operate on. */
    private AtomicReference<AffinityTopologyVersion> topVer = new AtomicReference<>();

    /** Map of current values. */
    private Map<KeyCacheObject, IgniteBiTuple<GridCacheVersion, CacheObject>> valMap;

    /** Trackable flag (here may be non-volatile). */
    private boolean trackable;

    /** TTL for read operation. */
    private long accessTtl;

    /** Skip store flag. */
    private final boolean skipStore;

    /**
     * @param cctx Registry.
     * @param keys Keys to lock.
     * @param tx Transaction.
     * @param read Read flag.
     * @param retval Flag to return value or not.
     * @param timeout Lock acquisition timeout.
     * @param accessTtl TTL for read operation.
     * @param filter Filter.
     * @param skipStore Skip store flag.
     */
    public GridDhtColocatedLockFuture(
        GridCacheContext<?, ?> cctx,
        Collection<KeyCacheObject> keys,
        @Nullable GridNearTxLocal tx,
        boolean read,
        boolean retval,
        long timeout,
        long accessTtl,
        CacheEntryPredicate[] filter,
        boolean skipStore) {
        super(cctx.kernalContext(), CU.boolReducer());

        assert keys != null;

        this.cctx = cctx;
        this.keys = keys;
        this.tx = tx;
        this.read = read;
        this.retval = retval;
        this.timeout = timeout;
        this.accessTtl = accessTtl;
        this.filter = filter;
        this.skipStore = skipStore;

        ignoreInterrupts(true);

        threadId = tx == null ? Thread.currentThread().getId() : tx.threadId();

        lockVer = tx != null ? tx.xidVersion() : cctx.versions().next();

        futId = IgniteUuid.randomUuid();

        if (log == null)
            log = U.logger(cctx.kernalContext(), logRef, GridDhtColocatedLockFuture.class);

        if (timeout > 0) {
            timeoutObj = new LockTimeoutObject();

            cctx.time().addTimeoutObject(timeoutObj);
        }

        valMap = new ConcurrentHashMap8<>(keys.size(), 1f);
    }

    /**
     * @return Participating nodes.
     */
    @Override public Collection<? extends ClusterNode> nodes() {
        return F.viewReadOnly(futures(), new IgniteClosure<IgniteInternalFuture<?>, ClusterNode>() {
            @Nullable @Override public ClusterNode apply(IgniteInternalFuture<?> f) {
                if (isMini(f))
                    return ((MiniFuture)f).node();

                return cctx.discovery().localNode();
            }
        });
    }

    /** {@inheritDoc} */
    @Override public GridCacheVersion version() {
        return lockVer;
    }

    /**
     * @return Future ID.
     */
    @Override public IgniteUuid futureId() {
        return futId;
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
     * @return {@code True} if transaction is not {@code null}.
     */
    private boolean inTx() {
        return tx != null;
    }

    /**
     * @return {@code True} if implicit-single-tx flag is set.
     */
    private boolean implicitSingleTx() {
        return tx != null && tx.implicitSingle();
    }

    /**
     * @return {@code True} if transaction is not {@code null} and has invalidate flag set.
     */
    private boolean isInvalidate() {
        return tx != null && tx.isInvalidate();
    }

    /**
     * @return {@code True} if commit is synchronous.
     */
    private boolean syncCommit() {
        return tx != null && tx.syncCommit();
    }

    /**
     * @return {@code True} if rollback is synchronous.
     */
    private boolean syncRollback() {
        return tx != null && tx.syncRollback();
    }

    /**
     * @return Transaction isolation or {@code null} if no transaction.
     */
    @Nullable private TransactionIsolation isolation() {
        return tx == null ? null : tx.isolation();
    }

    /**
     * @return {@code true} if related transaction is implicit.
     */
    private boolean implicitTx() {
        return tx != null && tx.implicit();
    }

    /**
     * Adds entry to future.
     *
     * @param entry Entry to add.
     * @return Non-reentry candidate if lock should be acquired on remote node,
     *      reentry candidate if locks has been already acquired and {@code null} if explicit locks is held and
     *      implicit transaction accesses locked entry.
     * @throws IgniteCheckedException If failed to add entry due to external locking.
     */
    @Nullable private GridCacheMvccCandidate addEntry(GridDistributedCacheEntry entry) throws IgniteCheckedException {
        GridCacheMvccCandidate cand = cctx.mvcc().explicitLock(threadId, entry.key());

        if (inTx()) {
            IgniteTxEntry txEntry = tx.entry(entry.txKey());

            txEntry.cached(entry);

            if (cand != null) {
                if (!tx.implicit())
                    throw new IgniteCheckedException("Cannot access key within transaction if lock is " +
                        "externally held [key=" + entry.key() + ", entry=" + entry + ']');
                else
                    return null;
            }
            else {
                // Check transaction entries (corresponding tx entries must be enlisted in transaction).
                cand = new GridCacheMvccCandidate(entry,
                    cctx.localNodeId(),
                    null,
                    null,
                    threadId,
                    lockVer,
                    timeout,
                    true,
                    tx.entry(entry.txKey()).locked(),
                    inTx(),
                    inTx() && tx.implicitSingle(),
                    false,
                    false);

                cand.topologyVersion(topVer.get());
            }
        }
        else {
            if (cand == null) {
                cand = new GridCacheMvccCandidate(entry,
                    cctx.localNodeId(),
                    null,
                    null,
                    threadId,
                    lockVer,
                    timeout,
                    true,
                    false,
                    inTx(),
                    inTx() && tx.implicitSingle(),
                    false,
                    false);

                cand.topologyVersion(topVer.get());
            }
            else
                cand = cand.reenter();

            cctx.mvcc().addExplicitLock(threadId, cand, topVer.get());
        }

        return cand;
    }

    /**
     * Undoes all locks.
     *
     * @param dist If {@code true}, then remove locks from remote nodes as well.
     * @param rollback {@code True} if should rollback tx.
     */
    private void undoLocks(boolean dist, boolean rollback) {
        // Transactions will undo during rollback.
        if (dist && tx == null)
            cctx.colocated().removeLocks(threadId, lockVer, keys);
        else {
            if (rollback && tx != null) {
                if (tx.setRollbackOnly()) {
                    if (log.isDebugEnabled())
                        log.debug("Marked transaction as rollback only because locks could not be acquired: " + tx);
                }
                else if (log.isDebugEnabled())
                    log.debug("Transaction was not marked rollback-only while locks were not acquired: " + tx);
            }
        }

        cctx.mvcc().recheckPendingLocks();
    }

    /**
     * @param success Success flag.
     */
    public void complete(boolean success) {
        onComplete(success, true);
    }

    /**
     * @param nodeId Left node ID
     * @return {@code True} if node was in the list.
     */
    @Override public boolean onNodeLeft(UUID nodeId) {
        boolean found = false;

        for (IgniteInternalFuture<?> fut : futures()) {
            if (isMini(fut)) {
                MiniFuture f = (MiniFuture)fut;

                if (f.node().id().equals(nodeId)) {
                    if (log.isDebugEnabled())
                        log.debug("Found mini-future for left node [nodeId=" + nodeId + ", mini=" + f + ", fut=" +
                            this + ']');

                    f.onResult(newTopologyException(null, nodeId));

                    found = true;
                }
            }
        }

        if (log.isDebugEnabled())
            log.debug("Near lock future does not have mapping for left node (ignoring) [nodeId=" + nodeId + ", fut=" +
                this + ']');

        return found;
    }

    /**
     * @param nodeId Sender.
     * @param res Result.
     */
    void onResult(UUID nodeId, GridNearLockResponse res) {
        if (!isDone()) {
            if (log.isDebugEnabled())
                log.debug("Received lock response from node [nodeId=" + nodeId + ", res=" + res + ", fut=" +
                    this + ']');

            for (IgniteInternalFuture<Boolean> fut : pending()) {
                if (isMini(fut)) {
                    MiniFuture mini = (MiniFuture)fut;

                    if (mini.futureId().equals(res.miniId())) {
                        assert mini.node().id().equals(nodeId);

                        if (log.isDebugEnabled())
                            log.debug("Found mini future for response [mini=" + mini + ", res=" + res + ']');

                        mini.onResult(res);

                        if (log.isDebugEnabled())
                            log.debug("Future after processed lock response [fut=" + this + ", mini=" + mini +
                                ", res=" + res + ']');

                        return;
                    }
                }
            }

            U.warn(log, "Failed to find mini future for response (perhaps due to stale message) [res=" + res +
                ", fut=" + this + ']');
        }
        else if (log.isDebugEnabled())
            log.debug("Ignoring lock response from node (future is done) [nodeId=" + nodeId + ", res=" + res +
                ", fut=" + this + ']');
    }

    /**
     * @param t Error.
     */
    private void onError(Throwable t) {
        err.compareAndSet(null, t instanceof GridCacheLockTimeoutException ? null : t);
    }

    /** {@inheritDoc} */
    @Override public boolean cancel() {
        if (onCancelled())
            onComplete(false, true);

        return isCancelled();
    }

    /** {@inheritDoc} */
    @Override public boolean onDone(Boolean success, Throwable err) {
        if (log.isDebugEnabled())
            log.debug("Received onDone(..) callback [success=" + success + ", err=" + err + ", fut=" + this + ']');

        if (isDone())
            return false;

        this.err.compareAndSet(null, err instanceof GridCacheLockTimeoutException ? null : err);

        if (err != null)
            success = false;

        return onComplete(success, true);
    }

    /**
     * Completeness callback.
     *
     * @param success {@code True} if lock was acquired.
     * @param distribute {@code True} if need to distribute lock removal in case of failure.
     * @return {@code True} if complete by this operation.
     */
    private boolean onComplete(boolean success, boolean distribute) {
        if (log.isDebugEnabled())
            log.debug("Received onComplete(..) callback [success=" + success + ", distribute=" + distribute +
                ", fut=" + this + ']');

        if (!success)
            undoLocks(distribute, true);

        if (tx != null)
            cctx.tm().txContext(tx);

        if (super.onDone(success, err.get())) {
            if (log.isDebugEnabled())
                log.debug("Completing future: " + this);

            // Clean up.
            cctx.mvcc().removeFuture(this);

            if (timeoutObj != null)
                cctx.time().removeTimeoutObject(timeoutObj);

            return true;
        }

        return false;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return futId.hashCode();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDhtColocatedLockFuture.class, this, "inTx", inTx(), "super", super.toString());
    }

    /**
     * @param f Future.
     * @return {@code True} if mini-future.
     */
    private boolean isMini(IgniteInternalFuture<?> f) {
        return f.getClass().equals(MiniFuture.class);
    }

    /**
     * Basically, future mapping consists from two parts. First, we must determine the topology version this future
     * will map on. Locking is performed within a user transaction, we must continue to map keys on the same
     * topology version as it started. If topology version is undefined, we get current topology future and wait
     * until it completes so the topology is ready to use.
     * <p/>
     * During the second part we map keys to primary nodes using topology snapshot we obtained during the first
     * part. Note that if primary node leaves grid, the future will fail and transaction will be rolled back.
     */
    void map() {
        // Obtain the topology version to use.
        AffinityTopologyVersion topVer = cctx.mvcc().lastExplicitLockTopologyVersion(threadId);

        // If there is another system transaction in progress, use it's topology version to prevent deadlock.
        if (topVer == null && tx != null && tx.system()) {
            IgniteInternalTx tx0 = cctx.tm().anyActiveThreadTx(tx);

            if (tx0 != null)
                topVer = tx0.topologyVersionSnapshot();
        }

        if (topVer != null && tx != null)
            tx.topologyVersion(topVer);

        if (topVer == null && tx != null)
            topVer = tx.topologyVersionSnapshot();

        if (topVer != null) {
            for (GridDhtTopologyFuture fut : cctx.shared().exchange().exchangeFutures()){
                if (fut.topologyVersion().equals(topVer)){
                    if (!fut.isCacheTopologyValid(cctx)) {
                        onDone(new IgniteCheckedException("Failed to perform cache operation (cache topology is not valid): " +
                            cctx.name()));

                        return;
                    }

                    break;
                }
            }

            // Continue mapping on the same topology version as it was before.
            this.topVer.compareAndSet(null, topVer);

            map(keys, false);

            markInitialized();

            return;
        }

        // Must get topology snapshot and map on that version.
        mapOnTopology(false, null);
    }

    /**
     * Acquires topology future and checks it completeness under the read lock. If it is not complete,
     * will asynchronously wait for it's completeness and then try again.
     *
     * @param remap Remap flag.
     * @param c Optional closure to run after map.
     */
    private void mapOnTopology(final boolean remap, @Nullable final Runnable c) {
        // We must acquire topology snapshot from the topology version future.
        cctx.topology().readLock();

        try {
            if (cctx.topology().stopping()) {
                onDone(new IgniteCheckedException("Failed to perform cache operation (cache is stopped): " +
                    cctx.name()));

                return;
            }

            GridDhtTopologyFuture fut = cctx.topologyVersionFuture();

            if (fut.isDone()) {
                if (!fut.isCacheTopologyValid(cctx)) {
                    onDone(new IgniteCheckedException("Failed to perform cache operation (cache topology is not valid): " +
                        cctx.name()));

                    return;
                }

                AffinityTopologyVersion topVer = fut.topologyVersion();

                if (remap) {
                    if (tx != null)
                        tx.onRemap(topVer);

                    this.topVer.set(topVer);
                }
                else {
                    if (tx != null)
                        tx.topologyVersion(topVer);

                    this.topVer.compareAndSet(null, topVer);
                }

                map(keys, remap);

                if (c != null)
                    c.run();

                markInitialized();
            }
            else {
                fut.listen(new CI1<IgniteInternalFuture<AffinityTopologyVersion>>() {
                    @Override public void apply(IgniteInternalFuture<AffinityTopologyVersion> t) {
                        try {
                            mapOnTopology(remap, c);
                        }
                        finally {
                            cctx.shared().txContextReset();
                        }
                    }
                });
            }
        }
        finally {
            cctx.topology().readUnlock();
        }
    }

    /**
     * Maps keys to nodes. Note that we can not simply group keys by nodes and send lock request as
     * such approach does not preserve order of lock acquisition. Instead, keys are split in continuous
     * groups belonging to one primary node and locks for these groups are acquired sequentially.
     *
     * @param keys Keys.
     * @param remap Remap flag.
     */
    private void map(Collection<KeyCacheObject> keys, boolean remap) {
        try {
            AffinityTopologyVersion topVer = this.topVer.get();

            assert topVer != null;

            assert topVer.topologyVersion() > 0;

            if (CU.affinityNodes(cctx, topVer).isEmpty()) {
                onDone(new ClusterTopologyServerNotFoundException("Failed to map keys for cache " +
                    "(all partition nodes left the grid): " + cctx.name()));

                return;
            }

            boolean clientNode = cctx.kernalContext().clientNode();

            assert !remap || (clientNode && (tx == null || !tx.hasRemoteLocks()));

            // First assume this node is primary for all keys passed in.
            if (!clientNode && mapAsPrimary(keys, topVer))
                return;

            Deque<GridNearLockMapping> mappings = new ConcurrentLinkedDeque8<>();

            // Assign keys to primary nodes.
            GridNearLockMapping map = null;

            for (KeyCacheObject key : keys) {
                GridNearLockMapping updated = map(key, map, topVer);

                // If new mapping was created, add to collection.
                if (updated != map) {
                    mappings.add(updated);

                    if (tx != null && updated.node().isLocal())
                        tx.colocatedLocallyMapped(true);
                }

                map = updated;
            }

            if (isDone()) {
                if (log.isDebugEnabled())
                    log.debug("Abandoning (re)map because future is done: " + this);

                return;
            }

            if (log.isDebugEnabled())
                log.debug("Starting (re)map for mappings [mappings=" + mappings + ", fut=" + this + ']');

            boolean hasRmtNodes = false;

            boolean first = true;

            // Create mini futures.
            for (Iterator<GridNearLockMapping> iter = mappings.iterator(); iter.hasNext(); ) {
                GridNearLockMapping mapping = iter.next();

                ClusterNode node = mapping.node();
                Collection<KeyCacheObject> mappedKeys = mapping.mappedKeys();

                boolean loc = node.equals(cctx.localNode());

                assert !mappedKeys.isEmpty();

                GridNearLockRequest req = null;

                Collection<KeyCacheObject> distributedKeys = new ArrayList<>(mappedKeys.size());

                for (KeyCacheObject key : mappedKeys) {
                    IgniteTxKey txKey = cctx.txKey(key);

                    GridDistributedCacheEntry entry = null;

                    if (tx != null) {
                        IgniteTxEntry txEntry = tx.entry(txKey);

                        if (txEntry != null) {
                            entry = (GridDistributedCacheEntry)txEntry.cached();

                            if (entry != null && !(loc ^ entry.detached())) {
                                entry = cctx.colocated().entryExx(key, topVer, true);

                                txEntry.cached(entry);
                            }
                        }
                    }

                    boolean explicit;

                    while (true) {
                        try {
                            if (entry == null)
                                entry = cctx.colocated().entryExx(key, topVer, true);

                            if (!cctx.isAll(entry, filter)) {
                                if (log.isDebugEnabled())
                                    log.debug("Entry being locked did not pass filter (will not lock): " + entry);

                                onComplete(false, false);

                                return;
                            }

                            assert loc ^ entry.detached() : "Invalid entry [loc=" + loc + ", entry=" + entry + ']';

                            GridCacheMvccCandidate cand = addEntry(entry);

                            // Will either return value from dht cache or null if this is a miss.
                            IgniteBiTuple<GridCacheVersion, CacheObject> val = entry.detached() ? null :
                                ((GridDhtCacheEntry)entry).versionedValue(topVer);

                            GridCacheVersion dhtVer = null;

                            if (val != null) {
                                dhtVer = val.get1();

                                valMap.put(key, val);
                            }

                            if (cand != null && !cand.reentry()) {
                                if (req == null) {
                                    boolean clientFirst = false;

                                    if (first) {
                                        clientFirst = clientNode && (tx == null || !tx.hasRemoteLocks());

                                        first = false;
                                    }

                                    req = new GridNearLockRequest(
                                        cctx.cacheId(),
                                        topVer,
                                        cctx.nodeId(),
                                        threadId,
                                        futId,
                                        lockVer,
                                        inTx(),
                                        implicitTx(),
                                        implicitSingleTx(),
                                        read,
                                        retval,
                                        isolation(),
                                        isInvalidate(),
                                        timeout,
                                        mappedKeys.size(),
                                        inTx() ? tx.size() : mappedKeys.size(),
                                        inTx() && tx.syncCommit(),
                                        inTx() ? tx.subjectId() : null,
                                        inTx() ? tx.taskNameHash() : 0,
                                        read ? accessTtl : -1L,
                                        skipStore,
                                        clientFirst);

                                    mapping.request(req);
                                }

                                distributedKeys.add(key);

                                if (tx != null)
                                    tx.addKeyMapping(txKey, mapping.node());

                                req.addKeyBytes(
                                    key,
                                    retval,
                                    dhtVer, // Include DHT version to match remote DHT entry.
                                    cctx);
                            }

                            explicit = inTx() && cand == null;

                            if (explicit)
                                tx.addKeyMapping(txKey, mapping.node());

                            break;
                        }
                        catch (GridCacheEntryRemovedException ignored) {
                            if (log.isDebugEnabled())
                                log.debug("Got removed entry in lockAsync(..) method (will retry): " + entry);

                            entry = null;
                        }
                    }

                    // Mark mapping explicit lock flag.
                    if (explicit) {
                        boolean marked = tx != null && tx.markExplicit(node.id());

                        assert tx == null || marked;
                    }
                }

                if (inTx() && req != null)
                    req.hasTransforms(tx.hasTransforms());

                if (!distributedKeys.isEmpty()) {
                    mapping.distributedKeys(distributedKeys);

                    hasRmtNodes |= !mapping.node().isLocal();
                }
                else {
                    assert mapping.request() == null;

                    iter.remove();
                }
            }

            if (hasRmtNodes) {
                trackable = true;

                if (!remap && !cctx.mvcc().addFuture(this))
                    throw new IllegalStateException("Duplicate future ID: " + this);
            }
            else
                trackable = false;

            proceedMapping(mappings);
        }
        catch (IgniteCheckedException ex) {
            onDone(false, ex);
        }
    }

    /**
     * Gets next near lock mapping and either acquires dht locks locally or sends near lock request to
     * remote primary node.
     *
     * @param mappings Queue of mappings.
     * @throws IgniteCheckedException If mapping can not be completed.
     */
    private void proceedMapping(final Deque<GridNearLockMapping> mappings)
        throws IgniteCheckedException {
        GridNearLockMapping map = mappings.poll();

        // If there are no more mappings to process, complete the future.
        if (map == null)
            return;

        final GridNearLockRequest req = map.request();
        final Collection<KeyCacheObject> mappedKeys = map.distributedKeys();
        final ClusterNode node = map.node();

        if (filter != null && filter.length != 0)
            req.filter(filter, cctx);

        if (node.isLocal())
            lockLocally(mappedKeys, req.topologyVersion(), mappings);
        else {
            final MiniFuture fut = new MiniFuture(node, mappedKeys, mappings);

            req.miniId(fut.futureId());

            add(fut); // Append new future.

            IgniteInternalFuture<?> txSync = null;

            if (inTx())
                txSync = cctx.tm().awaitFinishAckAsync(node.id(), tx.threadId());

            if (txSync == null || txSync.isDone()) {
                try {
                    if (log.isDebugEnabled())
                        log.debug("Sending near lock request [node=" + node.id() + ", req=" + req + ']');

                    cctx.io().send(node, req, cctx.ioPolicy());
                }
                catch (ClusterTopologyCheckedException ex) {
                    assert fut != null;

                    fut.onResult(ex);
                }
            }
            else {
                txSync.listen(new CI1<IgniteInternalFuture<?>>() {
                    @Override public void apply(IgniteInternalFuture<?> t) {
                        try {
                            if (log.isDebugEnabled())
                                log.debug("Sending near lock request [node=" + node.id() + ", req=" + req + ']');

                            cctx.io().send(node, req, cctx.ioPolicy());
                        }
                        catch (ClusterTopologyCheckedException ex) {
                            assert fut != null;

                            fut.onResult(ex);
                        }
                        catch (IgniteCheckedException e) {
                            onError(e);
                        }
                    }
                });
            }
        }
    }

    /**
     * Locks given keys directly through dht cache.
     *
     * @param keys Collection of keys.
     * @param topVer Topology version to lock on.
     * @param mappings Optional collection of mappings to proceed locking.
     */
    private void lockLocally(
        final Collection<KeyCacheObject> keys,
        AffinityTopologyVersion topVer,
        @Nullable final Deque<GridNearLockMapping> mappings
    ) {
        if (log.isDebugEnabled())
            log.debug("Before locally locking keys : " + keys);

        IgniteInternalFuture<Exception> fut = cctx.colocated().lockAllAsync(cctx,
            tx,
            threadId,
            lockVer,
            topVer,
            keys,
            read,
            retval,
            timeout,
            accessTtl,
            filter,
            skipStore);

        // Add new future.
        add(new GridEmbeddedFuture<>(
            new C2<Exception, Exception, Boolean>() {
                @Override public Boolean apply(Exception resEx, Exception e) {
                    if (CU.isLockTimeoutOrCancelled(e) ||
                        (resEx != null && CU.isLockTimeoutOrCancelled(resEx)))
                        return false;

                    if (e != null) {
                        onError(e);

                        return false;
                    }

                    if (resEx != null) {
                        onError(resEx);

                        return false;
                    }

                    if (log.isDebugEnabled())
                        log.debug("Acquired lock for local DHT mapping [locId=" + cctx.nodeId() +
                            ", mappedKeys=" + keys + ", fut=" + GridDhtColocatedLockFuture.this + ']');

                    if (inTx()) {
                        for (KeyCacheObject key : keys)
                            tx.entry(cctx.txKey(key)).markLocked();
                    }
                    else {
                        for (KeyCacheObject key : keys)
                            cctx.mvcc().markExplicitOwner(key, threadId);
                    }

                    try {
                        // Proceed and add new future (if any) before completing embedded future.
                        if (mappings != null)
                            proceedMapping(mappings);
                    }
                    catch (IgniteCheckedException ex) {
                        onError(ex);

                        return false;
                    }

                    return true;
                }
            },
            fut));
    }

    /**
     * Tries to map this future in assumption that local node is primary for all keys passed in.
     * If node is not primary for one of the keys, then mapping is reverted and full remote mapping is performed.
     *
     * @param keys Keys to lock.
     * @param topVer Topology version.
     * @return {@code True} if all keys were mapped locally, {@code false} if full mapping should be performed.
     * @throws IgniteCheckedException If key cannot be added to mapping.
     */
    private boolean mapAsPrimary(Collection<KeyCacheObject> keys, AffinityTopologyVersion topVer) throws IgniteCheckedException {
        // Assign keys to primary nodes.
        Collection<KeyCacheObject> distributedKeys = new ArrayList<>(keys.size());

        boolean explicit = false;

        for (KeyCacheObject key : keys) {
            if (!cctx.affinity().primary(cctx.localNode(), key, topVer)) {
                // Remove explicit locks added so far.
                for (KeyCacheObject k : keys)
                    cctx.mvcc().removeExplicitLock(threadId, k, lockVer);

                return false;
            }

            explicit |= addLocalKey(key, topVer, distributedKeys);

            if (isDone())
                return true;
        }

        trackable = false;

        if (tx != null) {
            if (explicit)
                tx.markExplicit(cctx.localNodeId());

            tx.colocatedLocallyMapped(true);
        }

        if (!distributedKeys.isEmpty()) {
            if (tx != null) {
                for (KeyCacheObject key : distributedKeys)
                    tx.addKeyMapping(cctx.txKey(key), cctx.localNode());
            }

            lockLocally(distributedKeys, topVer, null);
        }

        return true;
    }

    /**
     * Adds local key future.
     *
     * @param key Key to add.
     * @param topVer Topology version.
     * @param distributedKeys Collection of keys needs to be locked.
     * @return {@code True} if transaction accesses key that was explicitly locked before.
     * @throws IgniteCheckedException If lock is externally held and transaction is explicit.
     */
    private boolean addLocalKey(
        KeyCacheObject key,
        AffinityTopologyVersion topVer,
        Collection<KeyCacheObject> distributedKeys
    ) throws IgniteCheckedException {
        GridDistributedCacheEntry entry = cctx.colocated().entryExx(key, topVer, false);

        assert !entry.detached();

        if (!cctx.isAll(entry, filter)) {
            if (log.isDebugEnabled())
                log.debug("Entry being locked did not pass filter (will not lock): " + entry);

            onComplete(false, false);

            return false;
        }

        GridCacheMvccCandidate cand = addEntry(entry);

        if (cand != null && !cand.reentry())
            distributedKeys.add(key);

        return inTx() && cand == null;
    }

    /**
     * @param mapping Mappings.
     * @param key Key to map.
     * @param topVer Topology version.
     * @return Near lock mapping.
     * @throws IgniteCheckedException If mapping failed.
     */
    private GridNearLockMapping map(
        KeyCacheObject key,
        @Nullable GridNearLockMapping mapping,
        AffinityTopologyVersion topVer
    ) throws IgniteCheckedException {
        assert mapping == null || mapping.node() != null;

        ClusterNode primary = cctx.affinity().primary(key, topVer);

        if (primary == null)
            throw new ClusterTopologyServerNotFoundException("Failed to lock keys " +
                "(all partition nodes left the grid).");

        if (cctx.discovery().node(primary.id()) == null)
            // If primary node left the grid before lock acquisition, fail the whole future.
            throw newTopologyException(null, primary.id());

        if (mapping == null || !primary.id().equals(mapping.node().id()))
            mapping = new GridNearLockMapping(primary, key);
        else
            mapping.addKey(key);

        return mapping;
    }

    /**
     * Creates new topology exception for cases when primary node leaves grid during mapping.
     *
     * @param nested Optional nested exception.
     * @param nodeId Node ID.
     * @return Topology exception with user-friendly message.
     */
    private ClusterTopologyCheckedException newTopologyException(@Nullable Throwable nested, UUID nodeId) {
        ClusterTopologyCheckedException topEx = new ClusterTopologyCheckedException("Failed to acquire lock for keys " +
            "(primary node left grid, retry transaction if possible) [keys=" + keys + ", node=" + nodeId + ']', nested);

        topEx.retryReadyFuture(cctx.shared().nextAffinityReadyFuture(topVer.get()));

        return topEx;
    }

    /**
     * Lock request timeout object.
     */
    private class LockTimeoutObject extends GridTimeoutObjectAdapter {
        /**
         * Default constructor.
         */
        LockTimeoutObject() {
            super(timeout);
        }

        /** {@inheritDoc} */
        @Override public void onTimeout() {
            if (log.isDebugEnabled())
                log.debug("Timed out waiting for lock response: " + this);

            onComplete(false, true);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(LockTimeoutObject.class, this);
        }
    }

    /**
     * Mini-future for get operations. Mini-futures are only waiting on a single
     * node as opposed to multiple nodes.
     */
    private class MiniFuture extends GridFutureAdapter<Boolean> {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private final IgniteUuid futId = IgniteUuid.randomUuid();

        /** Node ID. */
        @GridToStringExclude
        private ClusterNode node;

        /** Keys. */
        @GridToStringInclude
        private Collection<KeyCacheObject> keys;

        /** Mappings to proceed. */
        @GridToStringExclude
        private Deque<GridNearLockMapping> mappings;

        /** */
        private AtomicBoolean rcvRes = new AtomicBoolean(false);

        /**
         * @param node Node.
         * @param keys Keys.
         * @param mappings Mappings to proceed.
         */
        MiniFuture(ClusterNode node,
            Collection<KeyCacheObject> keys,
            Deque<GridNearLockMapping> mappings) {
            this.node = node;
            this.keys = keys;
            this.mappings = mappings;
        }

        /**
         * @return Future ID.
         */
        IgniteUuid futureId() {
            return futId;
        }

        /**
         * @return Node ID.
         */
        public ClusterNode node() {
            return node;
        }

        /**
         * @return Keys.
         */
        public Collection<KeyCacheObject> keys() {
            return keys;
        }

        /**
         * @param e Error.
         */
        void onResult(Throwable e) {
            if (rcvRes.compareAndSet(false, true)) {
                if (log.isDebugEnabled())
                    log.debug("Failed to get future result [fut=" + this + ", err=" + e + ']');

                // Fail.
                onDone(e);
            }
            else
                U.warn(log, "Received error after another result has been processed [fut=" +
                    GridDhtColocatedLockFuture.this + ", mini=" + this + ']', e);
        }

        /**
         * @param e Node left exception.
         */
        void onResult(ClusterTopologyCheckedException e) {
            if (isDone())
                return;

            if (rcvRes.compareAndSet(false, true)) {
                if (log.isDebugEnabled())
                    log.debug("Remote node left grid while sending or waiting for reply (will fail): " + this);

                if (tx != null)
                    tx.removeMapping(node.id());

                // Primary node left the grid, so fail the future.
                GridDhtColocatedLockFuture.this.onDone(newTopologyException(e, node.id()));

                onDone(true);
            }
        }

        /**
         * @param res Result callback.
         */
        void onResult(GridNearLockResponse res) {
            if (rcvRes.compareAndSet(false, true)) {
                if (res.error() != null) {
                    if (log.isDebugEnabled())
                        log.debug("Finishing mini future with an error due to error in response [miniFut=" + this +
                            ", res=" + res + ']');

                    // Fail.
                    if (res.error() instanceof GridCacheLockTimeoutException)
                        onDone(false);
                    else
                        onDone(res.error());

                    return;
                }

                if (res.clientRemapVersion() != null) {
                    assert cctx.kernalContext().clientNode();

                    IgniteInternalFuture<?> affFut =
                        cctx.shared().exchange().affinityReadyFuture(res.clientRemapVersion());

                    if (affFut != null && !affFut.isDone()) {
                        affFut.listen(new CI1<IgniteInternalFuture<?>>() {
                            @Override public void apply(IgniteInternalFuture<?> fut) {
                                try {
                                    remap();
                                }
                                finally {
                                    cctx.shared().txContextReset();
                                }
                            }
                        });
                    }
                    else
                        remap();
                }
                else  {
                    int i = 0;

                    for (KeyCacheObject k : keys) {
                        IgniteBiTuple<GridCacheVersion, CacheObject> oldValTup = valMap.get(k);

                        CacheObject newVal = res.value(i);

                        GridCacheVersion dhtVer = res.dhtVersion(i);

                        if (newVal == null) {
                            if (oldValTup != null) {
                                if (oldValTup.get1().equals(dhtVer))
                                    newVal = oldValTup.get2();
                            }
                        }

                        if (inTx()) {
                            IgniteTxEntry txEntry = tx.entry(cctx.txKey(k));

                            // In colocated cache we must receive responses only for detached entries.
                            assert txEntry.cached().detached() : txEntry;

                            txEntry.markLocked();

                            GridDhtDetachedCacheEntry entry = (GridDhtDetachedCacheEntry)txEntry.cached();

                            if (res.dhtVersion(i) == null) {
                                onDone(new IgniteCheckedException("Failed to receive DHT version from remote node " +
                                    "(will fail the lock): " + res));

                                return;
                            }

                            // Set value to detached entry.
                            entry.resetFromPrimary(newVal, dhtVer);

                            tx.hasRemoteLocks(true);

                            if (log.isDebugEnabled())
                                log.debug("Processed response for entry [res=" + res + ", entry=" + entry + ']');
                        }
                        else
                            cctx.mvcc().markExplicitOwner(k, threadId);

                        if (retval && cctx.events().isRecordable(EVT_CACHE_OBJECT_READ)) {
                            cctx.events().addEvent(cctx.affinity().partition(k),
                                k,
                                tx,
                                null,
                                EVT_CACHE_OBJECT_READ,
                                newVal,
                                newVal != null,
                                null,
                                false,
                                CU.subjectId(tx, cctx.shared()),
                                null,
                                tx == null ? null : tx.resolveTaskName());
                        }

                        i++;
                    }

                    try {
                        proceedMapping(mappings);
                    }
                    catch (IgniteCheckedException e) {
                        onDone(e);
                    }

                    onDone(true);
                }
            }
        }

        /**
         *
         */
        private void remap() {
            undoLocks(false, false);

            for (KeyCacheObject key : GridDhtColocatedLockFuture.this.keys)
                cctx.mvcc().removeExplicitLock(threadId, key, lockVer);

            mapOnTopology(true, new Runnable() {
                @Override public void run() {
                    onDone(true);
                }
            });
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(MiniFuture.class, this, "node", node.id(), "super", super.toString());
        }
    }
}
