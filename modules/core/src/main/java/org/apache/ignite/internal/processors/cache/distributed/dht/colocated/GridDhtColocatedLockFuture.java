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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
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
import org.apache.ignite.internal.processors.cache.GridCacheEntryEx;
import org.apache.ignite.internal.processors.cache.GridCacheEntryRemovedException;
import org.apache.ignite.internal.processors.cache.GridCacheLockTimeoutException;
import org.apache.ignite.internal.processors.cache.GridCacheMvccCandidate;
import org.apache.ignite.internal.processors.cache.GridCacheMvccFuture;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.distributed.GridDistributedCacheEntry;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtCacheEntry;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTopologyFuture;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearLockMapping;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearLockRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearLockResponse;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxLocal;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxEntry;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxKey;
import org.apache.ignite.internal.processors.cache.transactions.TxDeadlock;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.timeout.GridTimeoutObjectAdapter;
import org.apache.ignite.internal.transactions.IgniteTxTimeoutCheckedException;
import org.apache.ignite.internal.util.future.GridCompoundIdentityFuture;
import org.apache.ignite.internal.util.future.GridEmbeddedFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.C1;
import org.apache.ignite.internal.util.typedef.C2;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.transactions.TransactionDeadlockException;
import org.apache.ignite.transactions.TransactionIsolation;
import org.jetbrains.annotations.Nullable;
import org.jsr166.ConcurrentHashMap8;

import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_READ;

/**
 * Colocated cache lock future.
 */
public final class GridDhtColocatedLockFuture extends GridCompoundIdentityFuture<Boolean>
    implements GridCacheMvccFuture<Boolean> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Logger reference. */
    private static final AtomicReference<IgniteLogger> logRef = new AtomicReference<>();

    /** Logger. */
    private static IgniteLogger log;

    /** Logger. */
    private static IgniteLogger msgLog;

    /** Cache registry. */
    @GridToStringExclude
    private final GridCacheContext<?, ?> cctx;

    /** Lock owner thread. */
    @GridToStringInclude
    private final long threadId;

    /** Keys to lock. */
    private Collection<KeyCacheObject> keys;

    /** Future ID. */
    private final IgniteUuid futId;

    /** Lock version. */
    private final GridCacheVersion lockVer;

    /** Read flag. */
    private final boolean read;

    /** Flag to return value. */
    private final boolean retval;

    /** Error. */
    private volatile Throwable err;

    /** Timeout object. */
    @GridToStringExclude
    private LockTimeoutObject timeoutObj;

    /** Lock timeout. */
    private final long timeout;

    /** Filter. */
    private final CacheEntryPredicate[] filter;

    /** Transaction. */
    @GridToStringExclude
    private final GridNearTxLocal tx;

    /** Topology snapshot to operate on. */
    private volatile AffinityTopologyVersion topVer;

    /** Map of current values. */
    private final Map<KeyCacheObject, IgniteBiTuple<GridCacheVersion, CacheObject>> valMap;

    /** Trackable flag (here may be non-volatile). */
    private boolean trackable;

    /** TTL for create operation. */
    private final long createTtl;

    /** TTL for read operation. */
    private final long accessTtl;

    /** Skip store flag. */
    private final boolean skipStore;

    /** */
    private Deque<GridNearLockMapping> mappings;

    /** Keep binary. */
    private final boolean keepBinary;

    /**
     * @param cctx Registry.
     * @param keys Keys to lock.
     * @param tx Transaction.
     * @param read Read flag.
     * @param retval Flag to return value or not.
     * @param timeout Lock acquisition timeout.
     * @param createTtl TTL for create operation.
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
        long createTtl,
        long accessTtl,
        CacheEntryPredicate[] filter,
        boolean skipStore,
        boolean keepBinary) {
        super(CU.boolReducer());

        assert keys != null;

        this.cctx = cctx;
        this.keys = keys;
        this.tx = tx;
        this.read = read;
        this.retval = retval;
        this.timeout = timeout;
        this.createTtl = createTtl;
        this.accessTtl = accessTtl;
        this.filter = filter;
        this.skipStore = skipStore;
        this.keepBinary = keepBinary;

        ignoreInterrupts(true);

        threadId = tx == null ? Thread.currentThread().getId() : tx.threadId();

        lockVer = tx != null ? tx.xidVersion() : cctx.versions().next();

        futId = IgniteUuid.randomUuid();

        if (log == null) {
            msgLog = cctx.shared().txLockMessageLogger();
            log = U.logger(cctx.kernalContext(), logRef, GridDhtColocatedLockFuture.class);
        }

        if (timeout > 0) {
            timeoutObj = new LockTimeoutObject();

            cctx.time().addTimeoutObject(timeoutObj);
        }

        valMap = new ConcurrentHashMap8<>();
    }

    /** {@inheritDoc} */
    @Override public GridCacheVersion version() {
        return lockVer;
    }

    /** {@inheritDoc} */
    @Override public boolean onOwnerChanged(GridCacheEntryEx entry, GridCacheMvccCandidate owner) {
        return false;
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
        IgniteTxKey txKey = entry.txKey();

        GridCacheMvccCandidate cand = cctx.mvcc().explicitLock(threadId, txKey);

        if (inTx()) {
            if (cand != null) {
                if (!tx.implicit())
                    throw new IgniteCheckedException("Cannot access key within transaction if lock is " +
                        "externally held [key=" + entry.key() + ", entry=" + entry + ']');
                else
                    return null;
            }
            else {
                IgniteTxEntry txEntry = tx.entry(txKey);

                txEntry.cached(entry);

                // Check transaction entries (corresponding tx entries must be enlisted in transaction).
                cand = new GridCacheMvccCandidate(entry,
                    cctx.localNodeId(),
                    null,
                    null,
                    threadId,
                    lockVer,
                    true,
                    tx.entry(txKey).locked(),
                    inTx(),
                    inTx() && tx.implicitSingle(),
                    false,
                    false,
                    null,
                    false);

                cand.topologyVersion(topVer);
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
                    true,
                    false,
                    inTx(),
                    inTx() && tx.implicitSingle(),
                    false,
                    false,
                    null,
                    false);

                cand.topologyVersion(topVer);
            }
            else
                cand = cand.reenter();

            cctx.mvcc().addExplicitLock(threadId, cand, topVer);
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
            MiniFuture mini = miniFuture(res.miniId());

            if (mini != null) {
                assert mini.node().id().equals(nodeId);

                mini.onResult(res);

                return;
            }

            U.warn(msgLog, "Collocated lock fut, failed to find mini future [txId=" + lockVer +
                ", inTx=" + inTx() +
                ", node=" + nodeId +
                ", res=" + res +
                ", fut=" + this + ']');
        }
        else {
            if (msgLog.isDebugEnabled()) {
                msgLog.debug("Collocated lock fut, response for finished future [txId=" + lockVer +
                    ", inTx=" + inTx() +
                    ", node=" + nodeId + ']');
            }
        }
    }

    /**
     * @return Keys for which locks requested from remote nodes but response isn't received.
     */
    public Set<IgniteTxKey> requestedKeys() {
        synchronized (sync) {
            if (timeoutObj != null && timeoutObj.requestedKeys != null)
                return timeoutObj.requestedKeys;

            return requestedKeys0();
        }
    }

    /**
     * @return Keys for which locks requested from remote nodes but response isn't received.
     */
    private Set<IgniteTxKey> requestedKeys0() {
        for (IgniteInternalFuture<Boolean> miniFut : futures()) {
            if (isMini(miniFut) && !miniFut.isDone()) {
                MiniFuture mini = (MiniFuture)miniFut;

                Set<IgniteTxKey> requestedKeys = U.newHashSet(mini.keys.size());

                for (KeyCacheObject key : mini.keys)
                    requestedKeys.add(new IgniteTxKey(key, cctx.cacheId()));

                return requestedKeys;
            }
        }

        return null;
    }

    /**
     * Finds pending mini future by the given mini ID.
     *
     * @param miniId Mini ID to find.
     * @return Mini future.
     */
    @SuppressWarnings({"ForLoopReplaceableByForEach", "IfMayBeConditional"})
    private MiniFuture miniFuture(IgniteUuid miniId) {
        // We iterate directly over the futs collection here to avoid copy.
        synchronized (sync) {
            int size = futuresCountNoLock();

            // Avoid iterator creation.
            for (int i = 0; i < size; i++) {
                IgniteInternalFuture<Boolean> fut = future(i);

                if (!isMini(fut))
                    continue;

                MiniFuture mini = (MiniFuture)fut;

                if (mini.futureId().equals(miniId)) {
                    if (!mini.isDone())
                        return mini;
                    else
                        return null;
                }
            }
        }

        return null;
    }

    /**
     * @param t Error.
     */
    private synchronized void onError(Throwable t) {
        if (err == null && !(t instanceof GridCacheLockTimeoutException))
            err = t;
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

        // Local GridDhtLockFuture
        if (inTx() && this.err instanceof IgniteTxTimeoutCheckedException && cctx.tm().deadlockDetectionEnabled())
            return false;

        if (isDone())
            return false;

        if (err != null)
            onError(err);

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

        if (super.onDone(success, err)) {
            if (log.isDebugEnabled())
                log.debug("Completing future: " + this);

            // Clean up.
            cctx.mvcc().removeMvccFuture(this);

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
        Collection<String> futs = F.viewReadOnly(futures(), new C1<IgniteInternalFuture<?>, String>() {
            @Override public String apply(IgniteInternalFuture<?> f) {
                if (isMini(f)) {
                    MiniFuture m = (MiniFuture)f;

                    return "[node=" + m.node().id() + ", loc=" + m.node().isLocal() + ", done=" + f.isDone() + "]";
                }
                else
                    return "[loc=true, done=" + f.isDone() + "]";
            }
        });

        return S.toString(GridDhtColocatedLockFuture.class, this,
            "innerFuts", futs,
            "inTx", inTx(),
            "super", super.toString());
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
        if (topVer == null && tx != null && tx.system())
            topVer = cctx.tm().lockedTopologyVersion(Thread.currentThread().getId(), tx);

        if (topVer != null && tx != null)
            tx.topologyVersion(topVer);

        if (topVer == null && tx != null)
            topVer = tx.topologyVersionSnapshot();

        if (topVer != null) {
            for (GridDhtTopologyFuture fut : cctx.shared().exchange().exchangeFutures()){
                if (fut.topologyVersion().equals(topVer)){
                    Throwable err = fut.validateCache(cctx);

                    if (err != null) {
                        onDone(err);

                        return;
                    }

                    break;
                }
            }

            // Continue mapping on the same topology version as it was before.
            synchronized (this) {
                if (this.topVer == null)
                    this.topVer = topVer;
            }

            map(keys, false, true);

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
                Throwable err = fut.validateCache(cctx);

                if (err != null) {
                    onDone(err);

                    return;
                }

                AffinityTopologyVersion topVer = fut.topologyVersion();

                if (remap) {
                    if (tx != null)
                        tx.onRemap(topVer);

                    synchronized (this) {
                        this.topVer = topVer;
                    }
                }
                else {
                    if (tx != null)
                        tx.topologyVersion(topVer);

                    synchronized (this) {
                        if (this.topVer == null)
                            this.topVer = topVer;
                    }
                }

                map(keys, remap, false);

                if (c != null)
                    c.run();

                markInitialized();
            }
            else {
                fut.listen(new CI1<IgniteInternalFuture<AffinityTopologyVersion>>() {
                    @Override public void apply(IgniteInternalFuture<AffinityTopologyVersion> fut) {
                        try {
                            fut.get();

                            mapOnTopology(remap, c);
                        }
                        catch (IgniteCheckedException e) {
                            onDone(e);
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
     * @param topLocked {@code True} if thread already acquired lock preventing topology change.
     */
    private void map(Collection<KeyCacheObject> keys, boolean remap, boolean topLocked) {
        try {
            map0(
                keys,
                remap,
                topLocked);
        }
        catch (IgniteCheckedException ex) {
            onDone(false, ex);
        }
    }

    /**
     * @param keys Keys to map.
     * @param remap Remap flag.
     * @param topLocked Topology locked flag.
     * @throws IgniteCheckedException If mapping failed.
     */
    private synchronized void map0(
        Collection<KeyCacheObject> keys,
        boolean remap,
        boolean topLocked
    ) throws IgniteCheckedException {
        AffinityTopologyVersion topVer = this.topVer;

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

        mappings = new ArrayDeque<>();

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
                                    clientFirst = clientNode &&
                                        !topLocked &&
                                        (tx == null || !tx.hasRemoteLocks());

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
                                        inTx() && tx.syncMode() == FULL_SYNC,
                                        inTx() ? tx.subjectId() : null,
                                        inTx() ? tx.taskNameHash() : 0,
                                        read ? createTtl : -1L,
                                        read ? accessTtl : -1L,
                                        skipStore,
                                        keepBinary,
                                        clientFirst,
                                        cctx.deploymentEnabled());

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

        proceedMapping();
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    private void proceedMapping() throws IgniteCheckedException {
        boolean set = tx != null && cctx.shared().tm().setTxTopologyHint(tx.topologyVersionSnapshot());

        try {
            proceedMapping0();
        }
        finally {
            if (set)
                cctx.tm().setTxTopologyHint(null);
        }
    }

    /**
     * Gets next near lock mapping and either acquires dht locks locally or sends near lock request to
     * remote primary node.
     *
     * @throws IgniteCheckedException If mapping can not be completed.
     */
    private void proceedMapping0()
        throws IgniteCheckedException {
        GridNearLockMapping map;

        synchronized (this) {
            map = mappings.poll();
        }

        // If there are no more mappings to process, complete the future.
        if (map == null)
            return;

        final GridNearLockRequest req = map.request();
        final Collection<KeyCacheObject> mappedKeys = map.distributedKeys();
        final ClusterNode node = map.node();

        if (filter != null && filter.length != 0)
            req.filter(filter, cctx);

        if (node.isLocal())
            lockLocally(mappedKeys, req.topologyVersion());
        else {
            final MiniFuture fut = new MiniFuture(node, mappedKeys);

            req.miniId(fut.futureId());

            add(fut); // Append new future.

            IgniteInternalFuture<?> txSync = null;

            if (inTx())
                txSync = cctx.tm().awaitFinishAckAsync(node.id(), tx.threadId());

            if (txSync == null || txSync.isDone()) {
                try {
                    cctx.io().send(node, req, cctx.ioPolicy());

                    if (msgLog.isDebugEnabled()) {
                        msgLog.debug("Collocated lock fut, sent request [txId=" + lockVer +
                            ", inTx=" + inTx() +
                            ", node=" + node.id() + ']');
                    }
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
                            cctx.io().send(node, req, cctx.ioPolicy());

                            if (msgLog.isDebugEnabled()) {
                                msgLog.debug("Collocated lock fut, sent request [txId=" + lockVer +
                                    ", inTx=" + inTx() +
                                    ", node=" + node.id() + ']');
                            }
                        }
                        catch (ClusterTopologyCheckedException ex) {
                            assert fut != null;

                            fut.onResult(ex);
                        }
                        catch (IgniteCheckedException e) {
                            if (msgLog.isDebugEnabled()) {
                                msgLog.debug("Collocated lock fut, failed to send request [txId=" + lockVer +
                                    ", inTx=" + inTx() +
                                    ", node=" + node.id() +
                                    ", err=" + e + ']');
                            }

                            onError(e);
                        }
                    }
                });
            }
        }
    }

    /**
     * Locks given keys directly through dht cache.
     * @param keys Collection of keys.
     * @param topVer Topology version to lock on.
     */
    private void lockLocally(
        final Collection<KeyCacheObject> keys,
        AffinityTopologyVersion topVer
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
            createTtl,
            accessTtl,
            filter,
            skipStore,
            keepBinary);

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
                            cctx.mvcc().markExplicitOwner(cctx.txKey(key), threadId);
                    }

                    try {
                        // Proceed and add new future (if any) before completing embedded future.
                        if (mappings != null)
                            proceedMapping();
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
    private boolean mapAsPrimary(Collection<KeyCacheObject> keys, AffinityTopologyVersion topVer)
        throws IgniteCheckedException {
        // Assign keys to primary nodes.
        Collection<KeyCacheObject> distributedKeys = new ArrayList<>(keys.size());

        boolean explicit = false;

        for (KeyCacheObject key : keys) {
            if (!cctx.affinity().primaryByKey(cctx.localNode(), key, topVer)) {
                // Remove explicit locks added so far.
                for (KeyCacheObject k : keys)
                    cctx.mvcc().removeExplicitLock(threadId, cctx.txKey(k), lockVer);

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

            lockLocally(distributedKeys, topVer);
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

        ClusterNode primary = cctx.affinity().primaryByKey(key, topVer);

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

        topEx.retryReadyFuture(cctx.shared().nextAffinityReadyFuture(topVer));

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

        /** Requested keys. */
        private Set<IgniteTxKey> requestedKeys;

        /** {@inheritDoc} */
        @Override public void onTimeout() {
            if (log.isDebugEnabled())
                log.debug("Timed out waiting for lock response: " + this);

            if (inTx() && cctx.tm().deadlockDetectionEnabled()) {
                synchronized (sync) {
                    requestedKeys = requestedKeys0();

                    clear(); // Stop response processing.
                }

                Set<IgniteTxKey> keys = new HashSet<>();

                for (IgniteTxEntry txEntry : tx.allEntries()) {
                    if (!txEntry.locked())
                        keys.add(txEntry.txKey());
                }

                IgniteInternalFuture<TxDeadlock> fut = cctx.tm().detectDeadlock(tx, keys);

                fut.listen(new IgniteInClosure<IgniteInternalFuture<TxDeadlock>>() {
                    @Override public void apply(IgniteInternalFuture<TxDeadlock> fut) {
                        try {
                            TxDeadlock deadlock = fut.get();

                            if (deadlock != null)
                                err = new TransactionDeadlockException(deadlock.toString(cctx.shared()));
                        }
                        catch (IgniteCheckedException e) {
                            err = e;

                            U.warn(log, "Failed to detect deadlock.", e);
                        }

                        onComplete(false, true);
                    }
                });
            }
            else
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
        private final ClusterNode node;

        /** Keys. */
        @GridToStringInclude
        private final Collection<KeyCacheObject> keys;

        /** */
        private boolean rcvRes;

        /**
         * @param node Node.
         * @param keys Keys.
         */
        MiniFuture(
            ClusterNode node,
            Collection<KeyCacheObject> keys
        ) {
            this.node = node;
            this.keys = keys;
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
         * @param e Node left exception.
         */
        void onResult(ClusterTopologyCheckedException e) {
            if (msgLog.isDebugEnabled()) {
                msgLog.debug("Collocated lock fut, mini future node left [txId=" + lockVer +
                    ", inTx=" + inTx() +
                    ", nodeId=" + node.id() + ']');
            }

            if (isDone())
                return;

            synchronized (this) {
                if (rcvRes)
                    return;

                rcvRes = true;
            }

            if (tx != null)
                tx.removeMapping(node.id());

            // Primary node left the grid, so fail the future.
            GridDhtColocatedLockFuture.this.onDone(false, newTopologyException(e, node.id()));

            onDone(true);
        }

        /**
         * @param res Result callback.
         */
        void onResult(GridNearLockResponse res) {
            synchronized (this) {
                if (rcvRes)
                    return;

                rcvRes = true;
            }

            if (res.error() != null) {
                if (inTx() && res.error() instanceof IgniteTxTimeoutCheckedException &&
                    cctx.tm().deadlockDetectionEnabled())
                    return;

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
                                fut.get();

                                remap();
                            }
                            catch (IgniteCheckedException e) {
                                onDone(e);
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
            else {
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
                        cctx.mvcc().markExplicitOwner(cctx.txKey(k), threadId);

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
                            tx == null ? null : tx.resolveTaskName(),
                            keepBinary);
                    }

                    i++;
                }

                try {
                    proceedMapping();
                }
                catch (IgniteCheckedException e) {
                    onDone(e);
                }

                onDone(true);
            }
        }

        /**
         *
         */
        private void remap() {
            undoLocks(false, false);

            for (KeyCacheObject key : GridDhtColocatedLockFuture.this.keys)
                cctx.mvcc().removeExplicitLock(threadId, cctx.txKey(key), lockVer);

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
