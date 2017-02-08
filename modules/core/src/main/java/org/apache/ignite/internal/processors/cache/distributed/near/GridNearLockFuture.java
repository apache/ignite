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

package org.apache.ignite.internal.processors.cache.distributed.near;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
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
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTransactionalCacheAdapter;
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
 * Cache lock future.
 */
public final class GridNearLockFuture extends GridCompoundIdentityFuture<Boolean>
    implements GridCacheMvccFuture<Boolean> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Logger reference. */
    private static final AtomicReference<IgniteLogger> logRef = new AtomicReference<>();

    /** */
    private static IgniteLogger log;

    /** Cache registry. */
    @GridToStringExclude
    private final GridCacheContext<?, ?> cctx;

    /** Lock owner thread. */
    @GridToStringInclude
    private long threadId;

    /** Keys to lock. */
    private final Collection<KeyCacheObject> keys;

    /** Future ID. */
    private final IgniteUuid futId;

    /** Lock version. */
    private final GridCacheVersion lockVer;

    /** Read flag. */
    private boolean read;

    /** Flag to return value. */
    private final boolean retval;

    /** Error. */
    private volatile Throwable err;

    /** Timed out flag. */
    private volatile boolean timedOut;

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

    /** Trackable flag. */
    private boolean trackable = true;

    /** Keys locked so far. */
    @SuppressWarnings({"FieldAccessedSynchronizedAndUnsynchronized"})
    @GridToStringExclude
    private List<GridDistributedCacheEntry> entries;

    /** TTL for create operation. */
    private long createTtl;

    /** TTL for read operation. */
    private long accessTtl;

    /** Skip store flag. */
    private final boolean skipStore;

    /** Mappings to proceed. */
    @GridToStringExclude
    private Queue<GridNearLockMapping> mappings;

    /** Keep binary context flag. */
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
     * @param skipStore skipStore
     * @param keepBinary Keep binary flag.
     */
    public GridNearLockFuture(
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
        assert (tx != null && timeout >= 0) || tx == null;

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

        entries = new ArrayList<>(keys.size());

        if (log == null)
            log = U.logger(cctx.kernalContext(), logRef, GridNearLockFuture.class);

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

    /**
     * @return Entries.
     */
    public synchronized List<GridDistributedCacheEntry> entriesCopy() {
        return new ArrayList<>(entries);
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
     * @param cached Entry.
     * @return {@code True} if locked.
     * @throws GridCacheEntryRemovedException If removed.
     */
    private boolean locked(GridCacheEntryEx cached) throws GridCacheEntryRemovedException {
        // Reentry-aware check (If filter failed, lock is failed).
        return cached.lockedLocallyByIdOrThread(lockVer, threadId) && filter(cached);
    }

    /**
     * Adds entry to future.
     *
     * @param topVer Topology version.
     * @param entry Entry to add.
     * @param dhtNodeId DHT node ID.
     * @return Lock candidate.
     * @throws GridCacheEntryRemovedException If entry was removed.
     */
    @Nullable private GridCacheMvccCandidate addEntry(
        AffinityTopologyVersion topVer,
        GridNearCacheEntry entry,
        UUID dhtNodeId
    ) throws GridCacheEntryRemovedException {
        assert Thread.holdsLock(this);

        // Check if lock acquisition is timed out.
        if (timedOut)
            return null;

        // Add local lock first, as it may throw GridCacheEntryRemovedException.
        GridCacheMvccCandidate c = entry.addNearLocal(
            dhtNodeId,
            threadId,
            lockVer,
            topVer,
            timeout,
            !inTx(),
            inTx(),
            implicitSingleTx(),
            false
        );

        if (inTx()) {
            IgniteTxEntry txEntry = tx.entry(entry.txKey());

            txEntry.cached(entry);
        }

        entries.add(entry);

        if (c == null && timeout < 0) {
            if (log.isDebugEnabled())
                log.debug("Failed to acquire lock with negative timeout: " + entry);

            onFailed(false);

            return null;
        }

        // Double check if lock acquisition has already timed out.
        if (timedOut) {
            entry.removeLock(lockVer);

            return null;
        }

        return c;
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
            cctx.nearTx().removeLocks(lockVer, keys);
        else {
            if (rollback && tx != null) {
                if (tx.setRollbackOnly()) {
                    if (log.isDebugEnabled())
                        log.debug("Marked transaction as rollback only because locks could not be acquired: " + tx);
                }
                else if (log.isDebugEnabled())
                    log.debug("Transaction was not marked rollback-only while locks were not acquired: " + tx);
            }

            for (GridCacheEntryEx e : entriesCopy()) {
                try {
                    e.removeLock(lockVer);
                }
                catch (GridCacheEntryRemovedException ignored) {
                    while (true) {
                        try {
                            e = cctx.cache().peekEx(e.key());

                            if (e != null)
                                e.removeLock(lockVer);

                            break;
                        }
                        catch (GridCacheEntryRemovedException ignore) {
                            if (log.isDebugEnabled())
                                log.debug("Attempted to remove lock on removed entry (will retry) [ver=" +
                                    lockVer + ", entry=" + e + ']');
                        }
                    }
                }
            }
        }

        cctx.mvcc().recheckPendingLocks();
    }

    /**
     *
     * @param dist {@code True} if need to distribute lock release.
     */
    private void onFailed(boolean dist) {
        undoLocks(dist, true);

        complete(false);
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
    @SuppressWarnings({"ThrowableInstanceNeverThrown"})
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

        if (!found) {
            if (log.isDebugEnabled())
                log.debug("Near lock future does not have mapping for left node (ignoring) [nodeId=" + nodeId +
                    ", fut=" + this + ']');
        }

        return found;
    }

    /**
     * @param nodeId Sender.
     * @param res Result.
     */
    void onResult(UUID nodeId, GridNearLockResponse res) {
        if (!isDone()) {
            if (log.isDebugEnabled())
                log.debug("Received lock response from node [nodeId=" + nodeId + ", res=" + res + ", fut=" + this + ']');

            MiniFuture mini = miniFuture(res.miniId());

            if (mini != null) {
                assert mini.node().id().equals(nodeId);

                if (log.isDebugEnabled())
                    log.debug("Found mini future for response [mini=" + mini + ", res=" + res + ']');

                mini.onResult(res);

                if (log.isDebugEnabled())
                    log.debug("Future after processed lock response [fut=" + this + ", mini=" + mini +
                        ", res=" + res + ']');

                return;
            }

            U.warn(log, "Failed to find mini future for response (perhaps due to stale message) [res=" + res +
                ", fut=" + this + ']');
        }
        else if (log.isDebugEnabled())
            log.debug("Ignoring lock response from node (future is done) [nodeId=" + nodeId + ", res=" + res +
                ", fut=" + this + ']');
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
    private void onError(Throwable t) {
        synchronized (this) {
            if (err == null)
                err = t;
        }
    }

    /**
     * @param cached Entry to check.
     * @return {@code True} if filter passed.
     */
    private boolean filter(GridCacheEntryEx cached) {
        try {
            if (!cctx.isAll(cached, filter)) {
                if (log.isDebugEnabled())
                    log.debug("Filter didn't pass for entry (will fail lock): " + cached);

                onFailed(true);

                return false;
            }

            return true;
        }
        catch (IgniteCheckedException e) {
            onError(e);

            return false;
        }
    }

    /**
     * Callback for whenever entry lock ownership changes.
     *
     * @param entry Entry whose lock ownership changed.
     */
    @Override public boolean onOwnerChanged(GridCacheEntryEx entry, GridCacheMvccCandidate owner) {
        if (owner != null && owner.nearLocal() && owner.version().equals(lockVer)) {
            onDone(true);

            return true;
        }

        return false;
    }

    /**
     * @return {@code True} if locks have been acquired.
     */
    private boolean checkLocks() {
        if (!isDone() && initialized() && !hasPending()) {
            synchronized (this) {
                for (int i = 0; i < entries.size(); i++) {
                    while (true) {
                        GridCacheEntryEx cached = entries.get(i);

                        try {
                            if (!locked(cached)) {
                                if (log.isDebugEnabled())
                                    log.debug("Lock is still not acquired for entry (will keep waiting) [entry=" +
                                        cached + ", fut=" + this + ']');

                                return false;
                            }

                            break;
                        }
                        // Possible in concurrent cases, when owner is changed after locks
                        // have been released or cancelled.
                        catch (GridCacheEntryRemovedException ignore) {
                            if (log.isDebugEnabled())
                                log.debug("Got removed entry in onOwnerChanged method (will retry): " + cached);

                            // Replace old entry with new one.
                            entries.set(
                                i,
                                (GridDistributedCacheEntry)cctx.cache().entryEx(cached.key()));
                        }
                    }
                }

                if (log.isDebugEnabled())
                    log.debug("Local lock acquired for entries [fut=" + this + ", entries=" + entries + "]");
            }

            onComplete(true, true);

            return true;
        }

        return false;
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

        if (inTx() && cctx.tm().deadlockDetectionEnabled() &&
            (this.err instanceof IgniteTxTimeoutCheckedException || timedOut))
            return false;

        // If locks were not acquired yet, delay completion.
        if (isDone() || (err == null && success && !checkLocks()))
            return false;

        if (err != null && !(err instanceof GridCacheLockTimeoutException))
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

        return S.toString(GridNearLockFuture.class, this,
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
        long threadId = Thread.currentThread().getId();

        AffinityTopologyVersion topVer = cctx.mvcc().lastExplicitLockTopologyVersion(threadId);

        // If there is another system transaction in progress, use it's topology version to prevent deadlock.
        if (topVer == null && tx != null && tx.system())
            topVer = cctx.tm().lockedTopologyVersion(threadId, tx);

        if (topVer != null && tx != null)
            tx.topologyVersion(topVer);

        if (topVer == null && tx != null)
            topVer = tx.topologyVersionSnapshot();

        if (topVer != null) {
            for (GridDhtTopologyFuture fut : cctx.shared().exchange().exchangeFutures()) {
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
            if (this.topVer == null)
                this.topVer = topVer;

            map(keys, false, true);

            markInitialized();

            return;
        }

        // Must get topology snapshot and map on that version.
        mapOnTopology(false);
    }

    /**
     * Acquires topology future and checks it completeness under the read lock. If it is not complete,
     * will asynchronously wait for it's completeness and then try again.
     *
     * @param remap Remap flag.
     */
    synchronized void mapOnTopology(final boolean remap) {
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

                    this.topVer = topVer;
                }
                else {
                    if (tx != null)
                        tx.topologyVersion(topVer);

                    if (this.topVer == null)
                        this.topVer = topVer;
                }

                map(keys, remap, false);

                markInitialized();
            }
            else {
                fut.listen(new CI1<IgniteInternalFuture<AffinityTopologyVersion>>() {
                    @Override public void apply(IgniteInternalFuture<AffinityTopologyVersion> fut) {
                        try {
                            fut.get();

                            mapOnTopology(remap);
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
    private void map(Iterable<KeyCacheObject> keys, boolean remap, boolean topLocked) {
        try {
            AffinityTopologyVersion topVer = this.topVer;

            assert topVer != null;

            assert topVer.topologyVersion() > 0 : topVer;

            if (CU.affinityNodes(cctx, topVer).isEmpty()) {
                onDone(new ClusterTopologyServerNotFoundException("Failed to map keys for near-only cache (all " +
                    "partition nodes left the grid)."));

                return;
            }

            boolean clientNode = cctx.kernalContext().clientNode();

            assert !remap || (clientNode && (tx == null || !tx.hasRemoteLocks()));

            synchronized (this) {
                mappings = new ArrayDeque<>();

                // Assign keys to primary nodes.
                GridNearLockMapping map = null;

                for (KeyCacheObject key : keys) {
                    GridNearLockMapping updated = map(
                        key,
                        map,
                        topVer);

                    // If new mapping was created, add to collection.
                    if (updated != map) {
                        mappings.add(updated);

                        if (tx != null && updated.node().isLocal())
                            tx.nearLocallyMapped(true);
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

                boolean first = true;

                // Create mini futures.
                for (Iterator<GridNearLockMapping> iter = mappings.iterator(); iter.hasNext(); ) {
                    GridNearLockMapping mapping = iter.next();

                    ClusterNode node = mapping.node();
                    Collection<KeyCacheObject> mappedKeys = mapping.mappedKeys();

                    assert !mappedKeys.isEmpty();

                    GridNearLockRequest req = null;

                    Collection<KeyCacheObject> distributedKeys = new ArrayList<>(mappedKeys.size());

                    boolean explicit = false;

                    for (KeyCacheObject key : mappedKeys) {
                        IgniteTxKey txKey = cctx.txKey(key);

                        while (true) {
                            GridNearCacheEntry entry = null;

                            try {
                                entry = cctx.near().entryExx(
                                    key,
                                    topVer);

                                if (!cctx.isAll(
                                    entry,
                                    filter)) {
                                    if (log.isDebugEnabled())
                                        log.debug("Entry being locked did not pass filter (will not lock): " + entry);

                                    onComplete(
                                        false,
                                        false);

                                    return;
                                }

                                // Removed exception may be thrown here.
                                GridCacheMvccCandidate cand = addEntry(
                                    topVer,
                                    entry,
                                    node.id());

                                if (isDone()) {
                                    if (log.isDebugEnabled())
                                        log.debug("Abandoning (re)map because future is done after addEntry attempt " +
                                            "[fut=" + this + ", entry=" + entry + ']');

                                    return;
                                }

                                if (cand != null) {
                                    if (tx == null && !cand.reentry())
                                        cctx.mvcc().addExplicitLock(
                                            threadId,
                                            cand,
                                            topVer);

                                    IgniteBiTuple<GridCacheVersion, CacheObject> val = entry.versionedValue();

                                    if (val == null) {
                                        GridDhtCacheEntry dhtEntry = dht().peekExx(key);

                                        try {
                                            if (dhtEntry != null)
                                                val = dhtEntry.versionedValue(topVer);
                                        }
                                        catch (GridCacheEntryRemovedException ignored) {
                                            assert dhtEntry.obsolete() : dhtEntry;

                                            if (log.isDebugEnabled())
                                                log.debug("Got removed exception for DHT entry in map (will ignore): "
                                                    + dhtEntry);
                                        }
                                    }

                                    GridCacheVersion dhtVer = null;

                                    if (val != null) {
                                        dhtVer = val.get1();

                                        valMap.put(
                                            key,
                                            val);
                                    }

                                    if (!cand.reentry()) {
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
                                            tx.addKeyMapping(
                                                txKey,
                                                mapping.node());

                                        req.addKeyBytes(
                                            key,
                                            retval && dhtVer == null,
                                            dhtVer,
                                            // Include DHT version to match remote DHT entry.
                                            cctx);
                                    }

                                    if (cand.reentry())
                                        explicit = tx != null && !entry.hasLockCandidate(tx.xidVersion());
                                }
                                else
                                    // Ignore reentries within transactions.
                                    explicit = tx != null && !entry.hasLockCandidate(tx.xidVersion());

                                if (explicit)
                                    tx.addKeyMapping(
                                        txKey,
                                        mapping.node());

                                break;
                            }
                            catch (GridCacheEntryRemovedException ignored) {
                                assert entry.obsolete() : "Got removed exception on non-obsolete entry: " + entry;

                                if (log.isDebugEnabled())
                                    log.debug("Got removed entry in lockAsync(..) method (will retry): " + entry);
                            }
                        }

                        // Mark mapping explicit lock flag.
                        if (explicit) {
                            boolean marked = tx != null && tx.markExplicit(node.id());

                            assert tx == null || marked;
                        }
                    }

                    if (!distributedKeys.isEmpty())
                        mapping.distributedKeys(distributedKeys);
                    else {
                        assert mapping.request() == null;

                        iter.remove();
                    }
                }
            }

            cctx.mvcc().recheckPendingLocks();

            proceedMapping();
        }
        catch (IgniteCheckedException ex) {
            onError(ex);
        }
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
    @SuppressWarnings("unchecked")
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

        if (node.isLocal()) {
            req.miniId(IgniteUuid.randomUuid());

            if (log.isDebugEnabled())
                log.debug("Before locally locking near request: " + req);

            IgniteInternalFuture<GridNearLockResponse> fut = dht().lockAllAsync(cctx, cctx.localNode(), req, filter);

            // Add new future.
            add(new GridEmbeddedFuture<>(
                new C2<GridNearLockResponse, Exception, Boolean>() {
                    @Override public Boolean apply(GridNearLockResponse res, Exception e) {
                        if (CU.isLockTimeoutOrCancelled(e) ||
                            (res != null && CU.isLockTimeoutOrCancelled(res.error())))
                            return false;

                        if (e != null) {
                            onError(e);

                            return false;
                        }

                        if (res == null) {
                            onError(new IgniteCheckedException("Lock response is null for future: " + this));

                            return false;
                        }

                        if (res.error() != null) {
                            onError(res.error());

                            return false;
                        }

                        if (log.isDebugEnabled())
                            log.debug("Acquired lock for local DHT mapping [locId=" + cctx.nodeId() +
                                ", mappedKeys=" + mappedKeys + ", fut=" + GridNearLockFuture.this + ']');

                        try {
                            int i = 0;

                            for (KeyCacheObject k : mappedKeys) {
                                while (true) {
                                    GridNearCacheEntry entry = cctx.near().entryExx(k, req.topologyVersion());

                                    try {
                                        IgniteBiTuple<GridCacheVersion, CacheObject> oldValTup =
                                            valMap.get(entry.key());

                                        boolean hasBytes = entry.hasValue();
                                        CacheObject oldVal = entry.rawGet();
                                        CacheObject newVal = res.value(i);

                                        GridCacheVersion dhtVer = res.dhtVersion(i);
                                        GridCacheVersion mappedVer = res.mappedVersion(i);

                                        // On local node don't record twice if DHT cache already recorded.
                                        boolean record = retval && oldValTup != null && oldValTup.get1().equals(dhtVer);

                                        if (newVal == null) {
                                            if (oldValTup != null) {
                                                if (oldValTup.get1().equals(dhtVer))
                                                    newVal = oldValTup.get2();

                                                oldVal = oldValTup.get2();
                                            }
                                        }

                                        // Lock is held at this point, so we can set the
                                        // returned value if any.
                                        entry.resetFromPrimary(newVal, lockVer, dhtVer, node.id(), topVer);

                                        entry.readyNearLock(lockVer, mappedVer, res.committedVersions(),
                                            res.rolledbackVersions(), res.pending());

                                        if (inTx() && implicitTx() && tx.onePhaseCommit()) {
                                            boolean pass = res.filterResult(i);

                                            tx.entry(cctx.txKey(k)).filters(pass ? CU.empty0() : CU.alwaysFalse0Arr());
                                        }

                                        if (record) {
                                            if (cctx.events().isRecordable(EVT_CACHE_OBJECT_READ))
                                                cctx.events().addEvent(
                                                    entry.partition(),
                                                    entry.key(),
                                                    tx,
                                                    null,
                                                    EVT_CACHE_OBJECT_READ,
                                                    newVal,
                                                    newVal != null,
                                                    oldVal,
                                                    hasBytes,
                                                    CU.subjectId(tx, cctx.shared()),
                                                    null,
                                                    inTx() ? tx.resolveTaskName() : null,
                                                    keepBinary);

                                            if (cctx.cache().configuration().isStatisticsEnabled())
                                                cctx.cache().metrics0().onRead(oldVal != null);
                                        }

                                        if (log.isDebugEnabled())
                                            log.debug("Processed response for entry [res=" + res +
                                                ", entry=" + entry + ']');

                                        break; // Inner while loop.
                                    }
                                    catch (GridCacheEntryRemovedException ignored) {
                                        if (log.isDebugEnabled())
                                            log.debug("Failed to add candidates because entry was " +
                                                "removed (will renew).");

                                        synchronized (GridNearLockFuture.this) {
                                            // Replace old entry with new one.
                                            entries.set(i,
                                                (GridDistributedCacheEntry)cctx.cache().entryEx(entry.key()));
                                        }
                                    }
                                }

                                i++; // Increment outside of while loop.
                            }

                            // Proceed and add new future (if any) before completing embedded future.
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
        else {
            final MiniFuture fut = new MiniFuture(node, mappedKeys);

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
     * @param mapping Mappings.
     * @param key Key to map.
     * @param topVer Topology version.
     * @return Near lock mapping.
     * @throws IgniteCheckedException If mapping for key failed.
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
     * @return DHT cache.
     */
    private GridDhtTransactionalCacheAdapter<?, ?> dht() {
        return cctx.nearTx().dht();
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
        @SuppressWarnings({"ThrowableInstanceNeverThrown"})
        @Override public void onTimeout() {
            if (log.isDebugEnabled())
                log.debug("Timed out waiting for lock response: " + this);

            timedOut = true;

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
        private ClusterNode node;

        /** Keys. */
        @GridToStringInclude(sensitive = true)
        private Collection<KeyCacheObject> keys;

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
            if (isDone())
                return;

            synchronized (this) {
                if (!rcvRes)
                    rcvRes = true;
                else
                    return;
            }

            if (log.isDebugEnabled())
                log.debug("Remote node left grid while sending or waiting for reply (will fail): " + this);

            if (tx != null)
                tx.removeMapping(node.id());

            // Primary node left the grid, so fail the future.
            GridNearLockFuture.this.onDone(false, newTopologyException(e, node.id()));

            onDone(true);
        }

        /**
         * @param res Result callback.
         */
        void onResult(GridNearLockResponse res) {
            synchronized (this) {
                if (!rcvRes)
                    rcvRes = true;
                else
                    return;
            }

            if (res.error() != null) {
                if (inTx() && cctx.tm().deadlockDetectionEnabled() &&
                    (res.error() instanceof IgniteTxTimeoutCheckedException || tx.remainingTime() == -1))
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

                AffinityTopologyVersion topVer = GridNearLockFuture.this.topVer;

                for (KeyCacheObject k : keys) {
                    while (true) {
                        GridNearCacheEntry entry = cctx.near().entryExx(k, topVer);

                        try {
                            if (res.dhtVersion(i) == null) {
                                onDone(new IgniteCheckedException("Failed to receive DHT version from remote node " +
                                    "(will fail the lock): " + res));

                                return;
                            }

                            IgniteBiTuple<GridCacheVersion, CacheObject> oldValTup = valMap.get(entry.key());

                            CacheObject oldVal = entry.rawGet();
                            boolean hasOldVal = false;
                            CacheObject newVal = res.value(i);

                            boolean readRecordable = false;

                            if (retval) {
                                readRecordable = cctx.events().isRecordable(EVT_CACHE_OBJECT_READ);

                                if (readRecordable)
                                    hasOldVal = entry.hasValue();
                            }

                            GridCacheVersion dhtVer = res.dhtVersion(i);
                            GridCacheVersion mappedVer = res.mappedVersion(i);

                            if (newVal == null) {
                                if (oldValTup != null) {
                                    if (oldValTup.get1().equals(dhtVer))
                                        newVal = oldValTup.get2();

                                    oldVal = oldValTup.get2();
                                }
                            }

                            // Lock is held at this point, so we can set the
                            // returned value if any.
                            entry.resetFromPrimary(newVal, lockVer, dhtVer, node.id(), topVer);

                            if (inTx()) {
                                tx.hasRemoteLocks(true);

                                if (implicitTx() && tx.onePhaseCommit()) {
                                    boolean pass = res.filterResult(i);

                                    tx.entry(cctx.txKey(k)).filters(pass ? CU.empty0() : CU.alwaysFalse0Arr());
                                }
                            }

                            entry.readyNearLock(lockVer,
                                mappedVer,
                                res.committedVersions(),
                                res.rolledbackVersions(),
                                res.pending());

                            if (retval) {
                                if (readRecordable)
                                    cctx.events().addEvent(
                                        entry.partition(),
                                        entry.key(),
                                        tx,
                                        null,
                                        EVT_CACHE_OBJECT_READ,
                                        newVal,
                                        newVal != null,
                                        oldVal,
                                        hasOldVal,
                                        CU.subjectId(tx, cctx.shared()),
                                        null,
                                        inTx() ? tx.resolveTaskName() : null,
                                        keepBinary);

                                if (cctx.cache().configuration().isStatisticsEnabled())
                                    cctx.cache().metrics0().onRead(false);
                            }

                            if (log.isDebugEnabled())
                                log.debug("Processed response for entry [res=" + res + ", entry=" + entry + ']');

                            break; // Inner while loop.
                        }
                        catch (GridCacheEntryRemovedException ignored) {
                            if (log.isDebugEnabled())
                                log.debug("Failed to add candidates because entry was removed (will renew).");

                            synchronized (GridNearLockFuture.this) {
                                // Replace old entry with new one.
                                entries.set(i,
                                    (GridDistributedCacheEntry)cctx.cache().entryEx(entry.key()));
                            }
                        }
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

            mapOnTopology(true);

            onDone(true);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(MiniFuture.class, this, "node", node.id(), "super", super.toString());
        }
    }
}
