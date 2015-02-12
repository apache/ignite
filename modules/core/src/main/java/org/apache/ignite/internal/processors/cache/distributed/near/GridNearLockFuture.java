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

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.cluster.*;
import org.apache.ignite.internal.managers.discovery.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.processors.cache.distributed.*;
import org.apache.ignite.internal.processors.cache.distributed.dht.*;
import org.apache.ignite.internal.processors.cache.transactions.*;
import org.apache.ignite.internal.processors.cache.version.*;
import org.apache.ignite.internal.processors.timeout.*;
import org.apache.ignite.internal.util.future.*;
import org.apache.ignite.internal.util.lang.*;
import org.apache.ignite.internal.util.tostring.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.transactions.*;
import org.jdk8.backport.*;
import org.jetbrains.annotations.*;

import javax.cache.*;
import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.*;

import static org.apache.ignite.events.EventType.*;

/**
 * Cache lock future.
 */
public final class GridNearLockFuture<K, V> extends GridCompoundIdentityFuture<Boolean>
    implements GridCacheMvccFuture<K, V, Boolean> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Logger reference. */
    private static final AtomicReference<IgniteLogger> logRef = new AtomicReference<>();

    /** Cache registry. */
    @GridToStringExclude
    private GridCacheContext<K, V> cctx;

    /** Lock owner thread. */
    @GridToStringInclude
    private long threadId;

    /** Keys to lock. */
    private Collection<? extends K> keys;

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

    /** Timed out flag. */
    private volatile boolean timedOut;

    /** Timeout object. */
    @GridToStringExclude
    private LockTimeoutObject timeoutObj;

    /** Lock timeout. */
    private long timeout;

    /** Logger. */
    @GridToStringExclude
    private IgniteLogger log;

    /** Filter. */
    private IgnitePredicate<Cache.Entry<K, V>>[] filter;

    /** Transaction. */
    @GridToStringExclude
    private GridNearTxLocal<K, V> tx;

    /** Topology snapshot to operate on. */
    private AtomicReference<GridDiscoveryTopologySnapshot> topSnapshot =
        new AtomicReference<>();

    /** Map of current values. */
    private Map<K, GridTuple3<GridCacheVersion, V, byte[]>> valMap;

    /** Trackable flag. */
    private boolean trackable = true;

    /** Mutex. */
    private final Object mux = new Object();

    /** Keys locked so far. */
    @SuppressWarnings({"FieldAccessedSynchronizedAndUnsynchronized"})
    @GridToStringExclude
    private List<GridDistributedCacheEntry<K, V>> entries;

    /** TTL for read operation. */
    private long accessTtl;

    /**
     * Empty constructor required by {@link Externalizable}.
     */
    public GridNearLockFuture() {
        // No-op.
    }

    /**
     * @param cctx Registry.
     * @param keys Keys to lock.
     * @param tx Transaction.
     * @param read Read flag.
     * @param retval Flag to return value or not.
     * @param timeout Lock acquisition timeout.
     * @param accessTtl TTL for read operation.
     * @param filter Filter.
     */
    public GridNearLockFuture(
        GridCacheContext<K, V> cctx,
        Collection<? extends K> keys,
        @Nullable GridNearTxLocal<K, V> tx,
        boolean read,
        boolean retval,
        long timeout,
        long accessTtl,
        IgnitePredicate<Cache.Entry<K, V>>[] filter) {
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

        threadId = tx == null ? Thread.currentThread().getId() : tx.threadId();

        lockVer = tx != null ? tx.xidVersion() : cctx.versions().next();

        futId = IgniteUuid.randomUuid();

        entries = new ArrayList<>(keys.size());

        log = U.logger(ctx, logRef, GridNearLockFuture.class);

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
        return
            F.viewReadOnly(futures(), new IgniteClosure<IgniteInternalFuture<?>, ClusterNode>() {
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
     * @return Entries.
     */
    public List<GridDistributedCacheEntry<K, V>> entriesCopy() {
        synchronized (mux) {
            return new ArrayList<>(entries);
        }
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
    @Nullable private IgniteTxIsolation isolation() {
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
    private boolean locked(GridCacheEntryEx<K, V> cached) throws GridCacheEntryRemovedException {
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
    @Nullable private GridCacheMvccCandidate<K> addEntry(long topVer, GridNearCacheEntry<K, V> entry, UUID dhtNodeId)
        throws GridCacheEntryRemovedException {
        // Check if lock acquisition is timed out.
        if (timedOut)
            return null;

        // Add local lock first, as it may throw GridCacheEntryRemovedException.
        GridCacheMvccCandidate<K> c = entry.addNearLocal(
            dhtNodeId,
            threadId,
            lockVer,
            timeout,
            !inTx(),
            inTx(),
            implicitSingleTx()
        );

        if (inTx()) {
            IgniteTxEntry<K, V> txEntry = tx.entry(entry.txKey());

            txEntry.cached(entry, txEntry.keyBytes());
        }

        if (c != null)
            c.topologyVersion(topVer);

        synchronized (mux) {
            entries.add(entry);
        }

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
     */
    private void undoLocks(boolean dist) {
        // Transactions will undo during rollback.
        if (dist && tx == null)
            cctx.nearTx().removeLocks(lockVer, keys);
        else {
            if (tx != null) {
                if (tx.setRollbackOnly()) {
                    if (log.isDebugEnabled())
                        log.debug("Marked transaction as rollback only because locks could not be acquired: " + tx);
                }
                else if (log.isDebugEnabled())
                    log.debug("Transaction was not marked rollback-only while locks were not acquired: " + tx);
            }

            for (GridCacheEntryEx<K, V> e : entriesCopy()) {
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
        undoLocks(dist);

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
    void onResult(UUID nodeId, GridNearLockResponse<K, V> res) {
        if (!isDone()) {
            if (log.isDebugEnabled())
                log.debug("Received lock response from node [nodeId=" + nodeId + ", res=" + res + ", fut=" + this + ']');

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

    /**
     * @param cached Entry to check.
     * @return {@code True} if filter passed.
     */
    private boolean filter(GridCacheEntryEx<K, V> cached) {
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
    @Override public boolean onOwnerChanged(GridCacheEntryEx<K, V> entry, GridCacheMvccCandidate<K> owner) {
        if (owner != null && owner.version().equals(lockVer)) {
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
            for (int i = 0; i < entries.size(); i++) {
                while (true) {
                    GridCacheEntryEx<K, V> cached = entries.get(i);

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
                        entries.set(i, (GridDistributedCacheEntry<K, V>)cctx.cache().entryEx(cached.key()));
                    }
                }
            }

            if (log.isDebugEnabled())
                log.debug("Local lock acquired for entries [fut=" + this + ", entries=" + entries + "]");

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

        // If locks were not acquired yet, delay completion.
        if (isDone() || (err == null && success && !checkLocks()))
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
            undoLocks(distribute);

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
        return S.toString(GridNearLockFuture.class, this, "inTx", inTx(), "super", super.toString());
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
        GridDiscoveryTopologySnapshot snapshot = tx != null ? tx.topologySnapshot() :
            cctx.mvcc().lastExplicitLockTopologySnapshot(Thread.currentThread().getId());

        if (snapshot != null) {
            // Continue mapping on the same topology version as it was before.
            topSnapshot.compareAndSet(null, snapshot);

            map(keys);

            markInitialized();

            return;
        }

        // Must get topology snapshot and map on that version.
        mapOnTopology();
    }

    /**
     * Acquires topology future and checks it completeness under the read lock. If it is not complete,
     * will asynchronously wait for it's completeness and then try again.
     */
    void mapOnTopology() {
        // We must acquire topology snapshot from the topology version future.
        try {
            cctx.topology().readLock();

            try {
                GridDhtTopologyFuture fut = cctx.topologyVersionFuture();

                if (fut.isDone()) {
                    GridDiscoveryTopologySnapshot snapshot = fut.topologySnapshot();

                    if (tx != null) {
                        tx.topologyVersion(snapshot.topologyVersion());
                        tx.topologySnapshot(snapshot);
                    }

                    topSnapshot.compareAndSet(null, snapshot);

                    map(keys);

                    markInitialized();
                }
                else {
                    fut.listenAsync(new CI1<IgniteInternalFuture<Long>>() {
                        @Override public void apply(IgniteInternalFuture<Long> t) {
                            mapOnTopology();
                        }
                    });
                }
            }
            finally {
                cctx.topology().readUnlock();
            }
        }
        catch (IgniteCheckedException e) {
            onDone(e);
        }
    }

    /**
     * Maps keys to nodes. Note that we can not simply group keys by nodes and send lock request as
     * such approach does not preserve order of lock acquisition. Instead, keys are split in continuous
     * groups belonging to one primary node and locks for these groups are acquired sequentially.
     *
     * @param keys Keys.
     */
    private void map(Iterable<? extends K> keys) {
        try {
            GridDiscoveryTopologySnapshot snapshot = topSnapshot.get();

            assert snapshot != null;

            long topVer = snapshot.topologyVersion();

            assert topVer > 0;

            if (CU.affinityNodes(cctx, topVer).isEmpty()) {
                onDone(new ClusterTopologyCheckedException("Failed to map keys for near-only cache (all " +
                    "partition nodes left the grid)."));

                return;
            }

            ConcurrentLinkedDeque8<GridNearLockMapping<K, V>> mappings =
                new ConcurrentLinkedDeque8<>();

            // Assign keys to primary nodes.
            GridNearLockMapping<K, V> map = null;

            for (K key : keys) {
                GridNearLockMapping<K, V> updated = map(key, map, topVer);

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

            // Create mini futures.
            for (Iterator<GridNearLockMapping<K, V>> iter = mappings.iterator(); iter.hasNext(); ) {
                GridNearLockMapping<K, V> mapping = iter.next();

                ClusterNode node = mapping.node();
                Collection<K> mappedKeys = mapping.mappedKeys();

                assert !mappedKeys.isEmpty();

                GridNearLockRequest<K, V> req = null;

                Collection<K> distributedKeys = new ArrayList<>(mappedKeys.size());

                boolean explicit = false;

                for (K key : mappedKeys) {
                    IgniteTxKey<K> txKey = cctx.txKey(key);

                    while (true) {
                        GridNearCacheEntry<K, V> entry = null;

                        try {
                            entry = cctx.near().entryExx(key, topVer);

                            if (!cctx.isAll(entry.wrapLazyValue(), filter)) {
                                if (log.isDebugEnabled())
                                    log.debug("Entry being locked did not pass filter (will not lock): " + entry);

                                onComplete(false, false);

                                return;
                            }

                            // Removed exception may be thrown here.
                            GridCacheMvccCandidate<K> cand = addEntry(topVer, entry, node.id());

                            if (isDone()) {
                                if (log.isDebugEnabled())
                                    log.debug("Abandoning (re)map because future is done after addEntry attempt " +
                                        "[fut=" + this + ", entry=" + entry + ']');

                                return;
                            }

                            if (cand != null) {
                                if (tx == null && !cand.reentry())
                                    cctx.mvcc().addExplicitLock(threadId, cand, snapshot);

                                GridTuple3<GridCacheVersion, V, byte[]> val = entry.versionedValue();

                                if (val == null) {
                                    GridDhtCacheEntry<K, V> dhtEntry = dht().peekExx(key);

                                    try {
                                        if (dhtEntry != null)
                                            val = dhtEntry.versionedValue(topVer);
                                    }
                                    catch (GridCacheEntryRemovedException ignored) {
                                        assert dhtEntry.obsolete() : " Got removed exception for non-obsolete entry: "
                                            + dhtEntry;

                                        if (log.isDebugEnabled())
                                            log.debug("Got removed exception for DHT entry in map (will ignore): "
                                                + dhtEntry);
                                    }
                                }

                                GridCacheVersion dhtVer = null;

                                if (val != null) {
                                    dhtVer = val.get1();

                                    valMap.put(key, val);
                                }

                                if (!cand.reentry()) {
                                    if (req == null) {
                                        req = new GridNearLockRequest<>(
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
                                            isolation(),
                                            isInvalidate(),
                                            timeout,
                                            mappedKeys.size(),
                                            inTx() ? tx.size() : mappedKeys.size(),
                                            inTx() && tx.syncCommit(),
                                            inTx() ? tx.groupLockKey() : null,
                                            inTx() && tx.partitionLock(),
                                            inTx() ? tx.subjectId() : null,
                                            inTx() ? tx.taskNameHash() : 0,
                                            read ? accessTtl : -1L);

                                        mapping.request(req);
                                    }

                                    distributedKeys.add(key);

                                    IgniteTxEntry<K, V> writeEntry = tx != null ? tx.writeMap().get(txKey) : null;

                                    if (tx != null)
                                        tx.addKeyMapping(txKey, mapping.node());

                                    req.addKeyBytes(
                                        key,
                                        node.isLocal() ? null : entry.getOrMarshalKeyBytes(),
                                        retval && dhtVer == null,
                                        dhtVer, // Include DHT version to match remote DHT entry.
                                        writeEntry,
                                        inTx() ? tx.entry(txKey).drVersion() : null,
                                        cctx);

                                    // Clear transfer required flag since we are sending message.
                                    if (writeEntry != null)
                                        writeEntry.transferRequired(false);
                                }

                                if (cand.reentry())
                                    explicit = tx != null && !entry.hasLockCandidate(tx.xidVersion());
                            }
                            else
                                // Ignore reentries within transactions.
                                explicit = tx != null && !entry.hasLockCandidate(tx.xidVersion());

                            if (explicit)
                                tx.addKeyMapping(txKey, mapping.node());

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

            cctx.mvcc().recheckPendingLocks();

            proceedMapping(mappings);
        }
        catch (IgniteCheckedException ex) {
            onError(ex);
        }
    }

    /**
     * Gets next near lock mapping and either acquires dht locks locally or sends near lock request to
     * remote primary node.
     *
     * @param mappings Queue of mappings.
     * @throws IgniteCheckedException If mapping can not be completed.
     */
    private void proceedMapping(final ConcurrentLinkedDeque8<GridNearLockMapping<K, V>> mappings)
        throws IgniteCheckedException {
        GridNearLockMapping<K, V> map = mappings.poll();

        // If there are no more mappings to process, complete the future.
        if (map == null)
            return;

        final GridNearLockRequest<K, V> req = map.request();
        final Collection<K> mappedKeys = map.distributedKeys();
        final ClusterNode node = map.node();

        if (filter != null && filter.length != 0)
            req.filter(filter, cctx);

        if (node.isLocal()) {
            req.miniId(IgniteUuid.randomUuid());

            if (log.isDebugEnabled())
                log.debug("Before locally locking near request: " + req);

            IgniteInternalFuture<GridNearLockResponse<K, V>> fut = dht().lockAllAsync(cctx, cctx.localNode(), req, filter);

            // Add new future.
            add(new GridEmbeddedFuture<>(
                cctx.kernalContext(),
                fut,
                new C2<GridNearLockResponse<K, V>, Exception, Boolean>() {
                    @Override public Boolean apply(GridNearLockResponse<K, V> res, Exception e) {
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

                            for (K k : mappedKeys) {
                                while (true) {
                                    GridNearCacheEntry<K, V> entry = cctx.near().entryExx(k, req.topologyVersion());

                                    try {
                                        GridTuple3<GridCacheVersion, V, byte[]> oldValTup = valMap.get(entry.key());

                                        boolean hasBytes = entry.hasValue();
                                        V oldVal = entry.rawGet();
                                        V newVal = res.value(i);
                                        byte[] newBytes = res.valueBytes(i);

                                        GridCacheVersion dhtVer = res.dhtVersion(i);
                                        GridCacheVersion mappedVer = res.mappedVersion(i);

                                        // On local node don't record twice if DHT cache already recorded.
                                        boolean record = retval && oldValTup != null && oldValTup.get1().equals(dhtVer);

                                        if (newVal == null) {
                                            if (oldValTup != null) {
                                                if (oldValTup.get1().equals(dhtVer)) {
                                                    newVal = oldValTup.get2();

                                                    newBytes = oldValTup.get3();
                                                }

                                                oldVal = oldValTup.get2();
                                            }
                                        }

                                        // Lock is held at this point, so we can set the
                                        // returned value if any.
                                        entry.resetFromPrimary(newVal, newBytes, lockVer, dhtVer, node.id());

                                        entry.readyNearLock(lockVer, mappedVer, res.committedVersions(),
                                            res.rolledbackVersions(), res.pending());

                                        if (inTx() && implicitTx() && tx.onePhaseCommit()) {
                                            boolean pass = res.filterResult(i);

                                            tx.entry(cctx.txKey(k)).filters(pass ? CU.<K, V>empty() : CU.<K, V>alwaysFalse());
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
                                                    inTx() ? tx.resolveTaskName() : null);

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

                                        // Replace old entry with new one.
                                        entries.set(i, (GridDistributedCacheEntry<K, V>)
                                            cctx.cache().entryEx(entry.key()));
                                    }
                                }

                                i++; // Increment outside of while loop.
                            }

                            // Proceed and add new future (if any) before completing embedded future.
                            proceedMapping(mappings);
                        }
                        catch (IgniteCheckedException ex) {
                            onError(ex);

                            return false;
                        }

                        return true;
                    }
                }
            ));
        }
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
                txSync.listenAsync(new CI1<IgniteInternalFuture<?>>() {
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
     * @param mapping Mappings.
     * @param key Key to map.
     * @param topVer Topology version.
     * @return Near lock mapping.
     * @throws IgniteCheckedException If mapping for key failed.
     */
    private GridNearLockMapping<K, V> map(K key, @Nullable GridNearLockMapping<K, V> mapping,
        long topVer) throws IgniteCheckedException {
        assert mapping == null || mapping.node() != null;

        ClusterNode primary = cctx.affinity().primary(key, topVer);

        if (cctx.discovery().node(primary.id()) == null)
            // If primary node left the grid before lock acquisition, fail the whole future.
            throw newTopologyException(null, primary.id());

        if (inTx() && tx.groupLock() && !primary.isLocal())
            throw new IgniteCheckedException("Failed to start group lock transaction (local node is not primary for " +
                " key) [key=" + key + ", primaryNodeId=" + primary.id() + ']');

        if (mapping == null || !primary.id().equals(mapping.node().id()))
            mapping = new GridNearLockMapping<>(primary, key);
        else
            mapping.addKey(key);

        return mapping;
    }

    /**
     * @return DHT cache.
     */
    private GridDhtTransactionalCacheAdapter<K, V> dht() {
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
        return new ClusterTopologyCheckedException("Failed to acquire lock for keys (primary node left grid, " +
            "retry transaction if possible) [keys=" + keys + ", node=" + nodeId + ']', nested);
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
        @SuppressWarnings({"ThrowableInstanceNeverThrown"})
        @Override public void onTimeout() {
            if (log.isDebugEnabled())
                log.debug("Timed out waiting for lock response: " + this);

            timedOut = true;

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
        private Collection<K> keys;

        /** Mappings to proceed. */
        @GridToStringExclude
        private ConcurrentLinkedDeque8<GridNearLockMapping<K, V>> mappings;

        /** */
        private AtomicBoolean rcvRes = new AtomicBoolean(false);

        /**
         * Empty constructor required for {@link Externalizable}.
         */
        public MiniFuture() {
            // No-op.
        }

        /**
         * @param node Node.
         * @param keys Keys.
         * @param mappings Mappings to proceed.
         */
        MiniFuture(ClusterNode node, Collection<K> keys,
            ConcurrentLinkedDeque8<GridNearLockMapping<K, V>> mappings) {
            super(cctx.kernalContext());

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
        public Collection<K> keys() {
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
                U.warn(log, "Received error after another result has been processed [fut=" + GridNearLockFuture.this +
                    ", mini=" + this + ']', e);
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
                GridNearLockFuture.this.onDone(newTopologyException(e, node.id()));

                onDone(true);
            }
        }

        /**
         * @param res Result callback.
         */
        void onResult(GridNearLockResponse<K, V> res) {
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

                int i = 0;

                long topVer = topSnapshot.get().topologyVersion();

                for (K k : keys) {
                    while (true) {
                        GridNearCacheEntry<K, V> entry = cctx.near().entryExx(k, topVer);

                        try {
                            if (res.dhtVersion(i) == null) {
                                onDone(new IgniteCheckedException("Failed to receive DHT version from remote node " +
                                    "(will fail the lock): " + res));

                                return;
                            }

                            GridTuple3<GridCacheVersion, V, byte[]> oldValTup = valMap.get(entry.key());

                            V oldVal = entry.rawGet();
                            boolean hasOldVal = false;
                            V newVal = res.value(i);
                            byte[] newBytes = res.valueBytes(i);

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
                                    if (oldValTup.get1().equals(dhtVer)) {
                                        newVal = oldValTup.get2();

                                        newBytes = oldValTup.get3();
                                    }

                                    oldVal = oldValTup.get2();
                                }
                            }

                            // Lock is held at this point, so we can set the
                            // returned value if any.
                            entry.resetFromPrimary(newVal, newBytes, lockVer, dhtVer, node.id());

                            if (inTx() && implicitTx() && tx.onePhaseCommit()) {
                                boolean pass = res.filterResult(i);

                                tx.entry(cctx.txKey(k)).filters(pass ? CU.<K, V>empty() : CU.<K, V>alwaysFalse());
                            }

                            entry.readyNearLock(lockVer, mappedVer, res.committedVersions(), res.rolledbackVersions(),
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
                                        newVal != null || newBytes != null,
                                        oldVal,
                                        hasOldVal,
                                        CU.subjectId(tx, cctx.shared()),
                                        null,
                                        inTx() ? tx.resolveTaskName() : null);

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

                            // Replace old entry with new one.
                            entries.set(i, (GridDistributedCacheEntry<K, V>)cctx.cache().entryEx(entry.key()));
                        }
                        catch (IgniteCheckedException e) {
                            onDone(e);

                            return;
                        }
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

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(MiniFuture.class, this, "node", node.id(), "super", super.toString());
        }
    }
}
