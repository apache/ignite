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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.IgniteClientDisconnectedCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.NodeStoppingException;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.managers.eventstorage.GridLocalEventListener;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.distributed.GridCacheMappedVersion;
import org.apache.ignite.internal.processors.cache.distributed.GridDistributedCacheEntry;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxFinishFuture;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxFinishFuture;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxKey;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.GridBoundedConcurrentLinkedHashSet;
import org.apache.ignite.internal.util.GridConcurrentFactory;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.internal.util.future.GridCompoundFuture;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.lang.GridPlainRunnable;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.P1;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.util.deque.FastSizeDeque;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_MAX_NESTED_LISTENER_CALLS;
import static org.apache.ignite.IgniteSystemProperties.getInteger;
import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;
import static org.apache.ignite.internal.util.GridConcurrentFactory.newMap;
import static org.jsr166.ConcurrentLinkedHashMap.QueuePolicy.PER_SEGMENT_Q;

/**
 * Manages lock order within a thread.
 */
public class GridCacheMvccManager extends GridCacheSharedManagerAdapter {
    /** Maxim number of removed locks. */
    private static final int MAX_REMOVED_LOCKS = 10240;

    /** Maxim number of atomic IDs for thread. Must be power of two! */
    protected static final int THREAD_RESERVE_SIZE = 0x4000;

    /** */
    private static final int MAX_NESTED_LSNR_CALLS = getInteger(IGNITE_MAX_NESTED_LISTENER_CALLS, 5);

    /** Pending locks per thread. */
    private final ThreadLocal<Deque<GridCacheMvccCandidate>> pending = new ThreadLocal<>();

    /** Pending near local locks and topology version per thread. */
    private ConcurrentMap<Long, GridCacheExplicitLockSpan> pendingExplicit;

    /** Set of removed lock versions. */
    private GridBoundedConcurrentLinkedHashSet<GridCacheVersion> rmvLocks =
        new GridBoundedConcurrentLinkedHashSet<>(MAX_REMOVED_LOCKS, MAX_REMOVED_LOCKS, 0.75f, 16, PER_SEGMENT_Q);

    /** Locked keys. */
    @GridToStringExclude
    private final ConcurrentMap<IgniteTxKey, GridDistributedCacheEntry> locked = newMap();

    /** Near locked keys. Need separate map because mvcc manager is shared between caches. */
    @GridToStringExclude
    private final ConcurrentMap<IgniteTxKey, GridDistributedCacheEntry> nearLocked = newMap();

    /** Active futures mapped by version ID. */
    @GridToStringExclude
    private final ConcurrentMap<GridCacheVersion, Collection<GridCacheVersionedFuture<?>>> verFuts = newMap();

    /** Pending atomic futures. */
    private final ConcurrentHashMap<Long, GridCacheAtomicFuture<?>> atomicFuts = new ConcurrentHashMap<>();

    /** Pending data streamer futures. */
    private final GridConcurrentHashSet<DataStreamerFuture> dataStreamerFuts = new GridConcurrentHashSet<>();

    /** */
    private final ConcurrentMap<IgniteUuid, GridCacheFuture<?>> futs = new ConcurrentHashMap<>();

    /** Near to DHT version mapping. */
    private final ConcurrentMap<GridCacheVersion, GridCacheVersion> near2dht = newMap();

    /** Finish futures. */
    private final FastSizeDeque<FinishLockFuture> finishFuts = new FastSizeDeque<>(new ConcurrentLinkedDeque<>());

    /** Nested listener calls. */
    private final ThreadLocal<Integer> nestedLsnrCalls = new ThreadLocal<Integer>() {
        @Override protected Integer initialValue() {
            return 0;
        }
    };

    /** Logger. */
    @SuppressWarnings( {"FieldAccessedSynchronizedAndUnsynchronized"})
    private IgniteLogger exchLog;

    /** */
    private volatile boolean stopping;

    /** Global atomic id counter. */
    protected final AtomicLong globalAtomicCnt = new AtomicLong();

    /** Per thread atomic id counter. */
    private final ThreadLocal<LongWrapper> threadAtomicCnt = new ThreadLocal<LongWrapper>() {
        @Override protected LongWrapper initialValue() {
            return new LongWrapper(globalAtomicCnt.getAndAdd(THREAD_RESERVE_SIZE));
        }
    };

    /** Lock callback. */
    @GridToStringExclude
    private final GridCacheLockCallback cb = new GridCacheLockCallback() {
        /** {@inheritDoc} */
        @SuppressWarnings({"unchecked"})
        @Override public void onOwnerChanged(final GridCacheEntryEx entry, final GridCacheMvccCandidate owner) {
            int nested = nestedLsnrCalls.get();

            if (nested < MAX_NESTED_LSNR_CALLS) {
                nestedLsnrCalls.set(nested + 1);

                try {
                    notifyOwnerChanged(entry, owner);
                }
                finally {
                    nestedLsnrCalls.set(nested);
                }
            }
            else {
                cctx.kernalContext().closure().runLocalSafe(new GridPlainRunnable() {
                    @Override public void run() {
                        notifyOwnerChanged(entry, owner);
                    }
                }, true);
            }
        }

        /** {@inheritDoc} */
        @Override public void onLocked(GridDistributedCacheEntry entry) {
            if (entry.isNear())
                nearLocked.put(entry.txKey(), entry);
            else
                locked.put(entry.txKey(), entry);
        }

        /** {@inheritDoc} */
        @Override public void onFreed(GridDistributedCacheEntry entry) {
            if (entry.isNear())
                nearLocked.remove(entry.txKey());
            else
                locked.remove(entry.txKey());
        }
    };

    /**
     * @param entry Entry to notify callback for.
     * @param owner Current lock owner.
     */
    private void notifyOwnerChanged(final GridCacheEntryEx entry, final GridCacheMvccCandidate owner) {
        assert entry != null;

        if (log.isDebugEnabled())
            log.debug("Received owner changed callback [" + entry.key() + ", owner=" + owner + ']');

        if (owner != null && (owner.local() || owner.nearLocal())) {
            Collection<GridCacheVersionedFuture<?>> futCol = verFuts.get(owner.version());

            if (futCol != null) {
                ArrayList<GridCacheVersionedFuture<?>> futColCp;

                synchronized (futCol) {
                    futColCp = new ArrayList<>(futCol.size());

                    futColCp.addAll(futCol);
                }

                // Must invoke onOwnerChanged outside of synchronization block.
                for (GridCacheVersionedFuture<?> fut : futColCp) {
                    if (!fut.isDone()) {
                        final GridCacheVersionedFuture<Boolean> mvccFut = (GridCacheVersionedFuture<Boolean>)fut;

                        // Since this method is called outside of entry synchronization,
                        // we can safely invoke any method on the future.
                        // Also note that we don't remove future here if it is done.
                        // The removal is initiated from within future itself.
                        if (mvccFut.onOwnerChanged(entry, owner))
                            return;
                    }
                }
            }
        }

        if (log.isDebugEnabled())
            log.debug("Lock future not found for owner change callback (will try transaction futures) [owner=" +
                owner + ", entry=" + entry + ']');

        // If no future was found, delegate to transaction manager.
        if (cctx.tm().onOwnerChanged(entry, owner)) {
            if (log.isDebugEnabled())
                log.debug("Found transaction for changed owner: " + owner);
        }
        else if (log.isDebugEnabled())
            log.debug("Failed to find transaction for changed owner: " + owner);

        if (!finishFuts.isEmptyx()) {
            for (FinishLockFuture f : finishFuts)
                f.recheck(entry);
        }
    }

    /** Discovery listener. */
    @GridToStringExclude private final GridLocalEventListener discoLsnr = new GridLocalEventListener() {
        @Override public void onEvent(Event evt) {
            assert evt instanceof DiscoveryEvent;
            assert evt.type() == EVT_NODE_FAILED || evt.type() == EVT_NODE_LEFT;

            DiscoveryEvent discoEvt = (DiscoveryEvent)evt;

            if (log.isDebugEnabled())
                log.debug("Processing node left [nodeId=" + discoEvt.eventNode().id() + "]");

            for (GridCacheFuture<?> fut : activeFutures())
                fut.onNodeLeft(discoEvt.eventNode().id());

            for (GridCacheAtomicFuture<?> cacheFut : atomicFuts.values())
                cacheFut.onNodeLeft(discoEvt.eventNode().id());
        }
    };

    /** {@inheritDoc} */
    @Override protected void start0() throws IgniteCheckedException {
        exchLog = cctx.logger(getClass().getName() + ".exchange");

        pendingExplicit = GridConcurrentFactory.newMap();

        cctx.gridEvents().addLocalEventListener(discoLsnr, EVT_NODE_FAILED, EVT_NODE_LEFT);
    }

    /** {@inheritDoc} */
    @Override public void onKernalStop0(boolean cancel) {
        cctx.gridEvents().removeLocalEventListener(discoLsnr);
    }

    /**
     * @return MVCC callback.
     */
    public GridCacheLockCallback callback() {
        return cb;
    }

    /**
     * @return Collection of pending explicit locks.
     */
    public Collection<GridCacheExplicitLockSpan> activeExplicitLocks() {
        return pendingExplicit.values();
    }

    /**
     * @return Collection of active futures.
     */
    public Collection<GridCacheFuture<?>> activeFutures() {
        ArrayList<GridCacheFuture<?>> col = new ArrayList<>();

        for (Collection<GridCacheVersionedFuture<?>> futs : verFuts.values()) {
            synchronized (futs) {
                col.addAll(futs);
            }
        }

        col.addAll(futs.values());

        return col;
    }

    /**
     * Creates a future that will wait for finishing all remote transactions (primary -> backup)
     * with topology version less or equal to {@code topVer}.
     *
     * @param topVer Topology version.
     * @return Compound future of all {@link GridDhtTxFinishFuture} futures.
     */
    public IgniteInternalFuture<?> finishRemoteTxs(AffinityTopologyVersion topVer) {
        GridCompoundFuture<?, ?> res = new CacheObjectsReleaseFuture<>("RemoteTx", topVer);

        for (GridCacheFuture<?> fut : futs.values()) {
            if (fut instanceof GridDhtTxFinishFuture) {
                GridDhtTxFinishFuture finishTxFuture = (GridDhtTxFinishFuture) fut;

                if (cctx.tm().needWaitTransaction(finishTxFuture.tx(), topVer))
                    res.add(ignoreErrors(finishTxFuture));
            }
        }

        res.markInitialized();

        return res;
    }

    /**
     * Future wrapper which ignores any underlying future errors.
     *
     * @param f Underlying future.
     * @return Future wrapper which ignore any underlying future errors.
     */
    private IgniteInternalFuture ignoreErrors(IgniteInternalFuture<?> f) {
        GridFutureAdapter<?> wrapper = new GridFutureAdapter();
        f.listen(future -> wrapper.onDone());
        return wrapper;
    }

    /**
     * @param leftNodeId Left node ID.
     * @param topVer Topology version.
     */
    public void removeExplicitNodeLocks(UUID leftNodeId, AffinityTopologyVersion topVer) {
        for (GridDistributedCacheEntry entry : locked()) {
            try {
                entry.removeExplicitNodeLocks(leftNodeId);

                entry.touch(topVer);
            }
            catch (GridCacheEntryRemovedException ignore) {
                if (log.isDebugEnabled())
                    log.debug("Attempted to remove node locks from removed entry in mvcc manager " +
                        "disco callback (will ignore): " + entry);
            }
        }
    }

    /**
     * @param from From version.
     * @param to To version.
     */
    public void mapVersion(GridCacheVersion from, GridCacheVersion to) {
        assert from != null;
        assert to != null;

        GridCacheVersion old = near2dht.put(from, to);

        assert old == null || old == to || old.equals(to);

        if (log.isDebugEnabled())
            log.debug("Added version mapping [from=" + from + ", to=" + to + ']');
    }

    /**
     * @param from Near version.
     * @return DHT version.
     */
    public GridCacheVersion mappedVersion(GridCacheVersion from) {
        assert from != null;

        GridCacheVersion to = near2dht.get(from);

        if (log.isDebugEnabled())
            log.debug("Retrieved mapped version [from=" + from + ", to=" + to + ']');

        return to;
    }

    /**
     * Cancels all client futures.
     */
    public void onStop() {
        stopping = true;

        cancelClientFutures(stopError());
    }

    /** {@inheritDoc} */
    @Override public void onDisconnected(IgniteFuture reconnectFut) {
        IgniteClientDisconnectedCheckedException err = disconnectedError(reconnectFut);

        cancelClientFutures(err);
    }

    /**
     * @param err Error.
     */
    private void cancelClientFutures(IgniteCheckedException err) {
        for (GridCacheFuture<?> fut : activeFutures())
            ((GridFutureAdapter)fut).onDone(err);

        for (GridCacheAtomicFuture<?> future : atomicFuts.values())
            ((GridFutureAdapter)future).onDone(err);
    }

    /**
     * @param reconnectFut Reconnect future.
     * @return Client disconnected exception.
     */
    private IgniteClientDisconnectedCheckedException disconnectedError(@Nullable IgniteFuture<?> reconnectFut) {
        if (reconnectFut == null)
            reconnectFut = cctx.kernalContext().cluster().clientReconnectFuture();

        return new IgniteClientDisconnectedCheckedException(reconnectFut,
            "Operation has been cancelled (client node disconnected).");
    }

    /**
     * @return Node stop exception.
     */
    private IgniteCheckedException stopError() {
        return new NodeStoppingException("Operation has been cancelled (node is stopping).");
    }

    /**
     * @param from From version.
     * @return To version.
     */
    public GridCacheVersion unmapVersion(GridCacheVersion from) {
        assert from != null;

        GridCacheVersion to = near2dht.remove(from);

        if (log.isDebugEnabled())
            log.debug("Removed mapped version [from=" + from + ", to=" + to + ']');

        return to;
    }

    /**
     * @param futId Future ID.
     * @param fut Future.
     * @return {@code False} if future was forcibly completed with error.
     */
    public boolean addAtomicFuture(long futId, GridCacheAtomicFuture<?> fut) {
        IgniteInternalFuture<?> old = atomicFuts.put(futId, fut);

        assert old == null : "Old future is not null [futId=" + futId + ", fut=" + fut + ", old=" + old + ']';

        return onFutureAdded(fut);
    }

    /**
     * @return Collection of pending atomic futures.
     */
    public Collection<GridCacheAtomicFuture<?>> atomicFutures() {
        return atomicFuts.values();
    }

    /**
     * @return Number of pending atomic futures.
     */
    public int atomicFuturesCount() {
        return atomicFuts.size();
    }

    /**
     * @return Collection of pending data streamer futures.
     */
    public Collection<DataStreamerFuture> dataStreamerFutures() {
        return dataStreamerFuts;
    }

    /**
     * Gets future by given future ID.
     *
     * @param futId Future ID.
     * @return Future.
     */
    @Nullable public IgniteInternalFuture<?> atomicFuture(long futId) {
        return atomicFuts.get(futId);
    }

    /**
     * @param futId Future ID.
     * @return Removed future.
     */
    @Nullable public IgniteInternalFuture<?> removeAtomicFuture(long futId) {
        return atomicFuts.remove(futId);
    }

    /**
     * @param fut Future.
     * @param futId Future ID.
     * @return {@code True} if added.
     */
    public boolean addFuture(final GridCacheFuture<?> fut, final IgniteUuid futId) {
        GridCacheFuture<?> old = futs.put(futId, fut);

        assert old == null : old;

        return onFutureAdded(fut);
    }

    /**
     * @param topVer Topology version.
     * @return Future.
     */
    public GridFutureAdapter addDataStreamerFuture(AffinityTopologyVersion topVer) {
        final DataStreamerFuture fut = new DataStreamerFuture(topVer);

        boolean add = dataStreamerFuts.add(fut);

        assert add;

        return fut;
    }

    /**

    /**
     * Adds future.
     *
     * @param fut Future.
     * @return {@code True} if added.
     */
    @SuppressWarnings({"SynchronizationOnLocalVariableOrMethodParameter"})
    public boolean addFuture(final GridCacheVersionedFuture<?> fut) {
        if (fut.isDone()) {
            fut.markNotTrackable();

            return true;
        }

        if (!fut.trackable())
            return true;

        while (true) {
            Collection<GridCacheVersionedFuture<?>> old = verFuts.get(fut.version());

            if (old == null) {
                Collection<GridCacheVersionedFuture<?>> col = new HashSet<GridCacheVersionedFuture<?>>(U.capacity(1), 0.75f) {
                    {
                        // Make sure that we add future to queue before
                        // adding queue to the map of futures.
                        add(fut);
                    }

                    @Override public int hashCode() {
                        return System.identityHashCode(this);
                    }

                    @Override public boolean equals(Object obj) {
                        return obj == this;
                    }
                };

                old = verFuts.putIfAbsent(fut.version(), col);
            }

            if (old != null) {
                boolean empty, dup = false;

                synchronized (old) {
                    empty = old.isEmpty();

                    if (!empty)
                        dup = !old.add(fut);
                }

                // Future is being removed, so we force-remove here and try again.
                if (empty) {
                    if (verFuts.remove(fut.version(), old)) {
                        if (log.isDebugEnabled())
                            log.debug("Removed future list from futures map for lock version: " + fut.version());
                    }

                    continue;
                }

                if (dup) {
                    if (log.isDebugEnabled())
                        log.debug("Found duplicate future in futures map (will not add): " + fut);

                    return false;
                }
            }

            // Handle version mappings.
            if (fut instanceof GridCacheMappedVersion) {
                GridCacheVersion from = ((GridCacheMappedVersion)fut).mappedVersion();

                if (from != null)
                    mapVersion(from, fut.version());
            }

            if (log.isDebugEnabled())
                log.debug("Added future to future map: " + fut);

            break;
        }

        // Just in case if future was completed before it was added.
        if (fut.isDone())
            removeVersionedFuture(fut);
        else
            onFutureAdded(fut);

        return true;
    }

    /**
     * @param fut Future.
     * @return {@code False} if future was forcibly completed with error.
     */
    private boolean onFutureAdded(IgniteInternalFuture<?> fut) {
        if (stopping) {
            ((GridFutureAdapter)fut).onDone(stopError());

            return false;
        }
        else if (cctx.kernalContext().clientDisconnected()) {
            ((GridFutureAdapter)fut).onDone(disconnectedError(null));

            return false;
        }

        return true;
    }

    /**
     * @param futId Future ID.
     */
    public void removeFuture(IgniteUuid futId) {
        futs.remove(futId);
    }

    /**
     * @param fut Future to remove.
     * @return {@code True} if removed.
     */
    @SuppressWarnings({"SynchronizationOnLocalVariableOrMethodParameter"})
    public boolean removeVersionedFuture(GridCacheVersionedFuture<?> fut) {
        if (!fut.trackable())
            return true;

        Collection<GridCacheVersionedFuture<?>> cur = verFuts.get(fut.version());

        if (cur == null)
            return false;

        boolean rmv, empty;

        synchronized (cur) {
            rmv = cur.remove(fut);

            empty = cur.isEmpty();
        }

        if (rmv) {
            if (log.isDebugEnabled())
                log.debug("Removed future from future map: " + fut);
        }
        else if (log.isDebugEnabled())
            log.debug("Attempted to remove a non-registered future (has it been already removed?): " + fut);

        if (empty && verFuts.remove(fut.version(), cur))
            if (log.isDebugEnabled())
                log.debug("Removed future list from futures map for lock version: " + fut.version());

        return rmv;
    }

    /**
     * Gets future for given future ID and lock ID.
     *
     * @param ver Lock ID.
     * @param futId Future ID.
     * @return Future.
     */
    @SuppressWarnings({"unchecked"})
    @Nullable public GridCacheVersionedFuture<?> versionedFuture(GridCacheVersion ver, IgniteUuid futId) {
        Collection<GridCacheVersionedFuture<?>> futs = this.verFuts.get(ver);

        if (futs != null) {
            synchronized (futs) {
                for (GridCacheVersionedFuture<?> fut : futs) {
                    if (fut.futureId().equals(futId)) {
                        if (log.isDebugEnabled())
                            log.debug("Found future in futures map: " + fut);

                        return fut;
                    }
                }
            }
        }

        if (log.isDebugEnabled())
            log.debug("Failed to find future in futures map [ver=" + ver + ", futId=" + futId + ']');

        return null;
    }

    /**
     * Gets futures for given lock ID.
     *
     * @param ver Lock ID.
     * @return Futures.
     */
    @SuppressWarnings({"unchecked"})
    @Nullable public Collection<GridCacheVersionedFuture<?>> futuresForVersion(GridCacheVersion ver) {
        Collection<GridCacheVersionedFuture<?>> futs = this.verFuts.get(ver);

        if (futs != null) {
            synchronized (futs) {
                return new ArrayList<>(futs);
            }
        }

        return null;
    }

    /**
     * @param futId Future ID.
     * @return Found future.
     */
    @Nullable public GridCacheFuture future(IgniteUuid futId) {
        return futs.get(futId);
    }

    /**
     * @param cacheCtx Cache context.
     * @param ver Lock version to check.
     * @return {@code True} if lock had been removed.
     */
    public boolean isRemoved(GridCacheContext cacheCtx, GridCacheVersion ver) {
        return !cacheCtx.isNear() && !cacheCtx.isLocal() && ver != null && rmvLocks.contains(ver);
    }

    /**
     * @param cacheCtx Cache context.
     * @param ver Obsolete entry version.
     * @return {@code True} if added.
     */
    public boolean addRemoved(GridCacheContext cacheCtx, GridCacheVersion ver) {
        if (cacheCtx.isNear() || cacheCtx.isLocal())
            return true;

        boolean ret = rmvLocks.add(ver);

        if (log.isDebugEnabled())
            log.debug("Added removed lock version: " + ver);

        return ret;
    }

    /**
     * @return Collection of all locked entries.
     */
    private Collection<GridDistributedCacheEntry> locked() {
        return F.concat(false, locked.values(), nearLocked.values());
    }

    /**
     * @return Locked keys.
     */
    public Collection<IgniteTxKey> lockedKeys() {
        return locked.keySet();
    }

    /**
     * @return Locked near keys.
     */
    public Collection<IgniteTxKey> nearLockedKeys() {
        return nearLocked.keySet();
    }

    /**
     * This method has poor performance, so use with care. It is currently only used by {@code DGC}.
     *
     * @return Remote candidates.
     */
    public Collection<GridCacheMvccCandidate> remoteCandidates() {
        Collection<GridCacheMvccCandidate> rmtCands = new ArrayList<>();

        for (GridDistributedCacheEntry entry : locked())
            rmtCands.addAll(entry.remoteMvccSnapshot());

        return rmtCands;
    }

    /**
     * This method has poor performance, so use with care. It is currently only used by {@code DGC}.
     *
     * @return Local candidates.
     */
    public Collection<GridCacheMvccCandidate> localCandidates() {
        Collection<GridCacheMvccCandidate> locCands = new ArrayList<>();

        for (GridDistributedCacheEntry entry : locked()) {
            try {
                locCands.addAll(entry.localCandidates());
            }
            catch (GridCacheEntryRemovedException ignore) {
                // No-op.
            }
        }

        return locCands;
    }

    /**
     * @param cacheCtx Cache context.
     * @param cand Cache lock candidate to add.
     * @return {@code True} if added as a result of this operation,
     *      {@code false} if was previously added.
     */
    public boolean addNext(GridCacheContext cacheCtx, GridCacheMvccCandidate cand) {
        assert cand != null;
        assert !cand.reentry() : "Lock reentries should not be linked: " + cand;

        // Don't order near candidates by thread as they will be ordered on
        // DHT node. Also, if candidate is implicit, no point to order him.
        if (cacheCtx.isNear() || cand.singleImplicit())
            return true;

        Deque<GridCacheMvccCandidate> queue = pending.get();

        if (queue == null)
            pending.set(queue = new ArrayDeque<>());

        GridCacheMvccCandidate prev = null;

        if (!queue.isEmpty())
            prev = queue.getLast();

        queue.add(cand);

        if (prev != null) {
            prev.next(cand);

            cand.previous(prev);
        }

        if (log.isDebugEnabled())
            log.debug("Linked new candidate: " + cand);

        return true;
    }

    /**
     * Reset MVCC context.
     */
    public void contextReset() {
        pending.set(null);
    }

    /**
     * Adds candidate to the list of near local candidates.
     *
     * @param threadId Thread ID.
     * @param cand Candidate to add.
     * @param topVer Topology version.
     */
    public void addExplicitLock(long threadId, GridCacheMvccCandidate cand, AffinityTopologyVersion topVer) {
        while (true) {
            GridCacheExplicitLockSpan span = pendingExplicit.get(cand.threadId());

            if (span == null) {
                span = new GridCacheExplicitLockSpan(topVer, cand);

                GridCacheExplicitLockSpan old = pendingExplicit.putIfAbsent(threadId, span);

                if (old == null)
                    break;
                else
                    span = old;
            }

            // Either span was not empty, or concurrent put did not succeed.
            if (span.addCandidate(topVer, cand))
                break;
            else
                pendingExplicit.remove(threadId, span);
        }
    }

    /**
     * Removes candidate from the list of near local candidates.
     *
     * @param cand Candidate to remove.
     */
    public void removeExplicitLock(GridCacheMvccCandidate cand) {
        GridCacheExplicitLockSpan span = pendingExplicit.get(cand.threadId());

        if (span == null)
            return;

        if (span.removeCandidate(cand))
            pendingExplicit.remove(cand.threadId(), span);
    }

    /**
     * Checks if given key is locked by thread with given id or any thread.
     *
     * @param key Key to check.
     * @param threadId Thread id. If -1, all threads will be checked.
     * @return {@code True} if locked by any or given thread (depending on {@code threadId} value).
     */
    public boolean isLockedByThread(IgniteTxKey key, long threadId) {
        if (threadId < 0) {
            for (GridCacheExplicitLockSpan span : pendingExplicit.values()) {
                GridCacheMvccCandidate cand = span.candidate(key, null);

                if (cand != null && cand.owner())
                    return true;
            }
        }
        else {
            GridCacheExplicitLockSpan span = pendingExplicit.get(threadId);

            if (span != null) {
                GridCacheMvccCandidate cand = span.candidate(key, null);

                return cand != null && cand.owner();
            }
        }

        return false;
    }

    /**
     * Marks candidates for given thread and given key as owned.
     *
     * @param key Key.
     * @param threadId Thread id.
     */
    public void markExplicitOwner(IgniteTxKey key, long threadId) {
        assert threadId > 0;

        GridCacheExplicitLockSpan span = pendingExplicit.get(threadId);

        if (span != null)
            span.markOwned(key);
    }

    /**
     * Removes explicit lock for given thread id, key and optional version.
     *
     * @param threadId Thread id.
     * @param key Key.
     * @param ver Optional version.
     * @return Candidate.
     */
    public GridCacheMvccCandidate removeExplicitLock(long threadId,
        IgniteTxKey key,
        @Nullable GridCacheVersion ver)
    {
        assert threadId > 0;

        GridCacheExplicitLockSpan span = pendingExplicit.get(threadId);

        if (span == null)
            return null;

        GridCacheMvccCandidate cand = span.removeCandidate(key, ver);

        if (cand != null && span.isEmpty())
            pendingExplicit.remove(cand.threadId(), span);

        return cand;
    }

    /**
     * Gets last added explicit lock candidate by thread id and key.
     *
     * @param threadId Thread id.
     * @param key Key to look up.
     * @return Last added explicit lock candidate for given thread id and key or {@code null} if
     *      no such candidate.
     */
    @Nullable public GridCacheMvccCandidate explicitLock(long threadId, IgniteTxKey key) {
        if (threadId < 0)
            return explicitLock(key, null);
        else {
            GridCacheExplicitLockSpan span = pendingExplicit.get(threadId);

            return span == null ? null : span.candidate(key, null);
        }
    }

    /**
     * Gets explicit lock candidate added by any thread by given key and lock version.
     *
     * @param key Key.
     * @param ver Version.
     * @return Lock candidate that satisfies given criteria or {@code null} if no such candidate.
     */
    @Nullable public GridCacheMvccCandidate explicitLock(IgniteTxKey key, @Nullable GridCacheVersion ver) {
        for (GridCacheExplicitLockSpan span : pendingExplicit.values()) {
            GridCacheMvccCandidate cand = span.candidate(key, ver);

            if (cand != null)
                return cand;
        }

        return null;
    }

    /**
     * @param threadId Thread ID.
     * @return Topology snapshot for last acquired and not released lock.
     */
    @Nullable public AffinityTopologyVersion lastExplicitLockTopologyVersion(long threadId) {
        GridCacheExplicitLockSpan span = pendingExplicit.get(threadId);

        return span != null ? span.topologyVersion() : null;
    }

    /** {@inheritDoc} */
    @Override public void printMemoryStats() {
        X.println(">>> ");
        X.println(">>> Mvcc manager memory stats [igniteInstanceName=" + cctx.igniteInstanceName() + ']');
        X.println(">>>   rmvLocksSize: " + rmvLocks.sizex());
        X.println(">>>   lockedSize: " + locked.size());
        X.println(">>>   futsSize: " + (verFuts.size() + futs.size()));
        X.println(">>>   near2dhtSize: " + near2dht.size());
        X.println(">>>   finishFutsSize: " + finishFuts.sizex());
    }

    /**
     * @param topVer Topology version.
     * @return Future that signals when all locks for given partitions are released.
     */
    @SuppressWarnings({"unchecked"})
    public IgniteInternalFuture<?> finishLocks(AffinityTopologyVersion topVer) {
        assert topVer.compareTo(AffinityTopologyVersion.ZERO) > 0;
        return finishLocks(null, topVer);
    }

    /**
     * @param topVer Topology version.
     * @return Locked keys.
     */
    public Map<IgniteTxKey, Collection<GridCacheMvccCandidate>> unfinishedLocks(AffinityTopologyVersion topVer) {
        Map<IgniteTxKey, Collection<GridCacheMvccCandidate>> cands = new HashMap<>();

        if (!finishFuts.isEmptyx()) {
            for (FinishLockFuture fut : finishFuts) {
                if (fut.topologyVersion().equals(topVer))
                    cands.putAll(fut.pendingLocks());
            }
        }

        return cands;
    }

    /**
     * Creates a future that will wait for all explicit locks acquired on given topology
     * version to be released.
     *
     * @param topVer Topology version to wait for.
     * @return Explicit locks release future.
     */
    public IgniteInternalFuture<?> finishExplicitLocks(AffinityTopologyVersion topVer) {
        GridCompoundFuture<Object, Object> res = new CacheObjectsReleaseFuture<>("ExplicitLock", topVer);

        for (GridCacheExplicitLockSpan span : pendingExplicit.values()) {
            AffinityTopologyVersion snapshot = span.topologyVersion();

            if (snapshot != null && snapshot.compareTo(topVer) < 0)
                res.add(span.releaseFuture());
        }

        res.markInitialized();

        return res;
    }

    /**
     * @param topVer Topology version to finish.
     *
     * @return Finish update future.
     */
    @SuppressWarnings("unchecked")
    public IgniteInternalFuture<?> finishAtomicUpdates(AffinityTopologyVersion topVer) {
        GridCompoundFuture<Object, Object> res = new FinishAtomicUpdateFuture("AtomicUpdate", topVer);

        for (GridCacheAtomicFuture<?> fut : atomicFuts.values()) {
            IgniteInternalFuture<Void> complete = fut.completeFuture(topVer);

            if (complete != null)
                res.add((IgniteInternalFuture)complete);
        }

        res.markInitialized();

        return res;
    }

    /**
     *
     * @return Finish update future.
     */
    @SuppressWarnings("unchecked")
    public IgniteInternalFuture<?> finishDataStreamerUpdates(AffinityTopologyVersion topVer) {
        GridCompoundFuture<Void, Object> res = new CacheObjectsReleaseFuture<>("DataStreamer", topVer);

        for (DataStreamerFuture fut : dataStreamerFuts) {
            if (fut.topVer.compareTo(topVer) < 0)
                res.add(fut);
        }

        res.markInitialized();

        return res;
    }

    /**
     * @param keys Key for which locks should be released.
     * @param cacheId Cache ID.
     * @param topVer Topology version.
     * @return Future that signals when all locks for given keys are released.
     */
    @SuppressWarnings("unchecked")
    public IgniteInternalFuture<?> finishKeys(Collection<KeyCacheObject> keys,
        final int cacheId,
        AffinityTopologyVersion topVer) {
        if (!(keys instanceof Set))
            keys = new HashSet<>(keys);

        final Collection<KeyCacheObject> keys0 = keys;

        return finishLocks(new P1<GridDistributedCacheEntry>() {
            @Override public boolean apply(GridDistributedCacheEntry e) {
                return e.context().cacheId() == cacheId && keys0.contains(e.key());
            }
        }, topVer);
    }

    /**
     * @param filter Entry filter.
     * @param topVer Topology version.
     * @return Future that signals when all locks for given partitions will be released.
     */
    private IgniteInternalFuture<?> finishLocks(
        @Nullable final IgnitePredicate<GridDistributedCacheEntry> filter,
        AffinityTopologyVersion topVer
    ) {
        assert topVer.topologyVersion() != 0;

        if (topVer.equals(AffinityTopologyVersion.NONE))
            return new GridFinishedFuture();

        final FinishLockFuture finishFut =
            new FinishLockFuture(filter == null ? locked() : F.view(locked(), filter), topVer);

        finishFuts.add(finishFut);

        finishFut.listen(new CI1<IgniteInternalFuture<?>>() {
            @Override public void apply(IgniteInternalFuture<?> e) {
                finishFuts.remove(finishFut);
            }
        });

        finishFut.recheck();

        return finishFut;
    }

    /**
     *
     */
    public void recheckPendingLocks() {
        if (exchLog.isDebugEnabled())
            exchLog.debug("Rechecking pending locks for completion.");

        if (!finishFuts.isEmptyx()) {
            for (FinishLockFuture fut : finishFuts)
                fut.recheck();
        }
    }

    /**
     * @return Next future ID for atomic futures.
     */
    public long nextAtomicId() {
        LongWrapper cnt = threadAtomicCnt.get();

        long res = cnt.getAndIncrement();

        if ((cnt.get() & (THREAD_RESERVE_SIZE - 1)) == 0)
            cnt.set(globalAtomicCnt.getAndAdd(THREAD_RESERVE_SIZE));

        return res;
    }

    /**
     *
     */
    private class FinishLockFuture extends GridFutureAdapter<Object> {
        /** */
        private static final long serialVersionUID = 0L;

        /** Topology version. Instance field for toString method only. */
        @GridToStringInclude
        private final AffinityTopologyVersion topVer;

        /** */
        @GridToStringInclude
        private final Map<IgniteTxKey, Collection<GridCacheMvccCandidate>> pendingLocks =
            new ConcurrentHashMap<>();

        /**
         * @param topVer Topology version.
         * @param entries Entries.
         */
        FinishLockFuture(Iterable<GridDistributedCacheEntry> entries, AffinityTopologyVersion topVer) {
            assert topVer.compareTo(AffinityTopologyVersion.ZERO) > 0;

            this.topVer = topVer;

            for (GridCacheEntryEx entry : entries) {
                // Either local or near local candidates.
                try {
                    Collection<GridCacheMvccCandidate> locs = entry.localCandidates();

                    if (!F.isEmpty(locs)) {
                        Collection<GridCacheMvccCandidate> cands = new ConcurrentLinkedQueue<>();

                        cands.addAll(F.view(locs, versionFilter()));

                        if (!F.isEmpty(cands))
                            pendingLocks.put(entry.txKey(), cands);
                    }
                }
                catch (GridCacheEntryRemovedException ignored) {
                    if (exchLog.isDebugEnabled())
                        exchLog.debug("Got removed entry when adding it to finish lock future (will ignore): " + entry);
                }
            }

            if (exchLog.isDebugEnabled())
                exchLog.debug("Pending lock set [topVer=" + topVer + ", locks=" + pendingLocks + ']');
        }

        /**
         * @return Topology version.
         */
        AffinityTopologyVersion topologyVersion() {
            return topVer;
        }

        /**
         * @return Pending locks.
         */
        Map<IgniteTxKey, Collection<GridCacheMvccCandidate>> pendingLocks() {
            return pendingLocks;
        }

        /**
         * @return Filter.
         */
        private IgnitePredicate<GridCacheMvccCandidate> versionFilter() {
            assert topVer.topologyVersion() > 0;

            return new P1<GridCacheMvccCandidate>() {
                @Override public boolean apply(GridCacheMvccCandidate c) {
                    assert c.nearLocal() || c.dhtLocal();

                    // Wait for explicit locks.
                    return c.topologyVersion().equals(AffinityTopologyVersion.ZERO) || c.topologyVersion().compareTo(topVer) < 0;
                }
            };
        }

        /**
         *
         */
        void recheck() {
            for (Iterator<IgniteTxKey> it = pendingLocks.keySet().iterator(); it.hasNext(); ) {
                IgniteTxKey key = it.next();

                GridCacheContext cacheCtx = cctx.cacheContext(key.cacheId());

                GridCacheEntryEx entry = cacheCtx.cache().peekEx(key.key());

                if (entry == null)
                    it.remove();
                else
                    recheck(entry);
            }

            if (log.isDebugEnabled())
                log.debug("After rechecking finished future: " + this);

            if (pendingLocks.isEmpty()) {
                if (exchLog.isDebugEnabled())
                    exchLog.debug("Finish lock future is done: " + this);

                onDone();
            }
        }

        /**
         * @param entry Entry.
         */
        @SuppressWarnings({"SynchronizationOnLocalVariableOrMethodParameter"})
        void recheck(@Nullable GridCacheEntryEx entry) {
            if (entry == null)
                return;

            if (exchLog.isDebugEnabled())
                exchLog.debug("Rechecking entry for completion [entry=" + entry + ", finFut=" + this + ']');

            Collection<GridCacheMvccCandidate> cands = pendingLocks.get(entry.txKey());

            if (cands != null) {
                synchronized (cands) {
                    for (Iterator<GridCacheMvccCandidate> it = cands.iterator(); it.hasNext(); ) {
                        GridCacheMvccCandidate cand = it.next();

                        // Check exclude ID again, as key could have been reassigned.
                        if (cand.removed())
                            it.remove();
                    }

                    if (cands.isEmpty())
                        pendingLocks.remove(entry.txKey());

                    if (pendingLocks.isEmpty()) {
                        onDone();

                        if (exchLog.isDebugEnabled())
                            exchLog.debug("Finish lock future is done: " + this);
                    }
                }
            }
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            if (!pendingLocks.isEmpty()) {
                Map<GridCacheVersion, IgniteInternalTx> txs = new HashMap<>(1, 1.0f);

                for (Collection<GridCacheMvccCandidate> cands : pendingLocks.values())
                    for (GridCacheMvccCandidate c : cands)
                        txs.put(c.version(), cctx.tm().tx(c.version()));

                return S.toString(FinishLockFuture.class, this, "txs=" + txs + ", super=" + super.toString());
            }
            else
                return S.toString(FinishLockFuture.class, this, super.toString());
        }
    }

    /**
     * Finish atomic update future.
     */
    private static class FinishAtomicUpdateFuture extends CacheObjectsReleaseFuture<Object, Object> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * @param type Type.
         * @param topVer Topology version.
         */
        private FinishAtomicUpdateFuture(String type, AffinityTopologyVersion topVer) {
            super(type, topVer);
        }

        /** {@inheritDoc} */
        @Override protected boolean ignoreFailure(Throwable err) {
            Class cls = err.getClass();

            return ClusterTopologyCheckedException.class.isAssignableFrom(cls) ||
                CachePartialUpdateCheckedException.class.isAssignableFrom(cls);
        }
    }

    /**
     *
     */
    private class DataStreamerFuture extends GridFutureAdapter<Void> {
        /** Topology version. Instance field for toString method only. */
        @GridToStringInclude
        private final AffinityTopologyVersion topVer;

        /**
         * @param topVer Topology version.
         */
        DataStreamerFuture(AffinityTopologyVersion topVer) {
            this.topVer = topVer;
        }

        /** {@inheritDoc} */
        @Override public boolean onDone(@Nullable Void res, @Nullable Throwable err) {
            if (super.onDone(res, err)) {
                dataStreamerFuts.remove(this);

                return true;
            }

            return false;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(DataStreamerFuture.class, this, super.toString());
        }
    }

    /** Long wrapper. */
    private static class LongWrapper {
        /** */
        private long val;

        /**
         * @param val Value.
         */
        public LongWrapper(long val) {
            this.val = val + 1;

            if (this.val == 0)
                this.val = 1;
        }

        /**
         * @param val Value to set.
         */
        public void set(long val) {
            this.val = val;
        }

        /**
         * @return Current value.
         */
        public long get() {
            return val;
        }

        /**
         * @return Current value (and stores incremented value).
         */
        public long getAndIncrement() {
            return val++;
        }
    }
}
