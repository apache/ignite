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

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.IgniteClientDisconnectedCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.managers.eventstorage.GridLocalEventListener;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.distributed.GridCacheMappedVersion;
import org.apache.ignite.internal.processors.cache.distributed.GridDistributedCacheEntry;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxKey;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.GridBoundedConcurrentLinkedHashSet;
import org.apache.ignite.internal.util.GridConcurrentFactory;
import org.apache.ignite.internal.util.future.GridCompoundFuture;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.P1;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;
import org.jsr166.ConcurrentHashMap8;
import org.jsr166.ConcurrentLinkedDeque8;

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

    /** Pending locks per thread. */
    private final ThreadLocal<LinkedList<GridCacheMvccCandidate>> pending =
        new ThreadLocal<LinkedList<GridCacheMvccCandidate>>() {
            @Override protected LinkedList<GridCacheMvccCandidate> initialValue() {
                return new LinkedList<>();
            }
        };

    /** Pending near local locks and topology version per thread. */
    private ConcurrentMap<Long, GridCacheExplicitLockSpan> pendingExplicit;

    /** Set of removed lock versions. */
    private Collection<GridCacheVersion> rmvLocks =
        new GridBoundedConcurrentLinkedHashSet<>(MAX_REMOVED_LOCKS, MAX_REMOVED_LOCKS, 0.75f, 16, PER_SEGMENT_Q);

    /** Current local candidates. */
    private Collection<GridCacheMvccCandidate> dhtLocCands = new ConcurrentSkipListSet<>();

    /** Locked keys. */
    @GridToStringExclude
    private final ConcurrentMap<IgniteTxKey, GridDistributedCacheEntry> locked = newMap();

    /** Near locked keys. Need separate map because mvcc manager is shared between caches. */
    @GridToStringExclude
    private final ConcurrentMap<IgniteTxKey, GridDistributedCacheEntry> nearLocked = newMap();

    /** Active futures mapped by version ID. */
    @GridToStringExclude
    private final ConcurrentMap<GridCacheVersion, Collection<GridCacheFuture<?>>> futs = newMap();

    /** Pending atomic futures. */
    private final ConcurrentMap<GridCacheVersion, GridCacheAtomicFuture<?>> atomicFuts =
        new ConcurrentHashMap8<>();

    /** Near to DHT version mapping. */
    private final ConcurrentMap<GridCacheVersion, GridCacheVersion> near2dht = newMap();

    /** Finish futures. */
    private final Queue<FinishLockFuture> finishFuts = new ConcurrentLinkedDeque8<>();

    /** Logger. */
    @SuppressWarnings( {"FieldAccessedSynchronizedAndUnsynchronized"})
    private IgniteLogger exchLog;

    /** Lock callback. */
    @GridToStringExclude
    private final GridCacheMvccCallback cb = new GridCacheMvccCallback() {
        /** {@inheritDoc} */
        @SuppressWarnings({"unchecked"})
        @Override public void onOwnerChanged(GridCacheEntryEx entry, GridCacheMvccCandidate prev,
            GridCacheMvccCandidate owner) {
            assert entry != null;
            assert owner != prev : "New and previous owner are identical instances: " + owner;
            assert owner == null || prev == null || !owner.version().equals(prev.version()) :
                "New and previous owners have identical versions [owner=" + owner + ", prev=" + prev + ']';

            if (log.isDebugEnabled())
                log.debug("Received owner changed callback [" + entry.key() + ", owner=" + owner + ", prev=" +
                    prev + ']');

            if (owner != null && (owner.local() || owner.nearLocal())) {
                Collection<? extends GridCacheFuture> futCol = futs.get(owner.version());

                if (futCol != null) {
                    for (GridCacheFuture fut : futCol) {
                        if (fut instanceof GridCacheMvccFuture && !fut.isDone()) {
                            GridCacheMvccFuture<Boolean> mvccFut =
                                (GridCacheMvccFuture<Boolean>)fut;

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
                    owner + ", prev=" + prev + ", entry=" + entry + ']');

            // If no future was found, delegate to transaction manager.
            if (cctx.tm().onOwnerChanged(entry, owner)) {
                if (log.isDebugEnabled())
                    log.debug("Found transaction for changed owner: " + owner);
            }
            else if (log.isDebugEnabled())
                log.debug("Failed to find transaction for changed owner: " + owner);

            for (FinishLockFuture f : finishFuts)
                f.recheck(entry);
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

    /** Discovery listener. */
    @GridToStringExclude private final GridLocalEventListener discoLsnr = new GridLocalEventListener() {
        @Override public void onEvent(Event evt) {
            assert evt instanceof DiscoveryEvent;
            assert evt.type() == EVT_NODE_FAILED || evt.type() == EVT_NODE_LEFT;

            DiscoveryEvent discoEvt = (DiscoveryEvent)evt;

            if (log.isDebugEnabled())
                log.debug("Processing node left [nodeId=" + discoEvt.eventNode().id() + "]");

            for (Collection<GridCacheFuture<?>> futsCol : futs.values()) {
                for (GridCacheFuture<?> fut : futsCol) {
                    if (!fut.trackable()) {
                        if (log.isDebugEnabled())
                            log.debug("Skipping non-trackable future: " + fut);

                        continue;
                    }

                    fut.onNodeLeft(discoEvt.eventNode().id());

                    if (fut.isCancelled() || fut.isDone())
                        removeFuture(fut);
                }
            }

            for (IgniteInternalFuture<?> fut : atomicFuts.values()) {
                if (fut instanceof GridCacheFuture) {
                    GridCacheFuture cacheFut = (GridCacheFuture)fut;

                    cacheFut.onNodeLeft(discoEvt.eventNode().id());

                    if (cacheFut.isCancelled() || cacheFut.isDone()) {
                        GridCacheVersion futVer = cacheFut.version();

                        if (futVer != null)
                            atomicFuts.remove(futVer, fut);
                    }
                }
            }
        }
    };

    /** {@inheritDoc} */
    @Override protected void start0() throws IgniteCheckedException {
        exchLog = cctx.logger(getClass().getName() + ".exchange");

        pendingExplicit = GridConcurrentFactory.newMap();
    }

    /** {@inheritDoc} */
    @Override protected void onKernalStart0(boolean reconnect) throws IgniteCheckedException {
        if (!reconnect)
            cctx.gridEvents().addLocalEventListener(discoLsnr, EVT_NODE_FAILED, EVT_NODE_LEFT);
    }

    /** {@inheritDoc} */
    @Override public void onKernalStop0(boolean cancel) {
        cctx.gridEvents().removeLocalEventListener(discoLsnr);
    }

    /**
     * @return MVCC callback.
     */
    public GridCacheMvccCallback callback() {
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
        return F.flatCollections(futs.values());
    }

    /**
     * @param leftNodeId Left node ID.
     * @param topVer Topology version.
     */
    public void removeExplicitNodeLocks(UUID leftNodeId, AffinityTopologyVersion topVer) {
        for (GridDistributedCacheEntry entry : locked()) {
            try {
                entry.removeExplicitNodeLocks(leftNodeId);

                entry.context().evicts().touch(entry, topVer);
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
    public void cancelClientFutures() {
        cancelClientFutures(new IgniteCheckedException("Operation has been cancelled (node is stopping)."));
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
        for (Collection<GridCacheFuture<?>> futures : futs.values()) {
            for (GridCacheFuture<?> future : futures)
                ((GridFutureAdapter)future).onDone(err);
        }

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
     * @param futVer Future ID.
     * @param fut Future.
     */
    public void addAtomicFuture(GridCacheVersion futVer, GridCacheAtomicFuture<?> fut) {
        IgniteInternalFuture<?> old = atomicFuts.put(futVer, fut);

        assert old == null : "Old future is not null [futVer=" + futVer + ", fut=" + fut + ", old=" + old + ']';

        if (cctx.kernalContext().clientDisconnected())
            ((GridFutureAdapter)fut).onDone(disconnectedError(null));
    }

    /**
     * @return Collection of pending atomic futures.
     */
    public Collection<GridCacheAtomicFuture<?>> atomicFutures() {
        return atomicFuts.values();
    }

    /**
     * Gets future by given future ID.
     *
     * @param futVer Future ID.
     * @return Future.
     */
    @Nullable public IgniteInternalFuture<?> atomicFuture(GridCacheVersion futVer) {
        return atomicFuts.get(futVer);
    }

    /**
     * @param futVer Future ID.
     * @return Removed future.
     */
    @Nullable public IgniteInternalFuture<?> removeAtomicFuture(GridCacheVersion futVer) {
        return atomicFuts.remove(futVer);
    }

    /**
     * Adds future.
     *
     * @param fut Future.
     * @return {@code True} if added.
     */
    @SuppressWarnings({"SynchronizationOnLocalVariableOrMethodParameter"})
    public boolean addFuture(final GridCacheFuture<?> fut) {
        if (fut.isDone()) {
            fut.markNotTrackable();

            return true;
        }

        if (!fut.trackable())
            return true;

        while (true) {
            Collection<GridCacheFuture<?>> old = futs.putIfAbsent(fut.version(),
                new ConcurrentLinkedDeque8<GridCacheFuture<?>>() {
                    /** */
                    private int hash;

                    {
                        // Make sure that we add future to queue before
                        // adding queue to the map of futures.
                        add(fut);
                    }

                    @Override public int hashCode() {
                        if (hash == 0)
                            hash = System.identityHashCode(this);

                        return hash;
                    }

                    @Override public boolean equals(Object obj) {
                        return obj == this;
                    }
                });

            if (old != null) {
                boolean empty, dup = false;

                synchronized (old) {
                    empty = old.isEmpty();

                    if (!empty)
                        dup = old.contains(fut);

                    if (!empty && !dup)
                        old.add(fut);
                }

                // Future is being removed, so we force-remove here and try again.
                if (empty) {
                    if (futs.remove(fut.version(), old)) {
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

        // Close window in case of node is gone before the future got added to
        // the map of futures.
        for (ClusterNode n : fut.nodes()) {
            if (cctx.discovery().node(n.id()) == null)
                fut.onNodeLeft(n.id());
        }

        if (cctx.kernalContext().clientDisconnected())
            ((GridFutureAdapter)fut).onDone(disconnectedError(null));

        // Just in case if future was completed before it was added.
        if (fut.isDone())
            removeFuture(fut);

        return true;
    }

    /**
     * @param fut Future to remove.
     * @return {@code True} if removed.
     */
    @SuppressWarnings({"SynchronizationOnLocalVariableOrMethodParameter"})
    public boolean removeFuture(GridCacheFuture<?> fut) {
        if (!fut.trackable())
            return true;

        Collection<GridCacheFuture<?>> cur = futs.get(fut.version());

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

        if (empty && futs.remove(fut.version(), cur))
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
    @Nullable public GridCacheFuture future(GridCacheVersion ver, IgniteUuid futId) {
        Collection<? extends GridCacheFuture> futs = this.futs.get(ver);

        if (futs != null)
            for (GridCacheFuture<?> fut : futs)
                if (fut.futureId().equals(futId)) {
                    if (log.isDebugEnabled())
                        log.debug("Found future in futures map: " + fut);

                    return fut;
                }

        if (log.isDebugEnabled())
            log.debug("Failed to find future in futures map [ver=" + ver + ", futId=" + futId + ']');

        return null;
    }

    /**
     * Gets all futures for given lock version, possibly empty collection.
     *
     * @param ver Version.
     * @return All futures for given lock version.
     */
    @SuppressWarnings("unchecked")
    public <T> Collection<? extends IgniteInternalFuture<T>> futures(GridCacheVersion ver) {
        Collection c = futs.get(ver);

        return c == null ? Collections.<IgniteInternalFuture<T>>emptyList() : (Collection<IgniteInternalFuture<T>>)c;
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
        Collection<GridCacheMvccCandidate> rmtCands = new LinkedList<>();

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
        Collection<GridCacheMvccCandidate> locCands = new LinkedList<>();

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
     * @param cand Local lock.
     * @return {@code True} if added.
     */
    public boolean addLocal(GridCacheMvccCandidate cand) {
        assert cand.key() != null;
        assert cand.local();

        if (cand.dhtLocal() && dhtLocCands.add(cand)) {
            if (log.isDebugEnabled())
                log.debug("Added local candidate: " + cand);

            return true;
        }

        return false;
    }

    /**
     *
     * @param cand Local candidate to remove.
     * @return {@code True} if removed.
     */
    public boolean removeLocal(GridCacheMvccCandidate cand) {
        assert cand.key() != null;
        assert cand.local();

        if (cand.dhtLocal() && dhtLocCands.remove(cand)) {
            if (log.isDebugEnabled())
                log.debug("Removed local candidate: " + cand);

            return true;
        }

        return false;
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

        LinkedList<GridCacheMvccCandidate> queue = pending.get();

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
        pending.set(new LinkedList<GridCacheMvccCandidate>());
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
    public boolean isLockedByThread(KeyCacheObject key, long threadId) {
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
    public void markExplicitOwner(KeyCacheObject key, long threadId) {
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
        KeyCacheObject key,
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
    @Nullable public GridCacheMvccCandidate explicitLock(long threadId, KeyCacheObject key) {
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
    @Nullable public GridCacheMvccCandidate explicitLock(KeyCacheObject key, @Nullable GridCacheVersion ver) {
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
        X.println(">>> Mvcc manager memory stats [grid=" + cctx.gridName() + ']');
        X.println(">>>   rmvLocksSize: " + rmvLocks.size());
        X.println(">>>   dhtLocCandsSize: " + dhtLocCands.size());
        X.println(">>>   lockedSize: " + locked.size());
        X.println(">>>   futsSize: " + futs.size());
        X.println(">>>   near2dhtSize: " + near2dht.size());
        X.println(">>>   finishFutsSize: " + finishFuts.size());
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

        for (FinishLockFuture fut : finishFuts) {
            if (fut.topologyVersion().equals(topVer))
                cands.putAll(fut.pendingLocks());
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
        GridCompoundFuture<Object, Object> res = new GridCompoundFuture<>();

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
        GridCompoundFuture<Object, Object> res = new GridCompoundFuture<>();

        res.ignoreChildFailures(ClusterTopologyCheckedException.class, CachePartialUpdateCheckedException.class);

        for (GridCacheAtomicFuture<?> fut : atomicFuts.values()) {
            IgniteInternalFuture<Void> complete = fut.completeFuture(topVer);

            if (complete != null)
                res.add((IgniteInternalFuture)complete);
        }

        res.markInitialized();

        return res;
    }

    /**
     * @param keys Key for which locks should be released.
     * @param topVer Topology version.
     * @return Future that signals when all locks for given keys are released.
     */
    @SuppressWarnings("unchecked")
    public IgniteInternalFuture<?> finishKeys(Collection<KeyCacheObject> keys, AffinityTopologyVersion topVer) {
        if (!(keys instanceof Set))
            keys = new HashSet<>(keys);

        final Collection<KeyCacheObject> keys0 = keys;

        return finishLocks(new P1<KeyCacheObject>() {
            @Override public boolean apply(KeyCacheObject key) {
                return keys0.contains(key);
            }
        }, topVer);
    }

    /**
     * @param keyFilter Key filter.
     * @param topVer Topology version.
     * @return Future that signals when all locks for given partitions will be released.
     */
    private IgniteInternalFuture<?> finishLocks(@Nullable final IgnitePredicate<KeyCacheObject> keyFilter, AffinityTopologyVersion topVer) {
        assert topVer.topologyVersion() != 0;

        if (topVer.equals(AffinityTopologyVersion.NONE))
            return new GridFinishedFuture();

        final FinishLockFuture finishFut = new FinishLockFuture(
            keyFilter == null ?
                locked() :
                F.view(locked(),
                    new P1<GridDistributedCacheEntry>() {
                        @Override public boolean apply(GridDistributedCacheEntry e) {
                            return F.isAll(e.key(), keyFilter);
                        }
                    }
                ),
            topVer);

        finishFuts.add(finishFut);

        finishFut.listen(new CI1<IgniteInternalFuture<?>>() {
            @Override public void apply(IgniteInternalFuture<?> e) {
                finishFuts.remove(finishFut);

                // This call is required to make sure that the concurrent queue
                // clears memory occupied by internal nodes.
                finishFuts.peek();
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

        for (FinishLockFuture fut : finishFuts)
            fut.recheck();
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
            new ConcurrentHashMap8<>();

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
}