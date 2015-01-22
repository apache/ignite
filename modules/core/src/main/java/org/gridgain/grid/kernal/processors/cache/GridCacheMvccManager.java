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

package org.gridgain.grid.kernal.processors.cache;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.events.*;
import org.apache.ignite.lang.*;
import org.gridgain.grid.kernal.managers.discovery.*;
import org.gridgain.grid.kernal.managers.eventstorage.*;
import org.gridgain.grid.kernal.processors.cache.distributed.*;
import org.gridgain.grid.kernal.processors.cache.transactions.*;
import org.gridgain.grid.util.*;
import org.apache.ignite.internal.util.future.*;
import org.apache.ignite.internal.util.tostring.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.jdk8.backport.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import static org.apache.ignite.events.IgniteEventType.*;
import static org.gridgain.grid.util.GridConcurrentFactory.*;
import static org.jdk8.backport.ConcurrentLinkedHashMap.QueuePolicy.*;

/**
 * Manages lock order within a thread.
 */
public class GridCacheMvccManager<K, V> extends GridCacheSharedManagerAdapter<K, V> {
    /** Maxim number of removed locks. */
    private static final int MAX_REMOVED_LOCKS = 10240;

    /** Pending locks per thread. */
    private final GridThreadLocal<Queue<GridCacheMvccCandidate<K>>> pending =
        new GridThreadLocal<Queue<GridCacheMvccCandidate<K>>>() {
            @Override protected Queue<GridCacheMvccCandidate<K>> initialValue() {
                return new LinkedList<>();
            }
        };

    /** Pending near local locks and topology version per thread. */
    private ConcurrentMap<Long, GridCacheExplicitLockSpan<K>> pendingExplicit;

    /** Set of removed lock versions. */
    private Collection<GridCacheVersion> rmvLocks =
        new GridBoundedConcurrentLinkedHashSet<>(MAX_REMOVED_LOCKS, MAX_REMOVED_LOCKS, 0.75f, 16, PER_SEGMENT_Q);

    /** Current local candidates. */
    private Collection<GridCacheMvccCandidate<K>> dhtLocCands = new ConcurrentSkipListSet<>();

    /** Locked keys. */
    @GridToStringExclude
    private final ConcurrentMap<IgniteTxKey<K>, GridDistributedCacheEntry<K, V>> locked = newMap();

    /** Near locked keys. Need separate map because mvcc manager is shared between caches. */
    @GridToStringExclude
    private final ConcurrentMap<IgniteTxKey<K>, GridDistributedCacheEntry<K, V>> nearLocked = newMap();

    /** Active futures mapped by version ID. */
    @GridToStringExclude
    private final ConcurrentMap<GridCacheVersion, Collection<GridCacheFuture<?>>> futs = newMap();

    /** Pending atomic futures. */
    private final ConcurrentMap<GridCacheVersion, GridCacheAtomicFuture<K, ?>> atomicFuts =
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
    private final GridCacheMvccCallback<K, V> cb = new GridCacheMvccCallback<K, V>() {
        /** {@inheritDoc} */
        @SuppressWarnings({"unchecked"})
        @Override public void onOwnerChanged(GridCacheEntryEx<K, V> entry, GridCacheMvccCandidate<K> prev,
            GridCacheMvccCandidate<K> owner) {
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
                            GridCacheMvccFuture<K, V, Boolean> mvccFut =
                                (GridCacheMvccFuture<K, V, Boolean>)fut;

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
        @Override public void onLocked(GridDistributedCacheEntry<K, V> entry) {
            if (entry.isNear())
                nearLocked.put(entry.txKey(), entry);
            else
                locked.put(entry.txKey(), entry);
        }

        /** {@inheritDoc} */
        @Override public void onFreed(GridDistributedCacheEntry<K, V> entry) {
            if (entry.isNear())
                nearLocked.remove(entry.txKey());
            else
                locked.remove(entry.txKey());
        }
    };

    /** Discovery listener. */
    @GridToStringExclude private final GridLocalEventListener discoLsnr = new GridLocalEventListener() {
        @Override public void onEvent(IgniteEvent evt) {
            assert evt instanceof IgniteDiscoveryEvent;
            assert evt.type() == EVT_NODE_FAILED || evt.type() == EVT_NODE_LEFT;

            IgniteDiscoveryEvent discoEvt = (IgniteDiscoveryEvent)evt;

            if (log.isDebugEnabled())
                log.debug("Processing node left [nodeId=" + discoEvt.eventNode().id() + "]");

            for (GridDistributedCacheEntry<K, V> entry : locked()) {
                try {
                    entry.removeExplicitNodeLocks(discoEvt.eventNode().id());
                }
                catch (GridCacheEntryRemovedException ignore) {
                    if (log.isDebugEnabled())
                        log.debug("Attempted to remove node locks from removed entry in mvcc manager " +
                            "disco callback (will ignore): " + entry);
                }
            }

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

            for (IgniteFuture<?> fut : atomicFuts.values()) {
                if (fut instanceof GridCacheFuture) {
                    GridCacheFuture cacheFut = (GridCacheFuture)fut;

                    cacheFut.onNodeLeft(discoEvt.eventNode().id());

                    if (cacheFut.isCancelled() || cacheFut.isDone())
                        atomicFuts.remove(cacheFut.futureId(), fut);
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
    @Override public void onKernalStart0() throws IgniteCheckedException {
        cctx.gridEvents().addLocalEventListener(discoLsnr, EVT_NODE_FAILED, EVT_NODE_LEFT);
    }

    /** {@inheritDoc} */
    @Override public void onKernalStop0(boolean cancel) {
        cctx.gridEvents().removeLocalEventListener(discoLsnr);
    }

    /**
     * @return MVCC callback.
     */
    public GridCacheMvccCallback<K, V> callback() {
        return cb;
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
     * @param fut Future to check.
     * @return {@code True} if future is registered.
     */
    public boolean hasFuture(GridCacheFuture<?> fut) {
        assert fut != null;

        return future(fut.version(), fut.futureId()) != null;
    }

    /**
     * @param futVer Future ID.
     * @param fut Future.
     */
    public void addAtomicFuture(GridCacheVersion futVer, GridCacheAtomicFuture<K, ?> fut) {
        IgniteFuture<?> old = atomicFuts.put(futVer, fut);

        assert old == null;
    }

    /**
     * @return Collection of pending atomic futures.
     */
    public Collection<GridCacheAtomicFuture<K, ?>> atomicFutures() {
        return atomicFuts.values();
    }

    /**
     * Gets future by given future ID.
     *
     * @param futVer Future ID.
     * @return Future.
     */
    @Nullable public IgniteFuture<?> atomicFuture(GridCacheVersion futVer) {
        return atomicFuts.get(futVer);
    }

    /**
     * @param futVer Future ID.
     * @return Removed future.
     */
    @Nullable public IgniteFuture<?> removeAtomicFuture(GridCacheVersion futVer) {
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

        // Just in case if future was complete before it was added.
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
    @Nullable public <T> GridCacheFuture<T> future(GridCacheVersion ver, IgniteUuid futId) {
        Collection<? extends GridCacheFuture> futs = this.futs.get(ver);

        if (futs != null)
            for (GridCacheFuture<?> fut : futs)
                if (fut.futureId().equals(futId)) {
                    if (log.isDebugEnabled())
                        log.debug("Found future in futures map: " + fut);

                    return (GridCacheFuture<T>)fut;
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
    public <T> Collection<? extends IgniteFuture<T>> futures(GridCacheVersion ver) {
        Collection c = futs.get(ver);

        return c == null ? Collections.<IgniteFuture<T>>emptyList() : (Collection<IgniteFuture<T>>)c;
    }

    /**
     * @param ver Lock version to check.
     * @return {@code True} if lock had been removed.
     */
    public boolean isRemoved(GridCacheContext<K, V> cacheCtx, GridCacheVersion ver) {
        return !cacheCtx.isNear() && !cacheCtx.isLocal() && ver != null && rmvLocks.contains(ver);
    }

    /**
     * @param ver Obsolete entry version.
     * @return {@code True} if added.
     */
    public boolean addRemoved(GridCacheContext<K, V> cacheCtx, GridCacheVersion ver) {
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
    private Collection<GridDistributedCacheEntry<K, V>> locked() {
        return F.concat(false, locked.values(), nearLocked.values());
    }

    /**
     * This method has poor performance, so use with care. It is currently only used by {@code DGC}.
     *
     * @return Remote candidates.
     */
    public Collection<GridCacheMvccCandidate<K>> remoteCandidates() {
        Collection<GridCacheMvccCandidate<K>> rmtCands = new LinkedList<>();

        for (GridDistributedCacheEntry<K, V> entry : locked()) {
            rmtCands.addAll(entry.remoteMvccSnapshot());
        }

        return rmtCands;
    }

    /**
     * This method has poor performance, so use with care. It is currently only used by {@code DGC}.
     *
     * @return Local candidates.
     */
    public Collection<GridCacheMvccCandidate<K>> localCandidates() {
        Collection<GridCacheMvccCandidate<K>> locCands = new LinkedList<>();

        for (GridDistributedCacheEntry<K, V> entry : locked()) {
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
    public boolean addLocal(GridCacheMvccCandidate<K> cand) {
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
    public boolean removeLocal(GridCacheMvccCandidate<K> cand) {
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
     * @param keys Keys.
     * @param base Base version.
     * @return Versions that are less than {@code base} whose keys are in the {@code keys} collection.
     */
    public Collection<GridCacheVersion> localDhtPendingVersions(Collection<K> keys, GridCacheVersion base) {
        Collection<GridCacheVersion> lessPending = new GridLeanSet<>(5);

        for (GridCacheMvccCandidate<K> cand : dhtLocCands) {
            if (cand.version().isLess(base)) {
                if (keys.contains(cand.key()))
                    lessPending.add(cand.version());
            }
            else
                break;
        }

        return lessPending;
    }

    /**
     * Unlinks a lock candidate.
     *
     * @param cand Lock candidate to unlink.
     */
    private void unlink(GridCacheMvccCandidate<K> cand) {
        GridCacheMvccCandidate<K> next = cand.next();

        if (next != null) {
            GridCacheMvccCandidate<K> prev = cand.previous();

            next.previous(prev);

            if (prev != null)
                prev.next(next);
        }

        /*
         * Note that we specifically don't set links from passed in
         * candidate to null because it is possible in some race
         * cases that it will get traversed. However, it should
         * still become available for GC and should not cause
         * an issue.
         */

        if (log.isDebugEnabled())
            log.debug("Unlinked lock candidate: " + cand);
    }

    /**
     *
     * @param cand Cache lock candidate to add.
     * @return {@code True} if added as a result of this operation,
     *      {@code false} if was previously added.
     */
    public boolean addNext(GridCacheContext<K, V> cacheCtx, GridCacheMvccCandidate<K> cand) {
        assert cand != null;
        assert !cand.reentry() : "Lock reentries should not be linked: " + cand;

        // Don't order near candidates by thread as they will be ordered on
        // DHT node. Also, if candidate is implicit, no point to order him.
        if (cacheCtx.isNear() || cand.singleImplicit())
            return true;

        Queue<GridCacheMvccCandidate<K>> queue = pending.get();

        boolean add = true;

        GridCacheMvccCandidate<K> prev = null;

        for (Iterator<GridCacheMvccCandidate<K>> it = queue.iterator(); it.hasNext(); ) {
            GridCacheMvccCandidate<K> c = it.next();

            if (c.equals(cand))
                add = false;

            if (c.used()) {
                it.remove();

                unlink(c);

                continue;
            }

            prev = c;
        }

        if (add) {
            queue.add(cand);

            if (prev != null) {
                prev.next(cand);

                cand.previous(prev);
            }

            if (log.isDebugEnabled())
                log.debug("Linked new candidate: " + cand);
        }

        return add;
    }

    /**
     * Adds candidate to the list of near local candidates.
     *
     * @param threadId Thread ID.
     * @param cand Candidate to add.
     * @param snapshot Topology snapshot.
     */
    public void addExplicitLock(long threadId, GridCacheMvccCandidate<K> cand, GridDiscoveryTopologySnapshot snapshot) {
        while (true) {
            GridCacheExplicitLockSpan<K> span = pendingExplicit.get(cand.threadId());

            if (span == null) {
                span = new GridCacheExplicitLockSpan<>(snapshot, cand);

                GridCacheExplicitLockSpan<K> old = pendingExplicit.putIfAbsent(threadId, span);

                if (old == null)
                    break;
                else
                    span = old;
            }

            // Either span was not empty, or concurrent put did not succeed.
            if (span.addCandidate(snapshot, cand))
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
    public void removeExplicitLock(GridCacheMvccCandidate<K> cand) {
        GridCacheExplicitLockSpan<K> span = pendingExplicit.get(cand.threadId());

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
    public boolean isLockedByThread(K key, long threadId) {
        if (threadId < 0) {
            for (GridCacheExplicitLockSpan<K> span : pendingExplicit.values()) {
                GridCacheMvccCandidate<K> cand = span.candidate(key, null);

                if (cand != null && cand.owner())
                    return true;
            }
        }
        else {
            GridCacheExplicitLockSpan<K> span = pendingExplicit.get(threadId);

            if (span != null) {
                GridCacheMvccCandidate<K> cand = span.candidate(key, null);

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
    public void markExplicitOwner(K key, long threadId) {
        assert threadId > 0;

        GridCacheExplicitLockSpan<K> span = pendingExplicit.get(threadId);

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
    public GridCacheMvccCandidate<K> removeExplicitLock(long threadId, K key, @Nullable GridCacheVersion ver) {
        assert threadId > 0;

        GridCacheExplicitLockSpan<K> span = pendingExplicit.get(threadId);

        if (span == null)
            return null;

        GridCacheMvccCandidate<K> cand = span.removeCandidate(key, ver);

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
    @Nullable public GridCacheMvccCandidate<K> explicitLock(long threadId, K key) {
        if (threadId < 0)
            return explicitLock(key, null);
        else {
            GridCacheExplicitLockSpan<K> span = pendingExplicit.get(threadId);

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
    @Nullable public GridCacheMvccCandidate<K> explicitLock(K key, @Nullable GridCacheVersion ver) {
        for (GridCacheExplicitLockSpan<K> span : pendingExplicit.values()) {
            GridCacheMvccCandidate<K> cand = span.candidate(key, ver);

            if (cand != null)
                return cand;
        }

        return null;
    }

    /**
     * @param threadId Thread ID.
     * @return Topology snapshot for last acquired and not released lock.
     */
    @Nullable public GridDiscoveryTopologySnapshot lastExplicitLockTopologySnapshot(long threadId) {
        GridCacheExplicitLockSpan<K> span = pendingExplicit.get(threadId);

        return span != null ? span.topologySnapshot() : null;
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
     * @param nodeId Node ID.
     * @return Filter.
     */
    private IgnitePredicate<GridCacheMvccCandidate<K>> nodeIdFilter(final UUID nodeId) {
        if (nodeId == null)
            return F.alwaysTrue();

        return new P1<GridCacheMvccCandidate<K>>() {
            @Override public boolean apply(GridCacheMvccCandidate<K> c) {
                UUID otherId = c.otherNodeId();

                return c.nodeId().equals(nodeId) || (otherId != null && otherId.equals(nodeId));
            }
        };
    }

    /**
     * @param topVer Topology version.
     * @return Future that signals when all locks for given partitions are released.
     */
    @SuppressWarnings({"unchecked"})
    public IgniteFuture<?> finishLocks(long topVer) {
        assert topVer > 0;
        return finishLocks(null, topVer);
    }

    /**
     * Creates a future that will wait for all explicit locks acquired on given topology
     * version to be released.
     *
     * @param topVer Topology version to wait for.
     * @return Explicit locks release future.
     */
    public IgniteFuture<?> finishExplicitLocks(long topVer) {
        GridCompoundFuture<Object, Object> res = new GridCompoundFuture<>(cctx.kernalContext());

        for (GridCacheExplicitLockSpan<K> span : pendingExplicit.values()) {
            GridDiscoveryTopologySnapshot snapshot = span.topologySnapshot();

            if (snapshot != null && snapshot.topologyVersion() < topVer)
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
    public IgniteFuture<?> finishAtomicUpdates(long topVer) {
        GridCompoundFuture<Object, Object> res = new GridCompoundFuture<>(cctx.kernalContext());

        res.ignoreChildFailures(ClusterTopologyException.class, GridCachePartialUpdateException.class);

        for (GridCacheAtomicFuture<K, ?> fut : atomicFuts.values()) {
            if (fut.waitForPartitionExchange() && fut.topologyVersion() < topVer)
                res.add((IgniteFuture<Object>)fut);
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
    public IgniteFuture<?> finishKeys(Collection<K> keys, long topVer) {
        if (!(keys instanceof Set))
            keys = new HashSet<>(keys);

        final Collection<K> keys0 = keys;

        return finishLocks(new P1<K>() {
            @Override public boolean apply(K key) {
                return keys0.contains(key);
            }
        }, topVer);
    }

    /**
     * @param keyFilter Key filter.
     * @param topVer Topology version.
     * @return Future that signals when all locks for given partitions will be released.
     */
    private IgniteFuture<?> finishLocks(@Nullable final IgnitePredicate<K> keyFilter, long topVer) {
        assert topVer != 0;

        if (topVer < 0)
            return new GridFinishedFuture(context().kernalContext());

        final FinishLockFuture finishFut = new FinishLockFuture(
            keyFilter == null ?
                locked() :
                F.view(locked(),
                    new P1<GridDistributedCacheEntry<K, V>>() {
                        @Override public boolean apply(GridDistributedCacheEntry<K, V> e) {
                            return F.isAll(e.key(), keyFilter);
                        }
                    }
                ),
            topVer);

        finishFuts.add(finishFut);

        finishFut.listenAsync(new CI1<IgniteFuture<?>>() {
            @Override public void apply(IgniteFuture<?> e) {
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
        private final long topVer;

        /** */
        @GridToStringInclude
        private final Map<IgniteTxKey<K>, Collection<GridCacheMvccCandidate<K>>> pendingLocks =
            new ConcurrentHashMap8<>();

        /**
         * Empty constructor required for {@link Externalizable}.
         */
        public FinishLockFuture() {
            assert false;

            topVer = 0;
        }

        /**
         * @param topVer Topology version.
         * @param entries Entries.
         */
        FinishLockFuture(Iterable<GridDistributedCacheEntry<K, V>> entries, long topVer) {
            super(cctx.kernalContext(), true);

            assert topVer > 0;

            this.topVer = topVer;

            for (GridCacheEntryEx<K, V> entry : entries) {
                // Either local or near local candidates.
                try {
                    Collection<GridCacheMvccCandidate<K>> locs = entry.localCandidates();

                    if (!F.isEmpty(locs)) {
                        Collection<GridCacheMvccCandidate<K>> cands =
                            new ConcurrentLinkedQueue<>();

                        if (locs != null)
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
         * @return Filter.
         */
        private IgnitePredicate<GridCacheMvccCandidate<K>> versionFilter() {
            assert topVer > 0;

            return new P1<GridCacheMvccCandidate<K>>() {
                @Override public boolean apply(GridCacheMvccCandidate<K> c) {
                    assert c.nearLocal() || c.dhtLocal();

                    // Wait for explicit locks.
                    return c.topologyVersion() == 0 || c.topologyVersion() < topVer;
                }
            };
        }

        /**
         *
         */
        void recheck() {
            for (Iterator<IgniteTxKey<K>> it = pendingLocks.keySet().iterator(); it.hasNext(); ) {
                IgniteTxKey<K> key = it.next();

                GridCacheContext<K, V> cacheCtx = cctx.cacheContext(key.cacheId());

                GridCacheEntryEx<K, V> entry = cacheCtx.cache().peekEx(key.key());

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
        void recheck(@Nullable GridCacheEntryEx<K, V> entry) {
            if (entry == null)
                return;

            if (exchLog.isDebugEnabled())
                exchLog.debug("Rechecking entry for completion [entry=" + entry + ", finFut=" + this + ']');

            Collection<GridCacheMvccCandidate<K>> cands = pendingLocks.get(entry.txKey());

            if (cands != null) {
                synchronized (cands) {
                    for (Iterator<GridCacheMvccCandidate<K>> it = cands.iterator(); it.hasNext(); ) {
                        GridCacheMvccCandidate<K> cand = it.next();

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
                Map<GridCacheVersion, IgniteTxEx> txs = new HashMap<>(1, 1.0f);

                for (Collection<GridCacheMvccCandidate<K>> cands : pendingLocks.values())
                    for (GridCacheMvccCandidate<K> c : cands)
                        txs.put(c.version(), cctx.tm().<IgniteTxEx>tx(c.version()));

                return S.toString(FinishLockFuture.class, this, "txs=" + txs + ", super=" + super.toString());
            }
            else
                return S.toString(FinishLockFuture.class, this, super.toString());
        }
    }
}
