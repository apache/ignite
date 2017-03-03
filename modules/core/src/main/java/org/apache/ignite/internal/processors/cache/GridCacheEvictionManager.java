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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheMemoryMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.eviction.EvictionFilter;
import org.apache.ignite.cache.eviction.EvictionPolicy;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.IgniteFutureCancelledCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.managers.eventstorage.GridLocalEventListener;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtCacheEntry;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtInvalidPartitionException;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxEntry;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.timeout.GridTimeoutObject;
import org.apache.ignite.internal.processors.timeout.GridTimeoutObjectAdapter;
import org.apache.ignite.internal.util.F0;
import org.apache.ignite.internal.util.GridBusyLock;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.lang.GridMetadataAwareAdapter;
import org.apache.ignite.internal.util.lang.IgnitePair;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.C1;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.CI2;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.P1;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.util.worker.GridWorker;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.thread.IgniteThread;
import org.jetbrains.annotations.Nullable;
import org.jsr166.ConcurrentHashMap8;
import org.jsr166.ConcurrentLinkedDeque8;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.cache.CacheMemoryMode.OFFHEAP_TIERED;
import static org.apache.ignite.cache.CacheMode.LOCAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.events.EventType.EVT_CACHE_ENTRY_EVICTED;
import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_JOINED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;
import static org.apache.ignite.internal.processors.cache.GridCacheUtils.isNearEnabled;
import static org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionState.MOVING;
import static org.jsr166.ConcurrentLinkedDeque8.Node;

/**
 * Cache eviction manager.
 */
public class GridCacheEvictionManager extends GridCacheManagerAdapter {
    /** Attribute name used to queue node in entry metadata. */
    private static final int META_KEY = GridMetadataAwareAdapter.EntryKey.CACHE_EVICTION_MANAGER_KEY.key();

    /** Eviction policy. */
    private EvictionPolicy plc;

    /** Eviction filter. */
    private EvictionFilter filter;

    /** Eviction buffer. */
    private final ConcurrentLinkedDeque8<EvictionInfo> bufEvictQ = new ConcurrentLinkedDeque8<>();

    /** Active eviction futures. */
    private final Map<Long, EvictionFuture> futs = new ConcurrentHashMap8<>();

    /** Futures count modification lock. */
    private final Lock futsCntLock = new ReentrantLock();

    /** Futures count condition. */
    private final Condition futsCntCond = futsCntLock.newCondition();

    /** Active futures count. */
    private volatile int activeFutsCnt;

    /** Max active futures count. */
    private int maxActiveFuts;

    /** Generator of future IDs. */
    private final AtomicLong idGen = new AtomicLong();

    /** Evict backup synchronized flag. */
    private boolean evictSync;

    /** Evict near synchronized flag. */
    private boolean nearSync;

    /** Flag to hold {@code evictSync || nearSync} result. */
    private boolean evictSyncAgr;

    /** Policy enabled. */
    private boolean plcEnabled;

    /** */
    private CacheMemoryMode memoryMode;

    /** Backup entries worker. */
    private BackupWorker backupWorker;

    /** Backup entries worker thread. */
    private IgniteThread backupWorkerThread;

    /** Busy lock. */
    private final GridBusyLock busyLock = new GridBusyLock();

    /** Stopping flag. */
    private volatile boolean stopping;

    /** Stopped flag. */
    private boolean stopped;

    /** Current future. */
    private final AtomicReference<EvictionFuture> curEvictFut = new AtomicReference<>();

    /** First eviction flag. */
    private volatile boolean firstEvictWarn;

    /** {@inheritDoc} */
    @Override public void start0() throws IgniteCheckedException {
        CacheConfiguration cfg = cctx.config();

        plc = cctx.isNear() ? cfg.getNearConfiguration().getNearEvictionPolicy() : cfg.getEvictionPolicy();

        memoryMode = cctx.config().getMemoryMode();

        plcEnabled = plc != null && (cctx.isNear() || memoryMode != OFFHEAP_TIERED);

        filter = cfg.getEvictionFilter();

        if (cfg.getEvictMaxOverflowRatio() < 0)
            throw new IgniteCheckedException("Configuration parameter 'maxEvictOverflowRatio' cannot be negative.");

        if (cfg.getEvictSynchronizedKeyBufferSize() < 0)
            throw new IgniteCheckedException("Configuration parameter 'evictSynchronizedKeyBufferSize' cannot be negative.");

        if (!cctx.isLocal()) {
            evictSync = cfg.isEvictSynchronized() && !cctx.isNear() && !cctx.isSwapOrOffheapEnabled();

            nearSync = isNearEnabled(cctx) && !cctx.isNear() && cfg.isEvictSynchronized();
        }
        else {
            if (cfg.isEvictSynchronized())
                U.warn(log, "Ignored 'evictSynchronized' configuration property for LOCAL cache: " + cctx.namexx());

            if (cfg.getNearConfiguration() != null && cfg.isEvictSynchronized())
                U.warn(log, "Ignored 'evictNearSynchronized' configuration property for LOCAL cache: " + cctx.namexx());
        }

        if (cctx.isDht() && !nearSync && evictSync && isNearEnabled(cctx))
            throw new IgniteCheckedException("Illegal configuration (may lead to data inconsistency) " +
                "[evictSync=true, evictNearSync=false]");

        reportConfigurationProblems();

        evictSyncAgr = evictSync || nearSync;

        if (evictSync && !cctx.isNear() && plcEnabled) {
            backupWorker = new BackupWorker();

            cctx.events().addListener(
                new GridLocalEventListener() {
                    @Override public void onEvent(Event evt) {
                        assert evt.type() == EVT_NODE_FAILED || evt.type() == EVT_NODE_LEFT ||
                            evt.type() == EVT_NODE_JOINED;

                        DiscoveryEvent discoEvt = (DiscoveryEvent)evt;

                        // Notify backup worker on each topology change.
                        if (cctx.discovery().cacheAffinityNode(discoEvt.eventNode(), cctx.name()))
                            backupWorker.addEvent(discoEvt);
                    }
                },
                EVT_NODE_FAILED, EVT_NODE_LEFT, EVT_NODE_JOINED);
        }

        if (evictSyncAgr) {
            if (cfg.getEvictSynchronizedTimeout() <= 0)
                throw new IgniteCheckedException("Configuration parameter 'evictSynchronousTimeout' should be positive.");

            if (cfg.getEvictSynchronizedConcurrencyLevel() <= 0)
                throw new IgniteCheckedException("Configuration parameter 'evictSynchronousConcurrencyLevel' " +
                    "should be positive.");

            maxActiveFuts = cfg.getEvictSynchronizedConcurrencyLevel();

            cctx.io().addHandler(cctx.cacheId(), GridCacheEvictionRequest.class, new CI2<UUID, GridCacheEvictionRequest>() {
                @Override public void apply(UUID nodeId, GridCacheEvictionRequest msg) {
                    processEvictionRequest(nodeId, msg);
                }
            });

            cctx.io().addHandler(cctx.cacheId(), GridCacheEvictionResponse.class, new CI2<UUID, GridCacheEvictionResponse>() {
                @Override public void apply(UUID nodeId, GridCacheEvictionResponse msg) {
                    processEvictionResponse(nodeId, msg);
                }
            });

            cctx.events().addListener(
                new GridLocalEventListener() {
                    @Override public void onEvent(Event evt) {
                        assert evt.type() == EVT_NODE_FAILED || evt.type() == EVT_NODE_LEFT;

                        DiscoveryEvent discoEvt = (DiscoveryEvent)evt;

                        for (EvictionFuture fut : futs.values())
                            fut.onNodeLeft(discoEvt.eventNode().id());
                    }
                },
                EVT_NODE_FAILED, EVT_NODE_LEFT);
        }

        if (log.isDebugEnabled())
            log.debug("Eviction manager started on node: " + cctx.nodeId());
    }

    /**
     * Outputs warnings if potential configuration problems are detected.
     */
    private void reportConfigurationProblems() {
        CacheMode mode = cctx.config().getCacheMode();

        if (plcEnabled && !cctx.isNear() && mode == PARTITIONED) {
            if (!evictSync) {
                U.warn(log, "Evictions are not synchronized with other nodes in topology " +
                    "which provides 2x-3x better performance but may cause data inconsistency if cache store " +
                    "is not configured (consider changing 'evictSynchronized' configuration property).",
                    "Evictions are not synchronized for cache: " + cctx.namexx());
            }

            if (!nearSync && isNearEnabled(cctx)) {
                U.warn(log, "Evictions on primary node are not synchronized with near caches on other nodes " +
                    "which provides 2x-3x better performance but may cause data inconsistency (consider changing " +
                    "'nearEvictSynchronized' configuration property).",
                    "Evictions are not synchronized with near caches on other nodes for cache: " + cctx.namexx());
            }
        }
    }

    /** {@inheritDoc} */
    @Override protected void onKernalStart0() throws IgniteCheckedException {
        super.onKernalStart0();

        if (plcEnabled && evictSync && !cctx.isNear()) {
            // Add dummy event to worker.
            DiscoveryEvent evt = cctx.discovery().localJoinEvent();

            backupWorker.addEvent(evt);

            backupWorkerThread = new IgniteThread(backupWorker);
            backupWorkerThread.start();
        }
    }

    /** {@inheritDoc} */
    @Override protected void onKernalStop0(boolean cancel) {
        super.onKernalStop0(cancel);

        // Change stopping first.
        stopping = true;

        busyLock.block();

        try {
            // Stop backup worker.
            if (evictSync && !cctx.isNear() && backupWorker != null) {
                backupWorker.cancel();

                U.join(
                    backupWorkerThread,
                    log);
            }

            // Cancel all active futures.
            for (EvictionFuture fut : futs.values())
                fut.cancel();

            if (log.isDebugEnabled())
                log.debug("Eviction manager stopped on node: " + cctx.nodeId());
        }
        finally {
            stopped = true;

            busyLock.unblock();
        }
    }

    /**
     * @return Current size of evict queue.
     */
    public int evictQueueSize() {
        return bufEvictQ.sizex();
    }

    /**
     * @param nodeId Sender node ID.
     * @param res Response.
     */
    private void processEvictionResponse(UUID nodeId, GridCacheEvictionResponse res) {
        assert nodeId != null;
        assert res != null;

        if (log.isDebugEnabled())
            log.debug("Processing eviction response [node=" + nodeId + ", localNode=" + cctx.nodeId() +
                ", res=" + res + ']');

        if (!enterBusy())
            return;

        try {
            EvictionFuture fut = futs.get(res.futureId());

            if (fut != null)
                fut.onResponse(nodeId, res);
            else if (log.isDebugEnabled())
                log.debug("Eviction future for response is not found [res=" + res + ", node=" + nodeId +
                    ", localNode=" + cctx.nodeId() + ']');
        }
        finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * @return {@code True} if entered busy.
     */
    private boolean enterBusy() {
        if (!busyLock.enterBusy())
            return false;

        if (stopped) {
            busyLock.leaveBusy();

            return false;
        }

        return true;
    }

    /**
     * @param nodeId Sender node ID.
     * @param req Request.
     */
    private void processEvictionRequest(UUID nodeId, GridCacheEvictionRequest req) {
        assert nodeId != null;
        assert req != null;

        if (!enterBusy())
            return;

        try {
            if (req.classError() != null) {
                if (log.isDebugEnabled())
                    log.debug("Class got undeployed during eviction: " + req.classError());

                sendEvictionResponse(nodeId, new GridCacheEvictionResponse(cctx.cacheId(), req.futureId(), true));

                return;
            }

            AffinityTopologyVersion topVer = lockTopology();

            try {
                if (!topVer.equals(req.topologyVersion())) {
                    if (log.isDebugEnabled())
                        log.debug("Topology version is different [locTopVer=" + topVer +
                            ", rmtTopVer=" + req.topologyVersion() + ']');

                    sendEvictionResponse(nodeId,
                        new GridCacheEvictionResponse(cctx.cacheId(), req.futureId(), true));

                    return;
                }

                processEvictionRequest0(nodeId, req);
            }
            finally {
                unlockTopology();
            }
        }
        finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * @param nodeId Sender node ID.
     * @param req Request.
     */
    private void processEvictionRequest0(UUID nodeId, GridCacheEvictionRequest req) {
        if (log.isDebugEnabled())
            log.debug("Processing eviction request [node=" + nodeId + ", localNode=" + cctx.nodeId() +
                ", reqSize=" + req.entries().size() + ']');

        // Partition -> {{Key, Version}, ...}.
        // Group DHT and replicated cache entries by their partitions.
        Map<Integer, Collection<CacheEvictionEntry>> dhtEntries = new HashMap<>();

        Collection<CacheEvictionEntry> nearEntries = new LinkedList<>();

        for (CacheEvictionEntry e : req.entries()) {
            boolean near = e.near();

            if (!near) {
                // Lock is required.
                Collection<CacheEvictionEntry> col =
                    F.addIfAbsent(dhtEntries, cctx.affinity().partition(e.key()),
                        new LinkedList<CacheEvictionEntry>());

                assert col != null;

                col.add(e);
            }
            else
                nearEntries.add(e);
        }

        GridCacheEvictionResponse res = new GridCacheEvictionResponse(cctx.cacheId(), req.futureId());

        GridCacheVersion obsoleteVer = cctx.versions().next();

        // DHT and replicated cache entries.
        for (Map.Entry<Integer, Collection<CacheEvictionEntry>> e : dhtEntries.entrySet()) {
            int part = e.getKey();

            boolean locked = lockPartition(part); // Will return false if preloading is disabled.

            try {
                for (CacheEvictionEntry t : e.getValue()) {
                    KeyCacheObject key = t.key();
                    GridCacheVersion ver = t.version();
                    boolean near = t.near();

                    assert !near;

                    boolean evicted = evictLocally(key, ver, near, obsoleteVer);

                    if (log.isDebugEnabled())
                        log.debug("Evicted key [key=" + key + ", ver=" + ver + ", near=" + near +
                            ", evicted=" + evicted +']');

                    if (locked && evicted)
                        // Preloading is in progress, we need to save eviction info.
                        saveEvictionInfo(key, ver, part);

                    if (!evicted)
                        res.addRejected(key);
                }
            }
            finally {
                if (locked)
                    unlockPartition(part);
            }
        }

        // Near entries.
        for (CacheEvictionEntry t : nearEntries) {
            KeyCacheObject key = t.key();
            GridCacheVersion ver = t.version();
            boolean near = t.near();

            assert near;

            boolean evicted = evictLocally(key, ver, near, obsoleteVer);

            if (log.isDebugEnabled())
                log.debug("Evicted key [key=" + key + ", ver=" + ver + ", near=" + near +
                    ", evicted=" + evicted +']');

            if (!evicted)
                res.addRejected(key);
        }

        sendEvictionResponse(nodeId, res);
    }

    /**
     * @param nodeId Node ID.
     * @param res Response.
     */
    private void sendEvictionResponse(UUID nodeId, GridCacheEvictionResponse res) {
        try {
            cctx.io().send(nodeId, res, cctx.ioPolicy());

            if (log.isDebugEnabled())
                log.debug("Sent eviction response [node=" + nodeId + ", localNode=" + cctx.nodeId() +
                    ", res" + res + ']');
        }
        catch (ClusterTopologyCheckedException ignored) {
            if (log.isDebugEnabled())
                log.debug("Failed to send eviction response since initiating node left grid " +
                    "[node=" + nodeId + ", localNode=" + cctx.nodeId() + ']');
        }
        catch (IgniteCheckedException e) {
            U.error(log, "Failed to send eviction response to node [node=" + nodeId +
                ", localNode=" + cctx.nodeId() + ", res" + res + ']', e);
        }
    }

    /**
     * @param key Key.
     * @param ver Version.
     * @param p Partition ID.
     */
    private void saveEvictionInfo(KeyCacheObject key, GridCacheVersion ver, int p) {
        assert cctx.rebalanceEnabled();

        if (!cctx.isNear()) {
            try {
                GridDhtLocalPartition part = cctx.dht().topology().localPartition(p,
                    AffinityTopologyVersion.NONE, false);

                assert part != null;

                part.onEntryEvicted(key, ver);
            }
            catch (GridDhtInvalidPartitionException ignored) {
                if (log.isDebugEnabled())
                    log.debug("Partition does not belong to local node [part=" + p +
                        ", nodeId" + cctx.localNode().id() + ']');
            }
        }
        else
            assert false : "Failed to save eviction info: " + cctx.namexx();
    }

    /**
     * @param p Partition ID.
     * @return {@code True} if partition has been actually locked,
     *      {@code false} if preloading is finished or disabled and no lock is needed.
     */
    private boolean lockPartition(int p) {
        if (!cctx.rebalanceEnabled())
            return false;

        if (!cctx.isNear()) {
            try {
                GridDhtLocalPartition part = cctx.dht().topology().localPartition(p, AffinityTopologyVersion.NONE,
                    false);

                if (part != null && part.reserve()) {
                    part.lock();

                    if (part.state() != MOVING) {
                        part.unlock();

                        part.release();

                        return false;
                    }

                    return true;
                }
            }
            catch (GridDhtInvalidPartitionException ignored) {
                if (log.isDebugEnabled())
                    log.debug("Partition does not belong to local node [part=" + p +
                        ", nodeId" + cctx.localNode().id() + ']');
            }
        }

        // No lock is needed.
        return false;
    }

    /**
     * @param p Partition ID.
     */
    private void unlockPartition(int p) {
        if (!cctx.rebalanceEnabled())
            return;

        if (!cctx.isNear()) {
            try {
                GridDhtLocalPartition part = cctx.dht().topology().localPartition(p, AffinityTopologyVersion.NONE,
                    false);

                if (part != null) {
                    part.unlock();

                    part.release();
                }
            }
            catch (GridDhtInvalidPartitionException ignored) {
                if (log.isDebugEnabled())
                    log.debug("Partition does not belong to local node [part=" + p +
                        ", nodeId" + cctx.localNode().id() + ']');
            }
        }
    }

    /**
     * Locks topology (for DHT cache only) and returns its version.
     *
     * @return Topology version after lock.
     */
    private AffinityTopologyVersion lockTopology() {
        if (!cctx.isNear()) {
            cctx.dht().topology().readLock();

            return cctx.dht().topology().topologyVersion();
        }

        return AffinityTopologyVersion.ZERO;
    }

    /**
     * Unlocks topology.
     */
    private void unlockTopology() {
        if (!cctx.isNear())
            cctx.dht().topology().readUnlock();
    }

    /**
     * @param key Key to evict.
     * @param ver Entry version on initial node.
     * @param near {@code true} if entry should be evicted from near cache.
     * @param obsoleteVer Obsolete version.
     * @return {@code true} if evicted successfully, {@code false} if could not be evicted.
     */
    private boolean evictLocally(KeyCacheObject key,
        final GridCacheVersion ver,
        boolean near,
        GridCacheVersion obsoleteVer)
    {
        assert key != null;
        assert ver != null;
        assert obsoleteVer != null;
        assert evictSyncAgr;
        assert !cctx.isNear() || cctx.isReplicated();

        if (log.isDebugEnabled())
            log.debug("Evicting key locally [key=" + key + ", ver=" + ver + ", obsoleteVer=" + obsoleteVer +
                ", localNode=" + cctx.localNode() + ']');

        GridCacheAdapter cache = near ? cctx.dht().near() : cctx.cache();

        GridCacheEntryEx entry = cache.peekEx(key);

        if (entry == null)
            return true;

        try {
            // If entry should be evicted from near cache it can be done safely
            // without any consistency risks. We don't use filter in this case.
            // If entry should be evicted from DHT cache, we do not compare versions
            // as well because versions may change outside the transaction.
            return evict0(cache, entry, obsoleteVer, null, false);
        }
        catch (IgniteCheckedException e) {
            U.error(log, "Failed to evict entry on remote node [key=" + key + ", localNode=" + cctx.nodeId() + ']', e);

            return false;
        }
    }

    /**
     * @param cache Cache from which to evict entry.
     * @param entry Entry to evict.
     * @param obsoleteVer Obsolete version.
     * @param filter Filter.
     * @param explicit If eviction is initiated by user.
     * @return {@code true} if entry has been evicted.
     * @throws IgniteCheckedException If failed to evict entry.
     */
    private boolean evict0(
        GridCacheAdapter cache,
        GridCacheEntryEx entry,
        GridCacheVersion obsoleteVer,
        @Nullable CacheEntryPredicate[] filter,
        boolean explicit
    ) throws IgniteCheckedException {
        assert cache != null;
        assert entry != null;
        assert obsoleteVer != null;

        boolean recordable = cctx.events().isRecordable(EVT_CACHE_ENTRY_EVICTED);

        CacheObject oldVal = recordable ? entry.rawGet() : null;

        boolean hasVal = recordable && entry.hasValue();

        boolean evicted = entry.evictInternal(cctx.isSwapOrOffheapEnabled(), obsoleteVer, filter);

        if (evicted) {
            // Remove manually evicted entry from policy.
            if (explicit && plcEnabled)
                notifyPolicy(entry);

            cache.removeEntry(entry);

            if (cache.configuration().isStatisticsEnabled())
                cache.metrics0().onEvict();

            if (recordable)
                cctx.events().addEvent(entry.partition(), entry.key(), cctx.nodeId(), (IgniteUuid)null, null,
                    EVT_CACHE_ENTRY_EVICTED, null, false, oldVal, hasVal, null, null, null, false);

            if (log.isDebugEnabled())
                log.debug("Entry was evicted [entry=" + entry + ", localNode=" + cctx.nodeId() + ']');
        }
        else {
            if (log.isDebugEnabled())
                log.debug("Entry was not evicted [entry=" + entry + ", localNode=" + cctx.nodeId() + ']');
        }

        return evicted;
    }

    /**
     * @param txEntry Transactional entry.
     */
    public void touch(IgniteTxEntry txEntry, boolean loc) {
        if (!plcEnabled && memoryMode != OFFHEAP_TIERED)
            return;

        if (!loc) {
            if (cctx.isNear())
                return;

            if (evictSync)
                return;
        }

        GridCacheEntryEx e = txEntry.cached();

        if (e.detached() || e.isInternal())
            return;

        try {
            if (e.markObsoleteIfEmpty(null) || e.obsolete())
                e.context().cache().removeEntry(e);
        }
        catch (IgniteCheckedException ex) {
            U.error(log, "Failed to evict entry from cache: " + e, ex);
        }

        if (memoryMode == OFFHEAP_TIERED) {
            try {
                evict0(cctx.cache(), e, cctx.versions().next(), null, false);
            }
            catch (IgniteCheckedException ex) {
                U.error(log, "Failed to evict entry from on heap memory: " + e, ex);
            }
        }
        else {
            notifyPolicy(e);

            if (evictSyncAgr)
                waitForEvictionFutures();
        }
    }

    /**
     * @param e Entry for eviction policy notification.
     * @param topVer Topology version.
     */
    public void touch(GridCacheEntryEx e, AffinityTopologyVersion topVer) {
        if (e.detached() || e.isInternal())
            return;

        try {
            if (e.markObsoleteIfEmpty(null) || e.obsolete())
                e.context().cache().removeEntry(e);
        }
        catch (IgniteCheckedException ex) {
            U.error(log, "Failed to evict entry from cache: " + e, ex);
        }

        if (!cctx.isNear() && memoryMode == OFFHEAP_TIERED) {
            try {
                evict0(cctx.cache(), e, cctx.versions().next(), null, false);
            }
            catch (IgniteCheckedException ex) {
                U.error(log, "Failed to evict entry from on heap memory: " + e, ex);
            }

            return;
        }

        if (!plcEnabled)
            return;

        // Don't track non-primary entries if evicts are synchronized.
        if (!cctx.isNear() && evictSync && !cctx.affinity().primaryByPartition(cctx.localNode(), e.partition(), topVer))
            return;

        if (!enterBusy())
            return;

        try {
            // Wait for futures to finish.
            if (evictSyncAgr)
                waitForEvictionFutures();

            if (log.isDebugEnabled())
                log.debug("Touching entry [entry=" + e + ", localNode=" + cctx.nodeId() + ']');

            notifyPolicy(e);
        }
        finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * @param e Entry for eviction policy notification.
     */
    private void touch0(GridCacheEntryEx e) {
        assert evictSyncAgr;
        assert plcEnabled;

        // Do not wait for futures here since only limited number
        // of entries can be passed to this method.
        notifyPolicy(e);
    }

    /**
     * @param entries Entries for eviction policy notification.
     */
    private void touchOnTopologyChange(Iterable<? extends GridCacheEntryEx> entries) {
        assert evictSync;
        assert plcEnabled;

        if (log.isDebugEnabled())
            log.debug("Touching entries [entries=" + entries + ", localNode=" + cctx.nodeId() + ']');

        for (GridCacheEntryEx e : entries) {
            if (e.key() instanceof GridCacheInternal)
                // Skip internal entry.
                continue;

            // Do not wait for futures here since only limited number
            // of entries can be passed to this method.
            notifyPolicy(e);
        }
    }

    /**
     * Warns on first eviction.
     */
    private void warnFirstEvict() {
        // Do not move warning output to synchronized block (it causes warning in IDE).
        synchronized (this) {
            if (firstEvictWarn)
                return;

            firstEvictWarn = true;
        }

        U.warn(log, "Evictions started (cache may have reached its capacity)." +
            " You may wish to increase 'maxSize' on eviction policy being used for cache: " + cctx.name(),
            "Evictions started (cache may have reached its capacity): " + cctx.name());
    }

    /**
     * @return {@code True} if either evicts or near evicts are synchronized, {@code false} otherwise.
     */
    public boolean evictSyncOrNearSync() {
        return evictSyncAgr;
    }

    /**
     * @param entry Entry to attempt to evict.
     * @param obsoleteVer Obsolete version.
     * @param filter Optional entry filter.
     * @param explicit {@code True} if evict is called explicitly, {@code false} if it's called
     *      from eviction policy.
     * @return {@code True} if entry was marked for eviction.
     * @throws IgniteCheckedException In case of error.
     */
    public boolean evict(@Nullable GridCacheEntryEx entry, @Nullable GridCacheVersion obsoleteVer,
        boolean explicit, @Nullable CacheEntryPredicate[] filter) throws IgniteCheckedException {
        if (entry == null)
            return true;

        // Do not evict internal entries.
        if (entry.key() instanceof GridCacheInternal)
            return false;

        if (!cctx.isNear() && !explicit && !firstEvictWarn)
            warnFirstEvict();

        if (evictSyncAgr) {
            assert !cctx.isNear(); // Make sure cache is not NEAR.

            if (cctx.affinity().backupsByKey(
                    entry.key(),
                    cctx.topology().topologyVersion()).contains(cctx.localNode()) &&
                evictSync)
                // Do not track backups if evicts are synchronized.
                return !explicit;

            try {
                if (!cctx.isAll(entry, filter))
                    return false;

                if (entry.lockedByAny())
                    return false;

                // Add entry to eviction queue.
                enqueue(entry, filter);
            }
            catch (GridCacheEntryRemovedException ignored) {
                if (log.isDebugEnabled())
                    log.debug("Entry got removed while evicting [entry=" + entry +
                        ", localNode=" + cctx.nodeId() + ']');
            }
        }
        else {
            if (obsoleteVer == null)
                obsoleteVer = cctx.versions().next();

            // Do not touch entry if not evicted:
            // 1. If it is call from policy, policy tracks it on its own.
            // 2. If it is explicit call, entry is touched on tx commit.
            return evict0(cctx.cache(), entry, obsoleteVer, filter, explicit);
        }

        return true;
    }

    /**
     * @param keys Keys to evict.
     * @param obsoleteVer Obsolete version.
     * @throws IgniteCheckedException In case of error.
     */
    public void batchEvict(Collection<?> keys, @Nullable GridCacheVersion obsoleteVer) throws IgniteCheckedException {
        assert !evictSyncAgr;
        assert cctx.isSwapOrOffheapEnabled();

        List<GridCacheEntryEx> locked = new ArrayList<>(keys.size());

        Set<GridCacheEntryEx> notRmv = null;

        Collection<GridCacheBatchSwapEntry> swapped = new ArrayList<>(keys.size());

        boolean recordable = cctx.events().isRecordable(EVT_CACHE_ENTRY_EVICTED);

        GridCacheAdapter cache = cctx.cache();

        Map<Object, GridCacheEntryEx> cached = U.newLinkedHashMap(keys.size());

        // Get all participating entries to avoid deadlock.
        for (Object k : keys) {
            KeyCacheObject cacheKey = cctx.toCacheKeyObject(k);

            GridCacheEntryEx e = cache.peekEx(cacheKey);

            if (e != null)
                cached.put(k, e);
        }

        try {
            for (GridCacheEntryEx entry : cached.values()) {
                // Do not evict internal entries.
                if (entry.key().internal())
                    continue;

                // Lock entry.
                GridUnsafe.monitorEnter(entry);

                locked.add(entry);

                if (entry.obsolete()) {
                    if (notRmv == null)
                        notRmv = new HashSet<>();

                    notRmv.add(entry);

                    continue;
                }

                if (obsoleteVer == null)
                    obsoleteVer = cctx.versions().next();

                GridCacheBatchSwapEntry swapEntry = entry.evictInBatchInternal(obsoleteVer);

                if (swapEntry != null) {
                    assert entry.obsolete() : entry;

                    swapped.add(swapEntry);

                    if (log.isDebugEnabled())
                        log.debug("Entry was evicted [entry=" + entry + ", localNode=" + cctx.nodeId() + ']');
                }
                else if (!entry.obsolete()) {
                    if (notRmv == null)
                        notRmv = new HashSet<>();

                    notRmv.add(entry);
                }
            }

            // Batch write to swap.
            if (!swapped.isEmpty())
                cctx.swap().writeAll(swapped);
        }
        finally {
            // Unlock entries in reverse order.
            for (ListIterator<GridCacheEntryEx> it = locked.listIterator(locked.size()); it.hasPrevious();) {
                GridCacheEntryEx e = it.previous();

                GridUnsafe.monitorExit(e);
            }

            // Remove entries and fire events outside the locks.
            for (GridCacheEntryEx entry : locked) {
                if (entry.obsolete() && (notRmv == null || !notRmv.contains(entry))) {
                    entry.onMarkedObsolete();

                    cache.removeEntry(entry);

                    if (plcEnabled)
                        notifyPolicy(entry);

                    if (recordable)
                        cctx.events().addEvent(entry.partition(), entry.key(), cctx.nodeId(), (IgniteUuid)null, null,
                            EVT_CACHE_ENTRY_EVICTED, null, false, entry.rawGet(), entry.hasValue(), null, null, null,
                            false);
                }
            }
        }
    }

    /**
     * Enqueues entry for synchronized eviction.
     *
     * @param entry Entry.
     * @param filter Filter.
     * @throws GridCacheEntryRemovedException If entry got removed.
     */
    private void enqueue(GridCacheEntryEx entry, CacheEntryPredicate[] filter)
        throws GridCacheEntryRemovedException {
        Node<EvictionInfo> node = entry.meta(META_KEY);

        if (node == null) {
            node = bufEvictQ.addLastx(new EvictionInfo(entry, entry.version(), filter));

            if (entry.putMetaIfAbsent(META_KEY, node) != null)
                // Was concurrently added, need to clear it from queue.
                bufEvictQ.unlinkx(node);
            else if (log.isDebugEnabled())
                log.debug("Added entry to eviction queue: " + entry);
        }
    }

    /**
     * Checks eviction queue.
     */
    private void checkEvictionQueue() {
        int maxSize = maxQueueSize();

        int bufSize = bufEvictQ.sizex();

        if (bufSize >= maxSize) {
            if (log.isDebugEnabled())
                log.debug("Processing eviction queue: " + bufSize);

            Collection<EvictionInfo> evictInfos = new ArrayList<>(bufSize);

            for (int i = 0; i < bufSize; i++) {
                EvictionInfo info = bufEvictQ.poll();

                if (info == null)
                    break;

                evictInfos.add(info);
            }

            if (!evictInfos.isEmpty())
                addToCurrentFuture(evictInfos);
        }
    }

    /**
     * @return Max queue size.
     */
    private int maxQueueSize() {
        int size = (int)(cctx.cache().size() * cctx.config().getEvictMaxOverflowRatio()) / 100;

        if (size <= 0)
            size = 500;

        return Math.min(size, cctx.config().getEvictSynchronizedKeyBufferSize());
    }

    /**
     * Processes eviction queue (sends required requests, etc.).
     *
     * @param evictInfos Eviction information to create future with.
     */
    private void addToCurrentFuture(Collection<EvictionInfo> evictInfos) {
        assert !evictInfos.isEmpty();

        while (true) {
            EvictionFuture fut = curEvictFut.get();

            if (fut == null) {
                curEvictFut.compareAndSet(null, new EvictionFuture());

                continue;
            }

            if (fut.prepareLock()) {
                boolean added;

                try {
                    added = fut.add(evictInfos);
                }
                finally {
                    fut.prepareUnlock();
                }

                if (added) {
                    if (fut.prepare()) {
                        // Thread that prepares future should remove it and install listener.
                        curEvictFut.compareAndSet(fut, null);

                        fut.listen(new CI1<IgniteInternalFuture<?>>() {
                            @Override public void apply(IgniteInternalFuture<?> f) {
                                if (!enterBusy()) {
                                    if (log.isDebugEnabled())
                                        log.debug("Will not notify eviction future completion (grid is stopping): " +
                                            f);

                                    return;
                                }

                                try {
                                    AffinityTopologyVersion topVer = lockTopology();

                                    try {
                                        onFutureCompleted((EvictionFuture)f, topVer);
                                    }
                                    finally {
                                        unlockTopology();
                                    }
                                }
                                finally {
                                    busyLock.leaveBusy();
                                }
                            }
                        });
                    }

                    break;
                }
                else
                    // Infos were not added, create another future for next iteration.
                    curEvictFut.compareAndSet(fut, new EvictionFuture());
            }
            else
                // Future has not been locked, create another future for next iteration.
                curEvictFut.compareAndSet(fut, new EvictionFuture());
        }
    }

    /**
     * @param fut Completed eviction future.
     * @param topVer Topology version on future complete.
     */
    private void onFutureCompleted(EvictionFuture fut, AffinityTopologyVersion topVer) {
        if (!enterBusy())
            return;

        try {
            IgniteBiTuple<Collection<EvictionInfo>, Collection<EvictionInfo>> t;

            try {
                t = fut.get();
            }
            catch (IgniteFutureCancelledCheckedException ignored) {
                assert false : "Future has been cancelled, but manager is not stopping: " + fut;

                return;
            }
            catch (IgniteCheckedException e) {
                U.error(log, "Eviction future finished with error (all entries will be touched): " + fut, e);

                if (plcEnabled) {
                    for (EvictionInfo info : fut.entries())
                        touch0(info.entry());
                }

                return;
            }

            // Check if topology version is different.
            if (!fut.topologyVersion().equals(topVer)) {
                if (log.isDebugEnabled())
                    log.debug("Topology has changed, all entries will be touched: " + fut);

                if (plcEnabled) {
                    for (EvictionInfo info : fut.entries())
                        touch0(info.entry());
                }

                return;
            }

            // Evict remotely evicted entries.
            GridCacheVersion obsoleteVer = null;

            Collection<EvictionInfo> evictedEntries = t.get1();

            for (EvictionInfo info : evictedEntries) {
                GridCacheEntryEx entry = info.entry();

                try {
                    // Remove readers on which the entry was evicted.
                    for (IgniteBiTuple<ClusterNode, Long> r : fut.evictedReaders(entry.key())) {
                        UUID readerId = r.get1().id();
                        Long msgId = r.get2();

                        ((GridDhtCacheEntry)entry).removeReader(readerId, msgId);
                    }

                    if (obsoleteVer == null)
                        obsoleteVer = cctx.versions().next();

                    // Do not touch primary entries, if not evicted.
                    // They will be touched within updating transactions.
                    evict0(cctx.cache(), entry, obsoleteVer, versionFilter(info), false);
                }
                catch (IgniteCheckedException e) {
                    U.error(log, "Failed to evict entry [entry=" + entry +
                        ", localNode=" + cctx.nodeId() + ']', e);
                }
                catch (GridCacheEntryRemovedException ignored) {
                    if (log.isDebugEnabled())
                        log.debug("Entry was concurrently removed while evicting [entry=" + entry +
                            ", localNode=" + cctx.nodeId() + ']');
                }
            }

            Collection<EvictionInfo> rejectedEntries = t.get2();

            // Touch remotely rejected entries (only if policy is enabled).
            if (plcEnabled && !rejectedEntries.isEmpty()) {
                for (EvictionInfo info : rejectedEntries)
                    touch0(info.entry());
            }
        }
        finally {
            busyLock.leaveBusy();

            // Signal on future completion.
            signal();
        }
    }

    /**
     * This method should be called when eviction future is processed
     * and unwind may continue.
     */
    private void signal() {
        futsCntLock.lock();

        try {
            // Avoid volatile read on assertion.
            int cnt = --activeFutsCnt;

            assert cnt >= 0 : "Invalid futures count: " + cnt;

            if (cnt < maxActiveFuts)
                futsCntCond.signalAll();
        }
        finally {
            futsCntLock.unlock();
        }
    }

    /**
     * @param info Eviction info.
     * @return Version aware filter.
     */
    private CacheEntryPredicate[] versionFilter(final EvictionInfo info) {
        // If version has changed since we started the whole process
        // then we should not evict entry.
        return new CacheEntryPredicate[]{new CacheEntryPredicateAdapter() {
            @Override public boolean apply(GridCacheEntryEx e) {
                try {
                    GridCacheVersion ver = e.version();

                    return info.version().equals(ver) && F.isAll(info.filter());
                }
                catch (GridCacheEntryRemovedException ignored) {
                    return false;
                }
            }
        }};
    }

    /**
     * Gets a collection of nodes to send eviction requests to.
     *
     *
     * @param entry Entry.
     * @param topVer Topology version.
     * @return Tuple of two collections: dht (in case of partitioned cache) nodes
     *      and readers (empty for replicated cache).
     * @throws GridCacheEntryRemovedException If entry got removed during method
     *      execution.
     */
    @SuppressWarnings( {"IfMayBeConditional"})
    private IgniteBiTuple<Collection<ClusterNode>, Collection<ClusterNode>> remoteNodes(GridCacheEntryEx entry,
        AffinityTopologyVersion topVer)
        throws GridCacheEntryRemovedException {
        assert entry != null;

        assert cctx.config().getCacheMode() != LOCAL;

        Collection<ClusterNode> backups;

        if (evictSync)
            backups = F.view(cctx.dht().topology().nodes(entry.partition(), topVer), F0.notEqualTo(cctx.localNode()));
        else
            backups = Collections.emptySet();

        Collection<ClusterNode> readers;

        if (nearSync) {
            readers = F.transform(((GridDhtCacheEntry)entry).readers(), new C1<UUID, ClusterNode>() {
                @Nullable @Override public ClusterNode apply(UUID nodeId) {
                    return cctx.node(nodeId);
                }
            });
        }
        else
            readers = Collections.emptySet();

        return new IgnitePair<>(backups, readers);
    }

    /**
     * Notifications.
     */
    public void unwind() {
        if (!evictSyncAgr)
            return;

        if (!enterBusy())
            return;

        try {
            checkEvictionQueue();
        }
        finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * @param e Entry to notify eviction policy.
     */
    @SuppressWarnings({"IfMayBeConditional", "RedundantIfStatement"})
    private void notifyPolicy(GridCacheEntryEx e) {
        assert plcEnabled;
        assert plc != null;
        assert !e.isInternal() : "Invalid entry for policy notification: " + e;

        if (log.isDebugEnabled())
            log.debug("Notifying eviction policy with entry: " + e);

        if (filter == null || filter.evictAllowed(e.wrapLazyValue(cctx.keepBinary())))
            plc.onEntryAccessed(e.obsoleteOrDeleted(), e.wrapEviction());
    }

    /**
     *
     */
    @SuppressWarnings("TooBroadScope")
    private void waitForEvictionFutures() {
        if (activeFutsCnt >= maxActiveFuts) {
            boolean interrupted = false;

            futsCntLock.lock();

            try {
                while(!stopping && activeFutsCnt >= maxActiveFuts) {
                    try {
                        futsCntCond.await(2000, MILLISECONDS);
                    }
                    catch (InterruptedException ignored) {
                        interrupted = true;
                    }
                }
            }
            finally {
                futsCntLock.unlock();

                if (interrupted)
                    Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * Prints out eviction stats.
     */
    public void printStats() {
        X.println("Eviction stats [grid=" + cctx.gridName() + ", cache=" + cctx.cache().name() +
            ", buffEvictQ=" + bufEvictQ.sizex() + ']');
    }

    /** {@inheritDoc} */
    @Override public void printMemoryStats() {
        X.println(">>> ");
        X.println(">>> Eviction manager memory stats [grid=" + cctx.gridName() + ", cache=" + cctx.name() + ']');
        X.println(">>>   buffEvictQ size: " + bufEvictQ.sizex());
        X.println(">>>   futsSize: " + futs.size());
        X.println(">>>   futsCreated: " + idGen.get());
    }

    /**
     *
     */
    private class BackupWorker extends GridWorker {
        /** */
        private final BlockingQueue<DiscoveryEvent> evts = new LinkedBlockingQueue<>();

        /** */
        private final Collection<Integer> primaryParts = new HashSet<>();

        /**
         *
         */
        private BackupWorker() {
            super(cctx.gridName(), "cache-eviction-backup-worker", GridCacheEvictionManager.this.log);

            assert plcEnabled;
        }

        /**
         * @param evt New event.
         */
        void addEvent(DiscoveryEvent evt) {
            assert evt != null;

            evts.add(evt);
        }

        /** {@inheritDoc} */
        @Override protected void body() throws InterruptedException, IgniteInterruptedCheckedException {
            try {
                assert !cctx.isNear() && evictSync;

                ClusterNode loc = cctx.localNode();

                AffinityTopologyVersion initTopVer =
                    new AffinityTopologyVersion(cctx.discovery().localJoinEvent().topologyVersion(), 0);

                AffinityTopologyVersion cacheStartVer = cctx.startTopologyVersion();

                if (cacheStartVer != null && cacheStartVer.compareTo(initTopVer) > 0)
                    initTopVer = cacheStartVer;

                // Initialize.
                primaryParts.addAll(cctx.affinity().primaryPartitions(cctx.localNodeId(), initTopVer));

                while (!isCancelled()) {
                    DiscoveryEvent evt = evts.take();

                    if (log.isDebugEnabled())
                        log.debug("Processing event: " + evt);

                    AffinityTopologyVersion topVer = new AffinityTopologyVersion(evt.topologyVersion());

                    // Remove partitions that are no longer primary.
                    for (Iterator<Integer> it = primaryParts.iterator(); it.hasNext();) {
                        if (!evts.isEmpty())
                            break;

                        if (!cctx.affinity().primaryByPartition(loc, it.next(), topVer))
                            it.remove();
                    }

                    // Move on to next event.
                    if (!evts.isEmpty())
                        continue;

                    for (GridDhtLocalPartition part : cctx.topology().localPartitions()) {
                        if (!evts.isEmpty())
                            break;

                        if (part.primary(topVer) && primaryParts.add(part.id())) {
                            if (log.isDebugEnabled())
                                log.debug("Touching partition entries: " + part);

                            touchOnTopologyChange(part.allEntries());
                        }
                    }
                }
            }
            catch (InterruptedException ignored) {
                // No-op.
            }
            catch (IgniteException e) {
                if (!e.hasCause(InterruptedException.class))
                    throw e;
            }
        }
    }

    /**
     * Wrapper around an entry to be put into queue.
     */
    private class EvictionInfo {
        /** Cache entry. */
        private GridCacheEntryEx entry;

        /** Start version. */
        private GridCacheVersion ver;

        /** Filter to pass before entry will be evicted. */
        private CacheEntryPredicate[] filter;

        /**
         * @param entry Entry.
         * @param ver Version.
         * @param filter Filter.
         */
        EvictionInfo(GridCacheEntryEx entry, GridCacheVersion ver,
            CacheEntryPredicate[] filter) {
            assert entry != null;
            assert ver != null;

            this.entry = entry;
            this.ver = ver;
            this.filter = filter;
        }

        /**
         * @return Entry.
         */
        GridCacheEntryEx entry() {
            return entry;
        }

        /**
         * @return Version.
         */
        GridCacheVersion version() {
            return ver;
        }

        /**
         * @return Filter.
         */
        CacheEntryPredicate[] filter() {
            return filter;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(EvictionInfo.class, this);
        }
    }

    /**
     * Future for synchronized eviction. Result is a tuple: {evicted entries, rejected entries}.
     */
    private class EvictionFuture extends GridFutureAdapter<IgniteBiTuple<Collection<EvictionInfo>,
        Collection<EvictionInfo>>> {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private final long id = idGen.incrementAndGet();

        /** */
        private ConcurrentLinkedDeque8<EvictionInfo> evictInfos = new ConcurrentLinkedDeque8<>();

        /** */
        private final ConcurrentMap<KeyCacheObject, EvictionInfo> entries = new ConcurrentHashMap8<>();

        /** */
        private final ConcurrentMap<KeyCacheObject, Collection<ClusterNode>> readers =
            new ConcurrentHashMap8<>();

        /** */
        private final Collection<EvictionInfo> evictedEntries = new GridConcurrentHashSet<>();

        /** */
        private final ConcurrentMap<KeyCacheObject, EvictionInfo> rejectedEntries = new ConcurrentHashMap8<>();

        /** Request map. */
        private final ConcurrentMap<UUID, GridCacheEvictionRequest> reqMap =
            new ConcurrentHashMap8<>();

        /** Response map. */
        private final ConcurrentMap<UUID, GridCacheEvictionResponse> resMap =
            new ConcurrentHashMap8<>();

        /** To make sure that future is completing within a single thread. */
        private final AtomicBoolean finishPrepare = new AtomicBoolean();

        /** Lock to use when future is being initialized. */
        @GridToStringExclude
        private final ReadWriteLock prepareLock = new ReentrantReadWriteLock();

        /** To make sure that future is completing within a single thread. */
        private final AtomicBoolean completing = new AtomicBoolean();

        /** Lock to use after future is initialized. */
        @GridToStringExclude
        private final ReadWriteLock lock = new ReentrantReadWriteLock();

        /** Object to force future completion on elapsing eviction timeout. */
        @GridToStringExclude
        private GridTimeoutObject timeoutObj;

        /** Topology version future is processed on. */
        private AffinityTopologyVersion topVer = AffinityTopologyVersion.ZERO;

        /**
         * @return {@code True} if prepare lock was acquired.
         */
        boolean prepareLock() {
            return prepareLock.readLock().tryLock();
        }

        /**
         *
         */
        void prepareUnlock() {
            prepareLock.readLock().unlock();
        }

        /**
         * @param infos Infos to add.
         * @return {@code False} if entries were not added due to capacity restrictions.
         */
        boolean add(Collection<EvictionInfo> infos) {
            assert infos != null && !infos.isEmpty();

            if (evictInfos.sizex() > maxQueueSize())
                return false;

            evictInfos.addAll(infos);

            return true;
        }

        /**
         * @return {@code True} if future has been prepared by this call.
         */
        @SuppressWarnings("LockAcquiredButNotSafelyReleased")
        boolean prepare() {
            if (evictInfos.sizex() >= maxQueueSize() && finishPrepare.compareAndSet(false, true)) {
                // Lock will never be released intentionally.
                prepareLock.writeLock().lock();

                futsCntLock.lock();

                try {
                    activeFutsCnt++;
                }
                finally {
                    futsCntLock.unlock();
                }

                // Put future in map.
                futs.put(id, this);

                prepare0();

                return true;
            }

            return false;
        }

        /**
         * Prepares future (sends all required requests).
         */
        private void prepare0() {
            if (log.isDebugEnabled())
                log.debug("Preparing eviction future [futId=" + id + ", localNode=" + cctx.nodeId() +
                    ", infos=" + evictInfos + ']');

            assert evictInfos != null && !evictInfos.isEmpty();

            topVer = lockTopology();

            try {
                Collection<EvictionInfo> locals = null;

                for (EvictionInfo info : evictInfos) {
                    // Queue node may have been stored in entry metadata concurrently, but we don't care
                    // about it since we are currently processing this entry.
                    Node<EvictionInfo> queueNode = info.entry().removeMeta(META_KEY);

                    if (queueNode != null)
                        bufEvictQ.unlinkx(queueNode);

                    IgniteBiTuple<Collection<ClusterNode>, Collection<ClusterNode>> tup;

                    try {
                        tup = remoteNodes(info.entry(), topVer);
                    }
                    catch (GridCacheEntryRemovedException ignored) {
                        if (log.isDebugEnabled())
                            log.debug("Entry got removed while preparing eviction future (ignoring) [entry=" +
                                info.entry() + ", nodeId=" + cctx.nodeId() + ']');

                        continue;
                    }

                    Collection<ClusterNode> entryReaders =
                        F.addIfAbsent(readers, info.entry().key(), new GridConcurrentHashSet<ClusterNode>());

                    assert entryReaders != null;

                    // Add entry readers so that we could remove them right before local eviction.
                    entryReaders.addAll(tup.get2());

                    Collection<ClusterNode> nodes = F.concat(true, tup.get1(), tup.get2());

                    if (!nodes.isEmpty()) {
                        entries.put(info.entry().key(), info);

                        // There are remote participants.
                        for (ClusterNode node : nodes) {
                            GridCacheEvictionRequest req = F.addIfAbsent(reqMap, node.id(),
                                new GridCacheEvictionRequest(cctx.cacheId(), id, evictInfos.size(), topVer,
                                    cctx.deploymentEnabled()));

                            assert req != null;

                            req.addKey(info.entry().key(), info.version(), entryReaders.contains(node));
                        }
                    }
                    else {
                        if (locals == null)
                            locals = new HashSet<>(evictInfos.size(), 1.0f);

                        // There are no remote participants, need to keep the entry as local.
                        locals.add(info);
                    }
                }

                if (locals != null) {
                    // Evict entries without remote participant nodes immediately.
                    GridCacheVersion obsoleteVer = cctx.versions().next();

                    for (EvictionInfo info : locals) {
                        if (log.isDebugEnabled())
                            log.debug("Evicting key without remote participant nodes: " + info);

                        try {
                            // Touch primary entry (without backup nodes) if not evicted
                            // to keep tracking.
                            if (!evict0(cctx.cache(), info.entry(), obsoleteVer, versionFilter(info), false) &&
                                plcEnabled)
                                touch0(info.entry());
                        }
                        catch (IgniteCheckedException e) {
                            U.error(log, "Failed to evict entry: " + info.entry(), e);
                        }
                    }
                }

                // If there were only local entries.
                if (entries.isEmpty()) {
                    complete(false);

                    return;
                }
            }
            finally {
                unlockTopology();
            }

            // Send eviction requests.
            for (Map.Entry<UUID, GridCacheEvictionRequest> e : reqMap.entrySet()) {
                UUID nodeId = e.getKey();

                GridCacheEvictionRequest req = e.getValue();

                if (log.isDebugEnabled())
                    log.debug("Sending eviction request [node=" + nodeId + ", req=" + req + ']');

                try {
                    cctx.io().send(nodeId, req, cctx.ioPolicy());
                }
                catch (ClusterTopologyCheckedException ignored) {
                    // Node left the topology.
                    onNodeLeft(nodeId);
                }
                catch (IgniteCheckedException ex) {
                    U.error(log, "Failed to send eviction request to node [node=" + nodeId + ", req=" + req + ']', ex);

                    rejectEntries(nodeId);
                }
            }

            registerTimeoutObject();
        }

        /**
         *
         */
        private void registerTimeoutObject() {
            // Check whether future has not been completed yet.
            if (lock.readLock().tryLock()) {
                try {
                    timeoutObj = new GridTimeoutObjectAdapter(cctx.config().getEvictSynchronizedTimeout()) {
                        @Override public void onTimeout() {
                            complete(true);
                        }
                    };

                    cctx.time().addTimeoutObject(timeoutObj);
                }
                finally {
                    lock.readLock().unlock();
                }
            }
        }

        /**
         * @return Future ID.
         */
        long id() {
            return id;
        }

        /**
         * @return Topology version.
         */
        AffinityTopologyVersion topologyVersion() {
            return topVer;
        }

        /**
         * @return Keys to readers mapping.
         */
        Map<KeyCacheObject, Collection<ClusterNode>> readers() {
            return readers;
        }

        /**
         * @return All entries associated with future that should be evicted (or rejected).
         */
        Collection<EvictionInfo> entries() {
            return entries.values();
        }

        /**
         * Reject all entries on behalf of specified node.
         *
         * @param nodeId Node ID.
         */
        private void rejectEntries(UUID nodeId) {
            assert nodeId != null;

            if (lock.readLock().tryLock()) {
                try {
                    if (log.isDebugEnabled())
                        log.debug("Rejecting entries for node: " + nodeId);

                    GridCacheEvictionRequest req = reqMap.remove(nodeId);

                    for (CacheEvictionEntry t : req.entries()) {
                        EvictionInfo info = entries.get(t.key());

                        assert info != null;

                        rejectedEntries.put(t.key(), info);
                    }
                }
                finally {
                    lock.readLock().unlock();
                }
            }

            checkDone();
        }

        /**
         * @param nodeId Node id that left the topology.
         */
        void onNodeLeft(UUID nodeId) {
            assert nodeId != null;

            if (lock.readLock().tryLock()) {
                try {
                    // Stop waiting response from this node.
                    reqMap.remove(nodeId);

                    resMap.remove(nodeId);
                }
                finally {
                    lock.readLock().unlock();
                }

                checkDone();
            }
        }

        /**
         * @param nodeId Sender node ID.
         * @param res Response.
         */
        void onResponse(UUID nodeId, GridCacheEvictionResponse res) {
            assert nodeId != null;
            assert res != null;

            if (lock.readLock().tryLock()) {
                try {
                    if (log.isDebugEnabled())
                        log.debug("Entered to eviction future onResponse() [fut=" + this + ", node=" + nodeId +
                            ", res=" + res + ']');

                    ClusterNode node = cctx.node(nodeId);

                    if (node != null)
                        resMap.put(nodeId, res);
                    else
                        // Sender node left grid.
                        reqMap.remove(nodeId);
                }
                finally {
                    lock.readLock().unlock();
                }

                if (res.evictError())
                    // Complete future, since there was a class loading error on at least one node.
                    complete(false);
                else
                    checkDone();
            }
            else {
                if (log.isDebugEnabled())
                    log.debug("Ignored eviction response [fut=" + this + ", node=" + nodeId + ", res=" + res + ']');
            }
        }

        /**
         *
         */
        private void checkDone() {
            if (reqMap.isEmpty() || resMap.keySet().containsAll(reqMap.keySet()))
                complete(false);
        }

        /**
         * Completes future.
         *
         * @param timedOut {@code True} if future is being forcibly completed on timeout.
         */
        @SuppressWarnings({"LockAcquiredButNotSafelyReleased"})
        private void complete(boolean timedOut) {
            if (completing.compareAndSet(false, true)) {
                // Lock will never be released intentionally.
                lock.writeLock().lock();

                futs.remove(id);

                if (timeoutObj != null && !timedOut)
                    // If future is timed out, corresponding object is already removed.
                    cctx.time().removeTimeoutObject(timeoutObj);

                if (log.isDebugEnabled())
                    log.debug("Building eviction future result [fut=" + this + ", timedOut=" + timedOut + ']');

                boolean err = false;

                for (GridCacheEvictionResponse res : resMap.values()) {
                    if (res.evictError()) {
                        err = true;

                        break;
                    }
                }

                if (err) {
                    Collection<UUID> ids = F.view(resMap.keySet(), new P1<UUID>() {
                        @Override public boolean apply(UUID e) {
                            return resMap.get(e).evictError();
                        }
                    });

                    assert !ids.isEmpty();

                    U.warn(log, "Remote node(s) failed to process eviction request " +
                        "due to topology changes " +
                        "(some backup or remote values maybe lost): " + ids);
                }

                if (timedOut)
                    U.warn(log, "Timed out waiting for eviction future " +
                        "(consider changing 'evictSynchronousTimeout' and 'evictSynchronousConcurrencyLevel' " +
                        "configuration parameters).");

                if (err || timedOut) {
                    // Future has not been completed successfully, all entries should be rejected.
                    assert evictedEntries.isEmpty();

                    rejectedEntries.putAll(entries);
                }
                else {
                    // Copy map to filter remotely rejected entries,
                    // as they will be touched within corresponding txs.
                    Map<KeyCacheObject, EvictionInfo> rejectedEntries0 = new HashMap<>(rejectedEntries);

                    // Future has been completed successfully - build result.
                    for (EvictionInfo info : entries.values()) {
                        KeyCacheObject key = info.entry().key();

                        if (rejectedEntries0.containsKey(key))
                            // Was already rejected.
                            continue;

                        boolean rejected = false;

                        for (GridCacheEvictionResponse res : resMap.values()) {
                            if (res.rejectedKeys().contains(key)) {
                                // Modify copied map.
                                rejectedEntries0.put(key, info);

                                rejected = true;

                                break;
                            }
                        }

                        if (!rejected)
                            evictedEntries.add(info);
                    }
                }

                // Pass entries that were rejected due to topology changes
                // or due to timeout or class loading issues.
                // Remotely rejected entries will be touched within corresponding txs.
                onDone(F.t(evictedEntries, rejectedEntries.values()));
            }
        }

        /**
         * @param key Key.
         * @return Reader nodes on which given key was evicted.
         */
        Collection<IgniteBiTuple<ClusterNode, Long>> evictedReaders(KeyCacheObject key) {
            Collection<ClusterNode> mappedReaders = readers.get(key);

            if (mappedReaders == null)
                return Collections.emptyList();

            Collection<IgniteBiTuple<ClusterNode, Long>> col = new LinkedList<>();

            for (Map.Entry<UUID, GridCacheEvictionResponse> e : resMap.entrySet()) {
                ClusterNode node = cctx.node(e.getKey());

                // If node has left or response did not arrive from near node
                // then just skip it.
                if (node == null || !mappedReaders.contains(node))
                    continue;

                GridCacheEvictionResponse res = e.getValue();

                if (!res.rejectedKeys().contains(key))
                    col.add(F.t(node, res.messageId()));
            }

            return col;
        }

        /** {@inheritDoc} */
        @SuppressWarnings("LockAcquiredButNotSafelyReleased")
        @Override public boolean cancel() {
            if (completing.compareAndSet(false, true)) {
                // Lock will never be released intentionally.
                lock.writeLock().lock();

                if (timeoutObj != null)
                    cctx.time().removeTimeoutObject(timeoutObj);

                boolean b = onCancelled();

                assert b;

                return true;
            }

            return false;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(EvictionFuture.class, this);
        }
    }
}
