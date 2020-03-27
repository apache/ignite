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
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.NodeStoppingException;
import org.apache.ignite.internal.managers.communication.GridMessageListener;
import org.apache.ignite.internal.pagemem.store.IgnitePageStoreManager;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.persistence.CheckpointProgress;
import org.apache.ignite.internal.processors.cache.persistence.IgniteCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.MetastorageLifecycleListener;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.ReadOnlyMetastorage;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.ReadWriteMetastorage;
import org.apache.ignite.internal.util.GridBoundedConcurrentLinkedHashSet;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.lang.IgniteInClosureX;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.util.worker.GridWorker;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.thread.IgniteThread;
import org.apache.ignite.thread.OomExceptionHandler;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.GridTopic.TOPIC_WAL;
import static org.apache.ignite.internal.managers.communication.GridIoPolicy.SYSTEM_POOL;
import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.MOVING;
import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.OWNING;
import static org.apache.ignite.internal.processors.cache.persistence.CheckpointState.FINISHED;
import static org.apache.ignite.internal.processors.cache.persistence.CheckpointState.LOCK_RELEASED;

/**
 * Write-ahead log state manager. Manages WAL enable and disable.
 */
public class WalStateManager extends GridCacheSharedManagerAdapter {
    /** History size for to track stale messages. */
    private static final int HIST_SIZE = 1000;

    /** ID history for discovery messages. */
    private final GridBoundedConcurrentLinkedHashSet<T2<UUID, Boolean>> discoMsgIdHist =
        new GridBoundedConcurrentLinkedHashSet<>(HIST_SIZE);

    /** History of already completed operations. */
    private final GridBoundedConcurrentLinkedHashSet<UUID> completedOpIds =
        new GridBoundedConcurrentLinkedHashSet<>(HIST_SIZE);

    /** Client futures. */
    private final Map<UUID, GridFutureAdapter<Boolean>> userFuts = new HashMap<>();

    /** Finished results awaiting discovery finish message. */
    private final Map<UUID, WalStateResult> ress = new HashMap<>();

    /** Active distributed processes. */
    private final Map<UUID, WalStateDistributedProcess> procs = new HashMap<>();

    /** Pending results created on cache processor start based on available discovery data. */
    private final Collection<WalStateResult> initialRess = new LinkedList<>();

    /** Pending acknowledge messages (i.e. received before node completed it's local part). */
    private final Collection<WalStateAckMessage> pendingAcks = new HashSet<>();

    /** Whether this is a server node. */
    private final boolean srv;

    /** IO message listener. */
    private final GridMessageListener ioLsnr;

    /** Operation mutex. */
    private final Object mux = new Object();

    /** Logger. */
    private final IgniteLogger log;

    /** Current coordinator node. */
    private ClusterNode crdNode;

    /** Disconnected flag. */
    private boolean disconnected;

    /** Holder for groups with temporary disabled WAL. */
    private volatile TemporaryDisabledWal tmpDisabledWal;

    /** */
    private volatile WALDisableContext walDisableContext;

    /** Denies or allows WAL disabling. */
    private volatile boolean prohibitDisabling;

    /**
     * Constructor.
     *
     * @param kernalCtx Kernal context.
     */
    public WalStateManager(GridKernalContext kernalCtx) {
        if (kernalCtx != null) {
            IgniteConfiguration cfg = kernalCtx.config();

            boolean client = cfg.isClientMode() != null && cfg.isClientMode();

            srv = !client && !cfg.isDaemon();

            log = kernalCtx.log(WalStateManager.class);
        }
        else {
            srv = false;

            log = null;
        }

        if (srv) {
            ioLsnr = new GridMessageListener() {
                @Override public void onMessage(UUID nodeId, Object msg, byte plc) {
                    if (msg instanceof WalStateAckMessage) {
                        WalStateAckMessage msg0 = (WalStateAckMessage) msg;

                        msg0.senderNodeId(nodeId);

                        onAck(msg0);
                    }
                    else
                        U.warn(log, "Unexpected IO message (will ignore): " + msg);
                }
            };
        }
        else
            ioLsnr = null;
    }

    /** {@inheritDoc} */
    @Override protected void start0() throws IgniteCheckedException {
        if (srv)
            cctx.kernalContext().io().addMessageListener(TOPIC_WAL, ioLsnr);

        walDisableContext = new WALDisableContext(
            cctx.cache().context().database(),
            cctx.pageStore(),
            log
        );

        cctx.kernalContext().internalSubscriptionProcessor().registerMetastorageListener(walDisableContext);

    }

    /** {@inheritDoc} */
    @Override protected void stop0(boolean cancel) {
        if (srv)
            cctx.kernalContext().io().removeMessageListener(TOPIC_WAL, ioLsnr);
    }

    /**
     * Callback invoked when caches info is collected inside cache processor start routine. Discovery is not
     * active at this point.
     */
    public void onCachesInfoCollected() {
        if (!srv)
            return;

        synchronized (mux) {
            // Process top pending requests.
            for (CacheGroupDescriptor grpDesc : cacheProcessor().cacheGroupDescriptors().values()) {
                WalStateProposeMessage msg = grpDesc.nextWalChangeRequest();

                if (msg != null) {
                    if (log.isDebugEnabled())
                        log.debug("Processing WAL state message on start: " + msg);

                    boolean enabled = grpDesc.walEnabled();

                    WalStateResult res;

                    if (F.eq(enabled, msg.enable()))
                        res = new WalStateResult(msg, false);
                    else {
                        res = new WalStateResult(msg, true);

                        grpDesc.walEnabled(!enabled);
                    }

                    initialRess.add(res);

                    addResult(res);
                }
            }
        }
    }

    /**
     * Handle cache processor kernal start. At this point we already collected discovery data from other nodes
     * (discovery already active), but exchange worker is not active yet. We need to iterate over available group
     * descriptors and perform top operations, taking in count that no cache operations are possible at this point,
     * so checkpoint is not needed.
     */
    public void onKernalStart() {
        if (!srv)
            return;

        synchronized (mux) {
            for (WalStateResult res : initialRess)
                onCompletedLocally(res);

            initialRess.clear();
        }
    }

    /** {@inheritDoc} */
    @Override public void onDisconnected(IgniteFuture reconnectFut) {
        Collection<GridFutureAdapter<Boolean>> userFuts0;

        synchronized (mux) {
            assert !disconnected;

            disconnected = true;

            userFuts0 = new ArrayList<>(userFuts.values());

            userFuts.clear();
        }

        for (GridFutureAdapter<Boolean> userFut : userFuts0)
            completeWithError(userFut, "Client node was disconnected from topology (operation result is unknown).");
    }

    /** {@inheritDoc} */
    @Override public void onReconnected(boolean active) {
        synchronized (mux) {
            assert disconnected;

            disconnected = false;
        }
    }

    /**
     * Denies or allows WAL disabling with subsequent {@link #init(Collection, boolean)} call.
     *
     * @param val denial status.
     */
    public void prohibitWALDisabling(boolean val) {
        prohibitDisabling = val;
    }

    /**
     * Reports whether WAL disabling with subsequent {@link #init(Collection, boolean)} is denied.
     *
     * @return denial status.
     */
    public boolean prohibitWALDisabling() {
        return prohibitDisabling;
    }

    /**
     * Change WAL mode.
     *
     * @param cacheNames Cache names.
     * @param enabled Enabled flag.
     * @return Future completed when operation finished.
     */
    public IgniteInternalFuture<Boolean> changeWalMode(Collection<String> cacheNames, boolean enabled) {
        cctx.tm().checkEmptyTransactions(() ->
            String.format("Cache WAL mode cannot be changed within lock or transaction " +
                    "[cacheNames=%s, walEnabled=%s]", cacheNames, enabled));

        return init(cacheNames, enabled);
    }

    /**
     * Initiate WAL mode change operation.
     *
     * @param cacheNames Cache names.
     * @param enabled Enabled flag.
     * @return Future completed when operation finished.
     */
    private IgniteInternalFuture<Boolean> init(Collection<String> cacheNames, boolean enabled) {
        if (!enabled && prohibitDisabling)
            return errorFuture("WAL disabling is prohibited.");

        if (F.isEmpty(cacheNames))
            return errorFuture("Cache names cannot be empty.");

        synchronized (mux) {
            if (disconnected)
                return errorFuture("Failed to initiate WAL mode change because client node is disconnected.");

            // Prepare cache and group infos.
            Map<String, IgniteUuid> caches = new HashMap<>(cacheNames.size());

            CacheGroupDescriptor grpDesc = null;

            for (String cacheName : cacheNames) {
                DynamicCacheDescriptor cacheDesc = cacheProcessor().cacheDescriptor(cacheName);

                if (cacheDesc == null)
                    return errorFuture("Cache doesn't exist: " + cacheName);

                caches.put(cacheName, cacheDesc.deploymentId());

                CacheGroupDescriptor curGrpDesc = cacheDesc.groupDescriptor();

                if (grpDesc == null)
                    grpDesc = curGrpDesc;
                else if (!F.eq(grpDesc.deploymentId(), curGrpDesc.deploymentId())) {
                    return errorFuture("Cannot change WAL mode for caches from different cache groups [" +
                        "cache1=" + cacheNames.iterator().next() + ", grp1=" + grpDesc.groupName() +
                        ", cache2=" + cacheName + ", grp2=" + curGrpDesc.groupName() + ']');
                }
            }

            assert grpDesc != null;

            HashSet<String> grpCaches = new HashSet<>(grpDesc.caches().keySet());

            grpCaches.removeAll(cacheNames);

            if (!grpCaches.isEmpty()) {
                return errorFuture("Cannot change WAL mode because not all cache names belonging to the group are " +
                    "provided [group=" + grpDesc.groupName() + ", missingCaches=" + grpCaches + ']');
            }

            if (grpDesc.config().getCacheMode() == CacheMode.LOCAL)
                return errorFuture("WAL mode cannot be changed for LOCAL cache(s): " + cacheNames);

            // WAL mode change makes sense only for persistent groups.
            if (!grpDesc.persistenceEnabled())
                return errorFuture("Cannot change WAL mode because persistence is not enabled for cache(s) [" +
                    "caches=" + cacheNames + ", dataRegion=" + grpDesc.config().getDataRegionName() + ']');

            // Send request.
            final UUID opId = UUID.randomUUID();

            GridFutureAdapter<Boolean> fut = new GridFutureAdapter<>();

            fut.listen(new IgniteInClosure<IgniteInternalFuture<Boolean>>() {
                @Override public void apply(IgniteInternalFuture<Boolean> fut) {
                    synchronized (mux) {
                        userFuts.remove(opId);
                    }
                }
            });

            WalStateProposeMessage msg = new WalStateProposeMessage(opId, grpDesc.groupId(), grpDesc.deploymentId(),
                cctx.localNodeId(), caches, enabled);

            userFuts.put(opId, fut);

            try {
                cctx.discovery().sendCustomEvent(msg);

                if (log.isDebugEnabled())
                    log.debug("Initiated WAL state change operation: " + msg);
            }
            catch (Exception e) {
                IgniteCheckedException e0 =
                    new IgniteCheckedException("Failed to initiate WAL mode change due to unexpected exception.", e);

                fut.onDone(e0);
            }

            return fut;
        }
    }

    /**
     * Change local WAL state before exchange is done. This method will disable WAL for groups without partitions
     * in OWNING state if such feature is enabled.
     *
     * @param topVer Topology version.
     * @param changedBaseline The exchange is caused by Baseline Topology change.
     */
    public void changeLocalStatesOnExchangeDone(AffinityTopologyVersion topVer, boolean changedBaseline) {
        if (changedBaseline
            && IgniteSystemProperties.getBoolean(IgniteSystemProperties.IGNITE_PENDING_TX_TRACKER_ENABLED)
            || !IgniteSystemProperties.getBoolean(IgniteSystemProperties.IGNITE_DISABLE_WAL_DURING_REBALANCING, true))
            return;

        Set<Integer> grpsToEnableWal = new HashSet<>();
        Set<Integer> grpsToDisableWal = new HashSet<>();
        Set<Integer> grpsWithWalDisabled = new HashSet<>();

        boolean hasNonEmptyOwning = false;

        for (CacheGroupContext grp : cctx.cache().cacheGroups()) {
            if (grp.isLocal() || !grp.affinityNode() || !grp.persistenceEnabled())
                continue;

            boolean hasOwning = false;
            boolean hasMoving = false;

            int parts = 0;

            for (GridDhtLocalPartition locPart : grp.topology().currentLocalPartitions()) {
                if (locPart.state() == OWNING) {
                    hasOwning = true;

                    if (hasNonEmptyOwning)
                        break;

                    if (!locPart.isEmpty()) {
                        hasNonEmptyOwning = true;

                        break;
                    }

                    parts++;
                }

                if (locPart.state() == MOVING)
                    hasMoving = true;
            }

            if (log.isDebugEnabled())
                log.debug("Prepare change WAL state, grp=" + grp.cacheOrGroupName() +
                    ", grpId=" + grp.groupId() + ", hasOwning=" + hasOwning + ", hasMoving=" + hasMoving +
                    ", WALState=" + grp.walEnabled() + ", parts=" + parts);

            if (hasOwning && !grp.localWalEnabled())
                grpsToEnableWal.add(grp.groupId());
            else if (hasMoving && !hasOwning && grp.localWalEnabled()) {
                grpsToDisableWal.add(grp.groupId());

                grpsWithWalDisabled.add(grp.groupId());
            }
            else if (!grp.localWalEnabled())
                grpsWithWalDisabled.add(grp.groupId());
        }

        tmpDisabledWal = new TemporaryDisabledWal(grpsWithWalDisabled, topVer);

        if (grpsToEnableWal.isEmpty() && grpsToDisableWal.isEmpty())
            return;

        try {
            if (hasNonEmptyOwning && !grpsToEnableWal.isEmpty())
                triggerCheckpoint("wal-local-state-change-" + topVer).futureFor(FINISHED).get();
        }
        catch (IgniteCheckedException ex) {
            throw new IgniteException(ex);
        }

        for (Integer grpId : grpsToEnableWal)
            cctx.cache().cacheGroup(grpId).localWalEnabled(true, true);

        for (Integer grpId : grpsToDisableWal)
            cctx.cache().cacheGroup(grpId).localWalEnabled(false, true);
    }

    /**
     * Callback when group rebalancing is finished. If there are no pending groups, it should trigger checkpoint and
     * change partition states.
     * @param grpId Group ID.
     * @param topVer Topology version.
     */
    public void onGroupRebalanceFinished(int grpId, AffinityTopologyVersion topVer) {
        TemporaryDisabledWal session0 = tmpDisabledWal;

        if (session0 == null || session0.topVer.compareTo(topVer) > 0)
            return;

        session0.remainingGrps.remove(grpId);

        if (session0.remainingGrps.isEmpty()) {
            synchronized (mux) {
                if (tmpDisabledWal != session0)
                    return;

                for (Integer grpId0 : session0.disabledGrps) {
                    CacheGroupContext grp = cctx.cache().cacheGroup(grpId0);

                    assert grp != null;

                    if (!grp.localWalEnabled())
                        grp.localWalEnabled(true, false);
                }

                tmpDisabledWal = null;
            }

            // Pending updates in groups with disabled WAL are not protected from crash.
            // Need to trigger checkpoint for attempt to persist them.
            CheckpointProgress cpFut = triggerCheckpoint("wal-local-state-changed-rebalance-finished-" + topVer);

            assert cpFut != null;

            // It's safe to switch partitions to owning state only if checkpoint was successfully finished.
            cpFut.futureFor(FINISHED).listen(new IgniteInClosureX<IgniteInternalFuture>() {
                @Override public void applyx(IgniteInternalFuture future) {
                    if (X.hasCause(future.error(), NodeStoppingException.class))
                        return;

                    for (Integer grpId0 : session0.disabledGrps) {
                        try {
                            cctx.database().walEnabled(grpId0, true, true);
                        }
                        catch (Exception e) {
                            if (!X.hasCause(e, NodeStoppingException.class))
                                throw e;
                        }

                        CacheGroupContext grp = cctx.cache().cacheGroup(grpId0);

                        if (grp != null)
                            grp.topology().ownMoving(topVer);
                        else if (log.isDebugEnabled())
                            log.debug("Cache group was destroyed before checkpoint finished, [grpId=" + grpId0 + ']');
                    }

                    if (log.isDebugEnabled())
                        log.debug("Refresh partitions due to rebalance finished");

                    // Trigger exchange for switching to ideal assignment when all nodes are ready.
                    cctx.exchange().refreshPartitions();
                }
            });
        }
    }

    /**
     * Handle propose message in discovery thread.
     *
     * @param msg Message.
     */
    public void onProposeDiscovery(WalStateProposeMessage msg) {
        if (isDuplicate(msg))
            return;

        synchronized (mux) {
            if (disconnected)
                return;

            // Validate current caches state before deciding whether to process message further.
            if (validateProposeDiscovery(msg)) {
                if (log.isDebugEnabled())
                    log.debug("WAL state change message is valid (will continue processing): " + msg);

                CacheGroupDescriptor grpDesc = cacheProcessor().cacheGroupDescriptors().get(msg.groupId());

                assert grpDesc != null;

                IgnitePredicate<ClusterNode> nodeFilter = grpDesc.config().getNodeFilter();

                boolean affNode = srv && (nodeFilter == null || nodeFilter.apply(cctx.localNode()));

                msg.affinityNode(affNode);

                if (grpDesc.addWalChangeRequest(msg)) {
                    msg.exchangeMessage(msg);

                    if (log.isDebugEnabled())
                        log.debug("WAL state change message will be processed in exchange thread: " + msg);
                }
                else {
                    if (log.isDebugEnabled())
                        log.debug("WAL state change message is added to pending set and will be processed later: " +
                            msg);
                }
            }
            else {
                if (log.isDebugEnabled())
                    log.debug("WAL state change message is invalid (will ignore): " + msg);
            }
        }
    }

    /**
     * Validate propose message.
     *
     * @param msg Message.
     * @return {@code True} if message should be processed further, {@code false} if no further processing is needed.
     */
    private boolean validateProposeDiscovery(WalStateProposeMessage msg) {
        GridFutureAdapter<Boolean> userFut = userFuts.get(msg.operationId());

        String errMsg = validate(msg);

        if (errMsg != null) {
            completeWithError(userFut, errMsg);

            return false;
        }

        return true;
    }

    /**
     * Validate propose message.
     *
     * @param msg Message.
     * @return Error message or {@code null} if everything is OK.
     */
    @Nullable private String validate(WalStateProposeMessage msg) {
        // Is group still there?
        CacheGroupDescriptor grpDesc = cacheProcessor().cacheGroupDescriptors().get(msg.groupId());

        if (grpDesc == null)
            return "Failed to change WAL mode because some caches no longer exist: " + msg.caches().keySet();

        // Are specified caches still there?
        for (Map.Entry<String, IgniteUuid> cache : msg.caches().entrySet()) {
            String cacheName = cache.getKey();

            DynamicCacheDescriptor cacheDesc = cacheProcessor().cacheDescriptor(cacheName);

            if (cacheDesc == null || !F.eq(cacheDesc.deploymentId(), cache.getValue()))
                return "Cache doesn't exist: " + cacheName;
        }

        // Are there any new caches in the group?
        HashSet<String> grpCacheNames = new HashSet<>(grpDesc.caches().keySet());

        grpCacheNames.removeAll(msg.caches().keySet());

        if (!grpCacheNames.isEmpty()) {
            return "Cannot change WAL mode because not all cache names belonging to the " +
                "group are provided [group=" + grpDesc.groupName() + ", missingCaches=" + grpCacheNames + ']';
        }

        return null;
    }

    /**
     * Handle propose message which is synchronized with other cache state actions through exchange thread.
     * If operation is no-op (i.e. state is not changed), then no additional processing is needed, and coordinator will
     * trigger finish request right away. Otherwise all nodes start asynchronous checkpoint flush, and send responses
     * to coordinator. Once all responses are received, coordinator node will trigger finish message.
     *
     * @param msg Message.
     */
    public void onProposeExchange(WalStateProposeMessage msg) {
        if (!srv)
            return;

        synchronized (mux) {
            WalStateResult res = null;

            if (msg.affinityNode()) {
                // Affinity node, normal processing.
                CacheGroupContext grpCtx = cacheProcessor().cacheGroup(msg.groupId());

                if (grpCtx == null) {
                    // Related caches were destroyed concurrently.
                    res = new WalStateResult(msg, "Failed to change WAL mode because some caches " +
                        "no longer exist: " + msg.caches().keySet());
                }
                else {
                    if (F.eq(msg.enable(), grpCtx.globalWalEnabled()))
                        // Nothing changed -> no-op.
                        res = new WalStateResult(msg, false);
                    else {
                        // Initiate a checkpoint.
                        CheckpointProgress cpFut = triggerCheckpoint("wal-state-change-grp-" + msg.groupId());

                        if (cpFut != null) {
                            try {
                                // Wait for checkpoint mark synchronously before releasing the control.
                                cpFut.futureFor(LOCK_RELEASED).get();

                                if (msg.enable()) {
                                    grpCtx.globalWalEnabled(true);

                                    // Enable: it is enough to release cache operations once mark is finished because
                                    // not-yet-flushed dirty pages have been logged.
                                    WalStateChangeWorker worker = new WalStateChangeWorker(msg, cpFut);

                                    IgniteThread thread = new IgniteThread(worker);

                                    thread.setUncaughtExceptionHandler(new OomExceptionHandler(
                                        cctx.kernalContext()));

                                    thread.start();
                                }
                                else {
                                    // Disable: not-yet-flushed operations are not logged, so wait for them
                                    // synchronously in exchange thread. Otherwise, we cannot define a point in
                                    // when it is safe to continue cache operations.
                                    res = awaitCheckpoint(cpFut, msg);

                                    // WAL state is persisted after checkpoint if finished. Otherwise in case of crash
                                    // and restart we will think that WAL is enabled, but data might be corrupted.
                                    grpCtx.globalWalEnabled(false);
                                }
                            }
                            catch (Exception e) {
                                U.warn(log, "Failed to change WAL mode due to unexpected exception [" +
                                    "msg=" + msg + ']', e);

                                res = new WalStateResult(msg, "Failed to change WAL mode due to unexpected " +
                                    "exception (see server logs for more information): " + e.getMessage());
                            }
                        }
                        else {
                            res = new WalStateResult(msg, "Failed to initiate a checkpoint (checkpoint thread " +
                                "is not available).");
                        }
                    }
                }
            }
            else {
                // We cannot know result on non-affinity server node, so just complete operation with "false" flag,
                // which will be ignored anyway.
                res = new WalStateResult(msg, false);
            }

            if (res != null) {
                addResult(res);

                onCompletedLocally(res);
            }
        }
    }

    /**
     * Handle local operation completion.
     *
     * @param res Result.
     */
    private void onCompletedLocally(WalStateResult res) {
        assert res != null;

        synchronized (mux) {
            ClusterNode crdNode = coordinator();

            UUID opId = res.message().operationId();

            WalStateAckMessage msg = new WalStateAckMessage(opId, res.message().affinityNode(),
                res.changed(), res.errorMessage());

            // Handle distributed completion.
            if (crdNode.isLocal()) {
                Collection<ClusterNode> srvNodes = cctx.discovery().aliveServerNodes();

                Collection<UUID> srvNodeIds = new ArrayList<>(srvNodes.size());

                for (ClusterNode srvNode : srvNodes) {
                    if (cctx.discovery().alive(srvNode))
                        srvNodeIds.add(srvNode.id());
                }

                WalStateDistributedProcess proc = new WalStateDistributedProcess(res.message(), srvNodeIds);

                procs.put(res.message().operationId(), proc);

                unwindPendingAcks(proc);

                proc.onNodeFinished(cctx.localNodeId(), msg);

                sendFinishMessageIfNeeded(proc);
            }
            else {
                // Just send message to coordinator.
                try {
                    cctx.kernalContext().io().sendToGridTopic(crdNode, TOPIC_WAL, msg, SYSTEM_POOL);
                }
                catch (IgniteCheckedException e) {
                    U.warn(log, "Failed to send ack message to coordinator node [opId=" + opId +
                        ", node=" + crdNode.id() + ']');
                }
            }
        }
    }

    /**
     * Unwind pending ack messages for the given distributed process.
     *
     * @param proc Process.
     */
    private void unwindPendingAcks(WalStateDistributedProcess proc) {
        assert Thread.holdsLock(mux);

        Iterator<WalStateAckMessage> iter = pendingAcks.iterator();

        while (iter.hasNext()) {
            WalStateAckMessage ackMsg = iter.next();

            if (F.eq(proc.operationId(), ackMsg.operationId())) {
                proc.onNodeFinished(ackMsg.senderNodeId(), ackMsg);

                iter.remove();
            }
        }
    }

    /**
     * Handle ack message.
     *
     * @param msg Ack message.
     */
    public void onAck(WalStateAckMessage msg) {
        synchronized (mux) {
            if (completedOpIds.contains(msg.operationId()))
                // Skip stale messages.
                return;

            WalStateDistributedProcess proc = procs.get(msg.operationId());

            if (proc == null)
                // If process if not initialized yet, add to pending set.
                pendingAcks.add(msg);
            else {
                // Notify process on node completion.
                proc.onNodeFinished(msg.senderNodeId(), msg);

                sendFinishMessageIfNeeded(proc);
            }
        }
    }

    /**
     * Send finish message for the given distributed process if needed.
     *
     * @param proc Process.
     */
    private void sendFinishMessageIfNeeded(WalStateDistributedProcess proc) {
        if (proc.completed())
            sendFinishMessage(proc.prepareFinishMessage());
    }

    /**
     * Send finish message.
     *
     * @param finishMsg Finish message.
     */
    private void sendFinishMessage(WalStateFinishMessage finishMsg) {
        try {
            cctx.discovery().sendCustomEvent(finishMsg);
        }
        catch (Exception e) {
            U.error(log, "Failed to send WAL mode change finish message due to unexpected exception: " + finishMsg, e);
        }
    }

    /**
     * Handle finish message in discovery thread.
     *
     * @param msg Message.
     */
    public void onFinishDiscovery(WalStateFinishMessage msg) {
        if (isDuplicate(msg))
            return;

        synchronized (mux) {
            if (disconnected)
                return;

            // Complete user future, if any.
            GridFutureAdapter<Boolean> userFut = userFuts.get(msg.operationId());

            if (userFut != null) {
                if (msg.errorMessage() != null)
                    completeWithError(userFut, msg.errorMessage());
                else
                    complete(userFut, msg.changed());
            }

            // Clear pending data.
            WalStateResult res = ress.remove(msg.operationId());

            if (res == null && srv)
                U.warn(log, "Received finish message for unknown operation (will ignore): " + msg.operationId());

            procs.remove(msg.operationId());

            CacheGroupDescriptor grpDesc = cacheProcessor().cacheGroupDescriptors().get(msg.groupId());

            if (grpDesc != null && F.eq(grpDesc.deploymentId(), msg.groupDeploymentId())) {
                // Toggle WAL mode in descriptor.
                if (msg.changed())
                    grpDesc.walEnabled(!grpDesc.walEnabled());

                // Remove now-outdated message from the queue.
                WalStateProposeMessage oldProposeMsg = grpDesc.nextWalChangeRequest();

                assert oldProposeMsg != null;
                assert F.eq(oldProposeMsg.operationId(), msg.operationId());

                grpDesc.removeWalChangeRequest();

                // Move next message to exchange thread.
                WalStateProposeMessage nextProposeMsg = grpDesc.nextWalChangeRequest();

                if (nextProposeMsg != null)
                    msg.exchangeMessage(nextProposeMsg);
            }

            if (srv) {
                // Remember operation ID to handle duplicates.
                completedOpIds.add(msg.operationId());

                // Remove possible stale messages.
                Iterator<WalStateAckMessage> ackIter = pendingAcks.iterator();

                while (ackIter.hasNext()) {
                    WalStateAckMessage ackMsg = ackIter.next();

                    if (F.eq(ackMsg.operationId(), msg.operationId()))
                        ackIter.remove();
                }
            }
        }
    }

    /**
     * Handle node leave event.
     *
     * @param nodeId Node ID.
     */
    public void onNodeLeft(UUID nodeId) {
        if (!srv)
            return;

        synchronized (mux) {
            if (crdNode == null) {
                assert ress.isEmpty();
                assert procs.isEmpty();

                return;
            }

            if (F.eq(crdNode.id(), nodeId)) {
                // Coordinator exited, re-send to new, or initialize new distirbuted processes.
                crdNode = null;

                for (WalStateResult res : ress.values())
                    onCompletedLocally(res);
            }
            else if (F.eq(cctx.localNodeId(), crdNode.id())) {
                // Notify distributed processes on node leave.
                for (Map.Entry<UUID, WalStateDistributedProcess> procEntry : procs.entrySet()) {
                    WalStateDistributedProcess proc = procEntry.getValue();

                    proc.onNodeLeft(nodeId);

                    sendFinishMessageIfNeeded(proc);
                }
            }
        }
    }

    /**
     * Create future with error.
     *
     * @param errMsg Error message.
     * @return Future.
     */
    @SuppressWarnings("Convert2Diamond")
    private static IgniteInternalFuture<Boolean> errorFuture(String errMsg) {
        return new GridFinishedFuture<Boolean>(new IgniteCheckedException(errMsg));
    }

    /**
     * Complete user future with normal result.
     *
     * @param userFut User future.
     * @param res Result.
     */
    private static void complete(@Nullable GridFutureAdapter<Boolean> userFut, boolean res) {
        if (userFut != null)
            userFut.onDone(res);
    }

    /**
     * Complete user future with error.
     *
     * @param errMsg Error message.
     */
    private static void completeWithError(@Nullable GridFutureAdapter<Boolean> userFut, String errMsg) {
        if (userFut != null)
            userFut.onDone(new IgniteCheckedException(errMsg));
    }

    /**
     * @return Cache processor.
     */
    private GridCacheProcessor cacheProcessor() {
        return cctx.cache();
    }

    /**
     * Get current coordinator node.
     *
     * @return Coordinator node.
     */
    private ClusterNode coordinator() {
        assert Thread.holdsLock(mux);

        if (crdNode != null)
            return crdNode;
        else {
            ClusterNode res = null;

            for (ClusterNode node : cctx.discovery().aliveServerNodes()) {
                if (res == null || res.order() > node.order())
                    res = node;
            }

            assert res != null;

            crdNode = res;

            return res;
        }
    }

    /**
     * Check if discovery message has already been received.
     *
     * @param msg Message.
     * @return {@code True} if this is a duplicate.
     */
    private boolean isDuplicate(WalStateAbstractMessage msg) {
        T2<UUID, Boolean> key;

        if (msg instanceof WalStateProposeMessage)
            key = new T2<>(msg.operationId(), true);
        else {
            assert msg instanceof WalStateFinishMessage;

            key = new T2<>(msg.operationId(), false);
        }

        if (!discoMsgIdHist.add(key)) {
            U.warn(log, "Received duplicate WAL mode change discovery message (will ignore): " + msg);

            return true;
        }

        return false;
    }

    /**
     * Add locally result to pending map.
     *
     * @param res Result.
     */
    private void addResult(WalStateResult res) {
        ress.put(res.message().operationId(), res);
    }

    /**
     * Force checkpoint.
     *
     * @param msg Message.
     * @return Checkpoint future or {@code null} if failed to get checkpointer.
     */
    @Nullable private CheckpointProgress triggerCheckpoint(String msg) {
        return cctx.database().forceCheckpoint(msg);
    }

    /**
     * Await for the checkpoint to finish.
     *
     * @param cpFut Checkpoint future.
     * @param msg Orignial message which triggered the process.
     * @return Result.
     */
    private WalStateResult awaitCheckpoint(CheckpointProgress cpFut, WalStateProposeMessage msg) {
        WalStateResult res;

        try {
            assert msg.affinityNode();

            if (cpFut != null)
                cpFut.futureFor(FINISHED).get();

            res = new WalStateResult(msg, true);
        }
        catch (Exception e) {
            U.warn(log, "Failed to change WAL mode due to unexpected exception [msg=" + msg + ']', e);

            res = new WalStateResult(msg, "Failed to change WAL mode due to unexpected exception " +
                "(see server logs for more information): " + e.getMessage());
        }

        return res;
    }

    /**
     * Checks WAL disabled for cache group.
     *
     * @param grpId Group id.
     * @return {@code True} if WAL disable for group. {@code False} If not.
     */
    public boolean isDisabled(int grpId) {
        CacheGroupContext ctx = cctx.cache().cacheGroup(grpId);

        return ctx != null && !ctx.walEnabled();
    }

    /**
     * @return WAL disable context.
     */
    public WALDisableContext walDisableContext(){
        return walDisableContext;
    }

    /**
     * None record will be logged in closure call.
     *
     * @param cls Closure to execute out of WAL scope.
     * @throws IgniteCheckedException If operation failed.
     */
    public void runWithOutWAL(IgniteRunnable cls) throws IgniteCheckedException {
        WALDisableContext ctx = walDisableContext;

        if (ctx == null)
            throw new IgniteCheckedException("Disable WAL context is not initialized.");

        ctx.execute(cls);
    }

    /**
     * WAL state change worker.
     */
    private class WalStateChangeWorker extends GridWorker {
        /** Message. */
        private final WalStateProposeMessage msg;

        /** Checkpoint future. */
        private final CheckpointProgress cpFut;

        /**
         * Constructor.
         *
         * @param msg Propose message.
         */
        private WalStateChangeWorker(WalStateProposeMessage msg, CheckpointProgress cpFut) {
            super(cctx.igniteInstanceName(), "wal-state-change-worker-" + msg.groupId(), WalStateManager.this.log);

            this.msg = msg;
            this.cpFut = cpFut;
        }

        /** {@inheritDoc} */
        @Override protected void body() throws InterruptedException, IgniteInterruptedCheckedException {
            WalStateResult res = awaitCheckpoint(cpFut, msg);

            addResult(res);

            onCompletedLocally(res);
        }
    }

    /**
     *
     */
    private static class TemporaryDisabledWal {
        /** Groups with disabled WAL. */
        private final Set<Integer> disabledGrps;

        /** Remaining groups. */
        private final Set<Integer> remainingGrps;

        /** Topology version*/
        private final AffinityTopologyVersion topVer;

        /** */
        public TemporaryDisabledWal(
            Set<Integer> disabledGrps,
            AffinityTopologyVersion topVer
        ) {
            this.disabledGrps = Collections.unmodifiableSet(disabledGrps);
            this.remainingGrps = new HashSet<>(disabledGrps);
            this.topVer = topVer;
        }
    }

    /**
     *
     */
    public static class WALDisableContext implements MetastorageLifecycleListener{
        /** */
        public static final String WAL_DISABLED = "wal-disabled";

        /** */
        private final IgniteLogger log;

        /** */
        private final IgniteCacheDatabaseSharedManager dbMgr;

        /** */
        private volatile ReadWriteMetastorage metaStorage;

        /** */
        private final IgnitePageStoreManager pageStoreMgr;

        /** */
        private volatile boolean resetWalFlag;

        /** */
        private volatile boolean disableWal;

        /**
         * @param dbMgr  Database manager.
         * @param pageStoreMgr Page store manager.
         * @param log
         *
         */
        public WALDisableContext(
            IgniteCacheDatabaseSharedManager dbMgr,
            IgnitePageStoreManager pageStoreMgr,
            @Nullable IgniteLogger log
        ) {
            this.dbMgr = dbMgr;
            this.pageStoreMgr = pageStoreMgr;
            this.log = log;
        }

        /**
         * @param cls Closure to execute with disabled WAL.
         * @throws IgniteCheckedException If execution failed.
         */
        public void execute(IgniteRunnable cls) throws IgniteCheckedException {
            if (cls == null)
                throw new IgniteCheckedException("Task to execute is not specified.");

            if (metaStorage == null)
                throw new IgniteCheckedException("Meta storage is not ready.");

            writeMetaStoreDisableWALFlag();

            dbMgr.waitForCheckpoint("Checkpoint before apply updates on recovery.");

            disableWAL(true);

            try {
                cls.run();
            }
            catch (IgniteException e) {
                throw new IgniteCheckedException(e);
            }
            finally {
                disableWAL(false);

                dbMgr.waitForCheckpoint("Checkpoint after apply updates on recovery.");

                removeMetaStoreDisableWALFlag();
            }
        }

        /**
         * @throws IgniteCheckedException If write meta store flag failed.
         */
        protected void writeMetaStoreDisableWALFlag() throws IgniteCheckedException {
            dbMgr.checkpointReadLock();

            try {
                metaStorage.write(WAL_DISABLED, Boolean.TRUE);
            }
            finally {
                dbMgr.checkpointReadUnlock();
            }
        }

        /**
         * @throws IgniteCheckedException If remove meta store flag failed.
         */
        protected void removeMetaStoreDisableWALFlag() throws IgniteCheckedException {
            dbMgr.checkpointReadLock();

            try {
                metaStorage.remove(WAL_DISABLED);
            }
            finally {
                dbMgr.checkpointReadUnlock();
            }
        }

        /**
         * @param disable Flag wal disable.
         */
        protected void disableWAL(boolean disable) throws IgniteCheckedException {
            dbMgr.checkpointReadLock();

            try {
                disableWal = disable;

                if (log != null)
                    log.info("WAL logging " + (disable ? "disabled" : "enabled"));
            }
            finally {
                dbMgr.checkpointReadUnlock();
            }
        }

        /** {@inheritDoc} */
        @Override public void onReadyForRead(ReadOnlyMetastorage ms) throws IgniteCheckedException {
            Boolean disabled = (Boolean)ms.read(WAL_DISABLED);

            // Node crash when WAL was disabled.
            if (disabled != null && disabled){
                resetWalFlag = true;

                pageStoreMgr.cleanupPersistentSpace();

                dbMgr.cleanupTempCheckpointDirectory();

                dbMgr.cleanupCheckpointDirectory();
            }
        }

        /** {@inheritDoc} */
        @Override public void onReadyForReadWrite(ReadWriteMetastorage ms) throws IgniteCheckedException {
            // On new node start WAL always enabled. Remove flag from metastore.
            if (resetWalFlag)
                ms.remove(WAL_DISABLED);

            metaStorage = ms;
        }

        /**
         * @return {@code true} If WAL is disabled.
         */
        public boolean check() {
            return disableWal;
        }
    }
}
