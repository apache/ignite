/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cluster;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.cluster.ClusterGroupAdapter;
import org.apache.ignite.internal.managers.discovery.CustomEventListener;
import org.apache.ignite.internal.managers.eventstorage.GridLocalEventListener;
import org.apache.ignite.internal.pagemem.store.IgnitePageStoreManager;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.ChangeGlobalStateMessage;
import org.apache.ignite.internal.processors.cache.ClusterState;
import org.apache.ignite.internal.processors.cache.DynamicCacheChangeBatch;
import org.apache.ignite.internal.processors.cache.DynamicCacheChangeRequest;
import org.apache.ignite.internal.processors.cache.ExchangeActions;
import org.apache.ignite.internal.processors.cache.GridCacheProcessor;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.GridChangeGlobalStateMessageResponse;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.CI2;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.spi.discovery.DiscoveryDataBag;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;
import static org.apache.ignite.internal.managers.communication.GridIoPolicy.SYSTEM_POOL;
import static org.apache.ignite.internal.processors.cache.ClusterState.ACTIVE;
import static org.apache.ignite.internal.processors.cache.ClusterState.INACTIVE;
import static org.apache.ignite.internal.processors.cache.ClusterState.TRANSITION;

/**
 *
 */
public class GridClusterStateProcessor extends GridProcessorAdapter {
    /** Global status. */
    private volatile ClusterState globalState;

    /** Action context. */
    private volatile ChangeGlobalStateContext lastCgsCtx;

    /** Local action future. */
    private final AtomicReference<GridChangeGlobalStateFuture> cgsLocFut = new AtomicReference<>();

    /** Process. */
    @GridToStringExclude
    private GridCacheProcessor cacheProc;

    /** Shared context. */
    @GridToStringExclude
    private GridCacheSharedContext<?, ?> sharedCtx;

    //todo may be add init latch

    /** Listener. */
    private final GridLocalEventListener lsr = new GridLocalEventListener() {
        @Override public void onEvent(Event evt) {
            assert evt != null;

            final DiscoveryEvent e = (DiscoveryEvent)evt;

            assert e.type() == EVT_NODE_LEFT || e.type() == EVT_NODE_FAILED : this;

            final GridChangeGlobalStateFuture f = cgsLocFut.get();

            if (f != null)
                f.initFut.listen(new CI1<IgniteInternalFuture>() {
                    @Override public void apply(IgniteInternalFuture fut) {
                        f.onDiscoveryEvent(e);
                    }
                });
        }
    };

    /**
     * @param ctx Kernal context.
     */
    public GridClusterStateProcessor(GridKernalContext ctx) {
        super(ctx);
    }

    /** {@inheritDoc} */
    @Override public void start(boolean activeOnStart) throws IgniteCheckedException {
        super.start(activeOnStart);

        globalState = activeOnStart ? ACTIVE : INACTIVE;
        cacheProc = ctx.cache();
        sharedCtx = cacheProc.context();

        sharedCtx.io().addHandler(0,
            GridChangeGlobalStateMessageResponse.class,
            new CI2<UUID, GridChangeGlobalStateMessageResponse>() {
                @Override public void apply(UUID nodeId, GridChangeGlobalStateMessageResponse msg) {
                    processChangeGlobalStateResponse(nodeId, msg);
                }
            });

        ctx.discovery().setCustomEventListener(
            ChangeGlobalStateMessage.class, new CustomEventListener<ChangeGlobalStateMessage>() {
                @Override public void onCustomEvent(
                    AffinityTopologyVersion topVer, ClusterNode snd, ChangeGlobalStateMessage msg) {
                    assert topVer != null;
                    assert snd != null;
                    assert msg != null;

                    boolean activate = msg.activate();

                    ChangeGlobalStateContext actx = lastCgsCtx;

                    if (actx != null && globalState == TRANSITION) {
                        GridChangeGlobalStateFuture f = cgsLocFut.get();

                        if (log.isDebugEnabled())
                            log.debug("Concurrent " + prettyStr(activate) + " [id=" +
                                ctx.localNodeId() + " topVer=" + topVer + " actx=" + actx + ", msg=" + msg + "]");

                        if (f != null && f.requestId.equals(msg.requestId()))
                            f.onDone(new IgniteCheckedException(
                                "Concurrent change state, now in progress=" + (activate)
                                    + ", initiatingNodeId=" + actx.initiatingNodeId
                                    + ", you try=" + (prettyStr(activate)) + ", locNodeId=" + ctx.localNodeId()
                            ));

                        msg.concurrentChangeState();
                    }
                    else {
                        if (log.isInfoEnabled())
                            log.info("Create " + prettyStr(activate) + " context [id=" +
                                ctx.localNodeId() + " topVer=" + topVer + ", reqId=" +
                                msg.requestId() + ", initiatingNodeId=" + msg.initiatorNodeId() + "]");

                        lastCgsCtx = new ChangeGlobalStateContext(
                            msg.requestId(),
                            msg.initiatorNodeId(),
                            msg.getDynamicCacheChangeBatch(),
                            msg.activate());

                        globalState = TRANSITION;
                    }
                }
            });

        ctx.event().addLocalEventListener(lsr, EVT_NODE_LEFT, EVT_NODE_FAILED);
    }

    /** {@inheritDoc} */
    @Override public void stop(boolean cancel) throws IgniteCheckedException {
        super.stop(cancel);

        sharedCtx.io().removeHandler(0, GridChangeGlobalStateMessageResponse.class);
        ctx.event().removeLocalEventListener(lsr, EVT_NODE_LEFT, EVT_NODE_FAILED);

        IgniteCheckedException stopErr = new IgniteInterruptedCheckedException(
            "Node is stopping: " + ctx.igniteInstanceName());

        GridChangeGlobalStateFuture f = cgsLocFut.get();

        if (f != null)
            f.onDone(stopErr);

        cgsLocFut.set(null);
    }

    /** {@inheritDoc} */
    @Nullable @Override public DiscoveryDataExchangeType discoveryDataType() {
        return DiscoveryDataExchangeType.STATE_PROC;
    }

    /** {@inheritDoc} */
    @Override public void collectGridNodeData(DiscoveryDataBag dataBag) {
        dataBag.addGridCommonData(DiscoveryDataExchangeType.STATE_PROC.ordinal(), globalState);
    }

    /** {@inheritDoc} */
    @Override public void onGridDataReceived(DiscoveryDataBag.GridDiscoveryData data) {
        ClusterState state = (ClusterState)data.commonData();

        if (state != null)
            globalState = state;
    }

    /**
     *
     */
    public IgniteInternalFuture<?> changeGlobalState(final boolean activate) {
        if (cacheProc.transactions().tx() != null || sharedCtx.lockedTopologyVersion(null) != null)
            throw new IgniteException("Cannot " + prettyStr(activate) + " cluster, because cache locked on transaction.");

        if ((globalState == ACTIVE && activate) || (this.globalState == INACTIVE && !activate))
            return new GridFinishedFuture<>();

        final UUID requestId = UUID.randomUUID();

        final GridChangeGlobalStateFuture cgsFut = new GridChangeGlobalStateFuture(requestId, activate, ctx);

        if (!cgsLocFut.compareAndSet(null, cgsFut)) {
            GridChangeGlobalStateFuture locF = cgsLocFut.get();

            if (locF.activate == activate)
                return locF;
            else
                return new GridFinishedFuture<>(new IgniteException(
                    "fail " + prettyStr(activate) + ", because now in progress" + prettyStr(locF.activate)));
        }

        try {
            if (ctx.clientNode()) {
                AffinityTopologyVersion topVer = ctx.discovery().topologyVersionEx();

                IgniteCompute comp = ((ClusterGroupAdapter)ctx.cluster().get().forServers())
                    .compute().withAsync();

                if (log.isInfoEnabled())
                    log.info("Send " + prettyStr(activate) + " request from client node [id=" +
                        ctx.localNodeId() + " topVer=" + topVer + " ]");

                comp.run(new ClientChangeGlobalStateComputeRequest(activate));

                comp.future().listen(new CI1<IgniteFuture>() {
                    @Override public void apply(IgniteFuture fut) {
                        try {
                            fut.get();

                            cgsFut.onDone();
                        }
                        catch (Exception e) {
                            cgsFut.onDone(e);
                        }
                    }
                });
            }
            else {
                List<DynamicCacheChangeRequest> reqs = new ArrayList<>();

                DynamicCacheChangeRequest changeGlobalStateReq = new DynamicCacheChangeRequest(
                    requestId, activate ? ACTIVE : INACTIVE, ctx.localNodeId());

                reqs.add(changeGlobalStateReq);

                reqs.addAll(activate ? cacheProc.startAllCachesRequests() : cacheProc.stopAllCachesRequests());

                ChangeGlobalStateMessage changeGlobalStateMsg = new ChangeGlobalStateMessage(
                    requestId, ctx.localNodeId(), activate, new DynamicCacheChangeBatch(reqs));

                try {
                    ctx.discovery().sendCustomEvent(changeGlobalStateMsg);

                    if (ctx.isStopping())
                        cgsFut.onDone(new IgniteCheckedException("Failed to execute " + prettyStr(activate) + " request, " +
                            "node is stopping."));
                }
                catch (IgniteCheckedException e) {
                    log.error("Fail create or send change global state request." + cgsFut, e);

                    cgsFut.onDone(e);
                }
            }
        }
        catch (IgniteCheckedException e) {
            log.error("Fail create or send change global state request." + cgsFut, e);

            cgsFut.onDone(e);
        }

        return cgsFut;
    }

    /**
     *
     */
    public boolean active() {
        ChangeGlobalStateContext actx = lastCgsCtx;

        if (actx != null && !actx.activate && globalState == TRANSITION)
            return true;

        if (actx != null && actx.activate && globalState == TRANSITION)
            return false;

        return globalState == ACTIVE;
    }

    /**
     * @param exchActions Requests.
     * @param topVer Exchange topology version.
     */
    public boolean changeGlobalState(
        ExchangeActions exchActions,
        AffinityTopologyVersion topVer
    ) {
        assert exchActions != null;
        assert topVer != null;

        if (exchActions.newClusterState() != null) {
            ChangeGlobalStateContext cgsCtx = lastCgsCtx;

            assert cgsCtx != null : exchActions;

            cgsCtx.topologyVersion(topVer);

            return true;
        }

        return false;
    }

    /**
     * Invoke from exchange future.
     */
    public Exception onChangeGlobalState() {
        GridChangeGlobalStateFuture f = cgsLocFut.get();

        ChangeGlobalStateContext cgsCtx = lastCgsCtx;

        assert cgsCtx != null;

        if (f != null)
            f.setRemaining(cgsCtx.topVer);

        return cgsCtx.activate ? onActivate(cgsCtx) : onDeActivate(cgsCtx);
    }

    /**
     * @param exs Exs.
     */
    public void onFullResponseMessage(Map<UUID, Exception> exs) {
        assert !F.isEmpty(exs);

        ChangeGlobalStateContext actx = lastCgsCtx;

        actx.setFail();

        // revert change if activation request fail
        if (actx.activate) {
            try {
                cacheProc.onKernalStopCaches(true);

                cacheProc.stopCaches(true);

                sharedCtx.affinity().removeAllCacheInfo();

                if (!ctx.clientNode()) {
                    sharedCtx.database().onDeActivate(ctx);

                    if (sharedCtx.pageStore() != null)
                        sharedCtx.pageStore().onDeActivate(ctx);

                    if (sharedCtx.wal() != null)
                        sharedCtx.wal().onDeActivate(ctx);
                }
            }
            catch (Exception e) {
                for (Map.Entry<UUID, Exception> entry : exs.entrySet())
                    e.addSuppressed(entry.getValue());

                log.error("Fail while revert activation request changes", e);
            }
        }
        else {
            //todo revert change if deactivate request fail
        }

        globalState = actx.activate ? INACTIVE : ACTIVE;

        GridChangeGlobalStateFuture af = cgsLocFut.get();

        if (af != null && af.requestId.equals(actx.requestId)) {
            IgniteCheckedException e = new IgniteCheckedException("see suppressed");

            for (Map.Entry<UUID, Exception> entry : exs.entrySet())
                e.addSuppressed(entry.getValue());

            af.onDone(e);
        }
    }

    /**
     *
     */
    private Exception onActivate(ChangeGlobalStateContext cgsCtx) {
        final boolean client = ctx.clientNode();

        if (log.isInfoEnabled())
            log.info("Start activation process [nodeId=" + this.ctx.localNodeId() + ", client=" + client +
                ", topVer=" + cgsCtx.topVer + "]");

        Collection<CacheConfiguration> cfgs = new ArrayList<>();

        for (DynamicCacheChangeRequest req : cgsCtx.batch.requests()) {
            if (req.startCacheConfiguration() != null)
                cfgs.add(req.startCacheConfiguration());
        }

        try {
            if (!client) {
                sharedCtx.database().lock();

                IgnitePageStoreManager pageStore = sharedCtx.pageStore();

                if (pageStore != null)
                    pageStore.onActivate(ctx);

                if (sharedCtx.wal() != null)
                    sharedCtx.wal().onActivate(ctx);

                sharedCtx.database().initDataBase();

                for (CacheConfiguration cfg : cfgs) {
                    if (CU.isSystemCache(cfg.getName()))
                        if (pageStore != null)
                            pageStore.initializeForCache(cfg);
                }

                for (CacheConfiguration cfg : cfgs) {
                    if (!CU.isSystemCache(cfg.getName()))
                        if (pageStore != null)
                            pageStore.initializeForCache(cfg);
                }

                sharedCtx.database().onActivate(ctx);
            }

            if (log.isInfoEnabled())
                log.info("Success activate wal, dataBase, pageStore [nodeId="
                    + ctx.localNodeId() + ", client=" + client + ", topVer=" + cgsCtx.topVer + "]");

            return null;
        }
        catch (Exception e) {
            log.error("Fail activate wal, dataBase, pageStore [nodeId=" + ctx.localNodeId() + ", client=" + client +
                ", topVer=" + cgsCtx.topVer + "]", e);

            if (!ctx.clientNode())
                sharedCtx.database().unLock();

            return e;
        }
    }

    /**
     *
     */
    public Exception onDeActivate(ChangeGlobalStateContext cgsCtx) {
        final boolean client = ctx.clientNode();

        if (log.isInfoEnabled())
            log.info("Start deactivate process [id=" + ctx.localNodeId() + ", client=" +
                client + ", topVer=" + cgsCtx.topVer + "]");

        try {
            ctx.dataStructures().onDeActivate(ctx);

            ctx.service().onDeActivate(ctx);

            if (log.isInfoEnabled())
                log.info("Success deactivate services, dataStructures, database, pageStore, wal [id=" + ctx.localNodeId() + ", client=" +
                    client + ", topVer=" + cgsCtx.topVer + "]");

            return null;
        }
        catch (Exception e) {
            log.error("DeActivation fail [nodeId=" + ctx.localNodeId() + ", client=" + client +
                ", topVer=" + cgsCtx.topVer + "]", e);

            return e;
        }
        finally {
            if (!client)
                sharedCtx.database().unLock();
        }
    }

    /**
     *
     */
    private void onFinalActivate(final ChangeGlobalStateContext cgsCtx) {
        IgniteInternalFuture<?> asyncActivateFut = ctx.closure().runLocalSafe(new Runnable() {
            @Override public void run() {
                boolean client = ctx.clientNode();

                Exception e = null;

                try {
                    if (!ctx.config().isDaemon())
                        ctx.cacheObjects().onUtilityCacheStarted();

                    ctx.service().onUtilityCacheStarted();

                    ctx.service().onActivate(ctx);

                    ctx.dataStructures().onActivate(ctx);

                    if (log.isInfoEnabled())
                        log.info("Success final activate [nodeId="
                            + ctx.localNodeId() + ", client=" + client + ", topVer=" + cgsCtx.topVer + "]");
                }
                catch (Exception ex) {
                    e = ex;

                    log.error("Fail activate finished [nodeId=" + ctx.localNodeId() + ", client=" + client +
                        ", topVer=" + GridClusterStateProcessor.this.lastCgsCtx.topVer + "]", ex);
                }
                finally {
                    globalState = ACTIVE;

                    sendChangeGlobalStateResponse(cgsCtx.requestId, cgsCtx.initiatingNodeId, e);

                    GridClusterStateProcessor.this.lastCgsCtx = null;
                }
            }
        });

        cgsCtx.setAsyncActivateFut(asyncActivateFut);
    }

    /**
     *
     */
    public void onFinalDeActivate(ChangeGlobalStateContext cgsCtx) {
        final boolean client = ctx.clientNode();

        if (log.isInfoEnabled())
            log.info("Success final deactivate [nodeId="
                + ctx.localNodeId() + ", client=" + client + ", topVer=" + cgsCtx.topVer + "]");

        Exception ex = null;

        try {
            if (!client) {
                sharedCtx.database().onDeActivate(ctx);

                if (sharedCtx.pageStore() != null)
                    sharedCtx.pageStore().onDeActivate(ctx);

                if (sharedCtx.wal() != null)
                    sharedCtx.wal().onDeActivate(ctx);

                sharedCtx.affinity().removeAllCacheInfo();
            }
        }
        catch (Exception e) {
            ex = e;
        }
        finally {
            globalState = INACTIVE;
        }

        sendChangeGlobalStateResponse(cgsCtx.requestId, cgsCtx.initiatingNodeId, ex);

        this.lastCgsCtx = null;
    }

    /**
     *
     */
    public void onExchangeDone() {
        ChangeGlobalStateContext cgsCtx = lastCgsCtx;

        assert cgsCtx != null;

        if (!cgsCtx.isFail()) {
            if (cgsCtx.activate)
                onFinalActivate(cgsCtx);
            else
                onFinalDeActivate(cgsCtx);
        }
        else
            lastCgsCtx = null;
    }

    /**
     * @param initNodeId Initialize node id.
     * @param ex Exception.
     */
    private void sendChangeGlobalStateResponse(UUID requestId, UUID initNodeId, Exception ex) {
        assert requestId != null;
        assert initNodeId != null;

        try {
            GridChangeGlobalStateMessageResponse actResp = new GridChangeGlobalStateMessageResponse(requestId, ex);

            if (log.isDebugEnabled())
                log.debug("Send change global state response [nodeId=" + ctx.localNodeId() +
                    ", topVer=" + ctx.discovery().topologyVersionEx() + ", response=" + actResp + "]");

            if (ctx.localNodeId().equals(initNodeId))
                processChangeGlobalStateResponse(ctx.localNodeId(), actResp);
            else
                sharedCtx.io().send(initNodeId, actResp, SYSTEM_POOL);
        }
        catch (IgniteCheckedException e) {
            log.error("Fail send change global state response to " + initNodeId, e);
        }
    }

    /**
     * @param msg Message.
     */
    private void processChangeGlobalStateResponse(final UUID nodeId, final GridChangeGlobalStateMessageResponse msg) {
        assert nodeId != null;
        assert msg != null;

        if (log.isDebugEnabled())
            log.debug("Received activation response [requestId=" + msg.getRequestId() +
                ", nodeId=" + nodeId + "]");

        ClusterNode node = ctx.discovery().node(nodeId);

        if (node == null) {
            U.warn(log, "Received activation response from unknown node (will ignore) [requestId=" +
                msg.getRequestId() + ']');

            return;
        }

        UUID requestId = msg.getRequestId();

        final GridChangeGlobalStateFuture fut = cgsLocFut.get();

        if (fut != null && !fut.isDone() && requestId.equals(fut.requestId)) {
            fut.initFut.listen(new CI1<IgniteInternalFuture<?>>() {
                @Override public void apply(IgniteInternalFuture<?> f) {
                    fut.onResponse(nodeId, msg);
                }
            });
        }
    }



    /**
     * @param activate Activate.
     */
    private String prettyStr(boolean activate) {
        return activate ? "activate" : "deactivate";
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridClusterStateProcessor.class, this);
    }

    /**
     *
     */
    private static class GridChangeGlobalStateFuture extends GridFutureAdapter {
        /** Request id. */
        @GridToStringInclude
        private final UUID requestId;

        /** Activate. */
        private final boolean activate;

        /** Nodes. */
        @GridToStringInclude
        private final Set<UUID> remaining = new HashSet<>();

        /** Responses. */
        @GridToStringInclude
        private final Map<UUID, GridChangeGlobalStateMessageResponse> resps = new HashMap<>();

        /** Context. */
        @GridToStringExclude
        private final GridKernalContext ctx;

        /** */
        @GridToStringExclude
        private final Object mux = new Object();

        /** */
        @GridToStringInclude
        private final GridFutureAdapter initFut = new GridFutureAdapter();

        /** Grid logger. */
        @GridToStringExclude
        private final IgniteLogger log;

        /**
         *
         */
        public GridChangeGlobalStateFuture(UUID requestId, boolean activate, GridKernalContext ctx) {
            this.requestId = requestId;
            this.activate = activate;
            this.ctx = ctx;
            this.log = ctx.log(getClass());
        }

        /**
         * @param event Event.
         */
        public void onDiscoveryEvent(DiscoveryEvent event) {
            assert event != null;

            if (isDone())
                return;

            boolean allReceived = false;

            synchronized (mux) {
                if (remaining.remove(event.eventNode().id()))
                    allReceived = remaining.isEmpty();
            }

            if (allReceived)
                onAllReceived();
        }

        /**
         *
         */
        public void setRemaining(AffinityTopologyVersion topVer) {
            Collection<ClusterNode> nodes = ctx.discovery().nodes(topVer);

            List<UUID> ids = new ArrayList<>(nodes.size());

            for (ClusterNode n : nodes)
                ids.add(n.id());

            if (log.isDebugEnabled())
                log.debug("Setup remaining node [id=" + ctx.localNodeId() + ", client=" +
                    ctx.clientNode() + ", topVer=" + ctx.discovery().topologyVersionEx() +
                    ", nodes=" + Arrays.toString(ids.toArray()) + "]");

            synchronized (mux) {
                remaining.addAll(ids);
            }

            initFut.onDone();
        }

        /**
         * @param msg Activation message response.
         */
        public void onResponse(UUID nodeId, GridChangeGlobalStateMessageResponse msg) {
            assert msg != null;

            if (isDone())
                return;

            boolean allReceived = false;

            synchronized (mux) {
                if (remaining.remove(nodeId))
                    allReceived = remaining.isEmpty();

                resps.put(nodeId, msg);
            }

            if (allReceived)
                onAllReceived();
        }

        /**
         *
         */
        private void onAllReceived() {
            Throwable e = new Throwable();

            boolean fail = false;

            for (Map.Entry<UUID, GridChangeGlobalStateMessageResponse> entry : resps.entrySet()) {
                GridChangeGlobalStateMessageResponse r = entry.getValue();

                if (r.getError() != null) {
                    fail = true;

                    e.addSuppressed(r.getError());
                }
            }

            if (fail)
                onDone(e);
            else
                onDone();
        }

        /** {@inheritDoc} */
        @Override public boolean onDone(@Nullable Object res, @Nullable Throwable err) {
            ctx.state().cgsLocFut.set(null);

            return super.onDone(res, err);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(GridChangeGlobalStateFuture.class, this);
        }
    }

    /**
     *
     *
     */
    private static class ChangeGlobalStateContext {
        /** Request id. */
        private final UUID requestId;

        /** Initiating node id. */
        private final UUID initiatingNodeId;

        /** Batch requests. */
        private final DynamicCacheChangeBatch batch;

        /** Activate. */
        private final boolean activate;

        /** Topology version. */
        private AffinityTopologyVersion topVer;

        /** Fail. */
        private boolean fail;

        /** Async activate future. */
        private IgniteInternalFuture<?> asyncActivateFut;

        /**
         *
         */
        public ChangeGlobalStateContext(
            UUID requestId,
            UUID initiatingNodeId,
            DynamicCacheChangeBatch batch,
            boolean activate
        ) {
            this.requestId = requestId;
            this.batch = batch;
            this.activate = activate;
            this.initiatingNodeId = initiatingNodeId;
        }

        /**
         * @param topVer Topology version.
         */
        public void topologyVersion(AffinityTopologyVersion topVer) {
            this.topVer = topVer;
        }

        /**
         *
         */
        private void setFail() {
            fail = true;
        }

        /**
         *
         */
        private boolean isFail() {
            return fail;
        }

        /**
         *
         */
        public IgniteInternalFuture<?> getAsyncActivateFut() {
            return asyncActivateFut;
        }

        /**
         * @param asyncActivateFut Async activate future.
         */
        public void setAsyncActivateFut(IgniteInternalFuture<?> asyncActivateFut) {
            this.asyncActivateFut = asyncActivateFut;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(ChangeGlobalStateContext.class, this);
        }
    }

    /**
     *
     */
    private static class ClientChangeGlobalStateComputeRequest implements IgniteRunnable {
        /** */
        private static final long serialVersionUID = 0L;

        /** Activation. */
        private final boolean activation;

        /** Ignite. */
        @IgniteInstanceResource
        private Ignite ignite;

        /**
         *
         */
        private ClientChangeGlobalStateComputeRequest(boolean activation) {
            this.activation = activation;
        }

        /** {@inheritDoc} */
        @Override public void run() {
            ignite.active(activation);
        }
    }
}
