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

package org.apache.ignite.internal.processors.service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteFutureTimeoutCheckedException;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.cluster.ClusterTopologyServerNotFoundException;
import org.apache.ignite.internal.events.DiscoveryCustomEvent;
import org.apache.ignite.internal.managers.communication.GridMessageListener;
import org.apache.ignite.internal.managers.discovery.DiscoCache;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.managers.eventstorage.DiscoveryEventListener;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheAffinityChangeMessage;
import org.apache.ignite.internal.processors.cache.DynamicCacheChangeBatch;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture;
import org.apache.ignite.internal.processors.cluster.ChangeGlobalStateFinishMessage;
import org.apache.ignite.internal.processors.cluster.ChangeGlobalStateMessage;
import org.apache.ignite.internal.util.GridSpinBusyLock;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.util.worker.GridWorker;
import org.apache.ignite.thread.IgniteThread;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_LONG_OPERATIONS_DUMP_TIMEOUT_LIMIT;
import static org.apache.ignite.IgniteSystemProperties.getLong;
import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_JOINED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;
import static org.apache.ignite.failure.FailureType.CRITICAL_ERROR;
import static org.apache.ignite.failure.FailureType.SYSTEM_WORKER_TERMINATION;
import static org.apache.ignite.internal.GridTopic.TOPIC_SERVICES;
import static org.apache.ignite.internal.events.DiscoveryCustomEvent.EVT_DISCOVERY_CUSTOM_EVT;

/**
 * Services deployment exchange manager.
 */
public class ServicesDeploymentExchangeManager {
    /** Busy lock. */
    private final GridSpinBusyLock busyLock = new GridSpinBusyLock();

    /** Services discovery messages listener. */
    private final DiscoveryEventListener discoLsnr = new ServiceDiscoveryListener();

    /** Services communication messages listener. */
    private final GridMessageListener commLsnr = new ServiceCommunicationListener();

    /** Services deployments tasks. */
    private final Map<ServicesDeploymentExchangeId, ServicesDeploymentExchangeTask> tasks = new ConcurrentHashMap<>();

    /** Discovery events received while cluster state transition was in progress. */
    private final List<PendingEventHolder> pendingEvts = new ArrayList<>();

    /** Topology version of latest deployment task's event. */
    private final AtomicReference<AffinityTopologyVersion> readyTopVer =
        new AtomicReference<>(AffinityTopologyVersion.NONE);

    /** Kernal context. */
    private final GridKernalContext ctx;

    /** Logger. */
    private final IgniteLogger log;

    /** Exchange worker. */
    private final ServicesDeploymentExchangeWorker exchWorker;

    /** Default dump operation limit. */
    private final long dfltDumpTimeoutLimit;

    /**
     * @param ctx Grid kernal context.
     */
    protected ServicesDeploymentExchangeManager(@NotNull GridKernalContext ctx) {
        this.ctx = ctx;

        log = ctx.log(getClass());

        ctx.event().addDiscoveryEventListener(discoLsnr,
            EVT_NODE_JOINED, EVT_NODE_LEFT, EVT_NODE_FAILED, EVT_DISCOVERY_CUSTOM_EVT);

        ctx.io().addMessageListener(TOPIC_SERVICES, commLsnr);

        exchWorker = new ServicesDeploymentExchangeWorker();

        long limit = getLong(IGNITE_LONG_OPERATIONS_DUMP_TIMEOUT_LIMIT, 0);

        dfltDumpTimeoutLimit = limit <= 0 ? 30 * 60_000 : limit;
    }

    /**
     * Starts processing of services deployments exchange tasks.
     */
    protected void startProcessing() {
        assert exchWorker.runner() == null : "Method shouldn't be called twice during lifecycle;";

        new IgniteThread(ctx.igniteInstanceName(), "services-deployment-exchange-worker", exchWorker).start();
    }

    /**
     * Stops processing of services deployments exchange tasks.
     *
     * @param stopErr Cause error of exchange manager stop.
     */
    protected void stopProcessing(IgniteCheckedException stopErr) {
        try {
            busyLock.block(); // Will not release it.

            ctx.event().removeDiscoveryEventListener(discoLsnr);

            ctx.io().removeMessageListener(commLsnr);

            U.cancel(exchWorker);

            U.join(exchWorker, log);

            exchWorker.tasksQueue.clear();

            pendingEvts.clear();

            tasks.values().forEach(t -> t.completeError(stopErr));

            tasks.clear();
        }
        catch (Exception e) {
            log.error("Error occurred during stopping exchange worker.", e);
        }
    }

    /**
     * @return Ready topology version.
     */
    public AffinityTopologyVersion readyTopologyVersion() {
        return readyTopVer.get();
    }

    /**
     * Special handler for local discovery events for which the regular events are not generated, e.g. local join and
     * client reconnect events.
     *
     * @param evt Discovery event.
     * @param discoCache Discovery cache.
     */
    protected void onLocalJoin(DiscoveryEvent evt, DiscoCache discoCache, ServicesExchangeActions exchangeActions) {
        checkClusterStateAndAddTask(evt, discoCache, exchangeActions);
    }

    /**
     * Invokes {@link GridWorker#blockingSectionBegin()} for service deployment exchange worker.
     * <p/>
     * Should be called from service deployment exchange worker thread.
     */
    protected void exchangerBlockingSectionBegin() {
        assert exchWorker != null && Thread.currentThread() == exchWorker.runner();

        exchWorker.blockingSectionBegin();
    }

    /**
     * Invokes {@link GridWorker#blockingSectionEnd()} for service deployment exchange worker.
     * <p/>
     * Should be called from service deployment exchange worker thread.
     */
    protected void exchangerBlockingSectionEnd() {
        assert exchWorker != null && Thread.currentThread() == exchWorker.runner();

        exchWorker.blockingSectionEnd();
    }

    /**
     * Checks cluster state and handles given event.
     * <pre>
     * - if cluster is active, then adds event in exchange queue;
     * - if cluster state in transition, them adds to pending events;
     * - if cluster is inactive, then ignore event;
     * </pre>
     * <b>Should be called from discovery thread.</b>
     *
     * @param evt Discovery event.
     * @param discoCache Discovery cache.
     * @param exchangeActions Services deployment exchange actions.
     */
    private void checkClusterStateAndAddTask(@NotNull DiscoveryEvent evt, @NotNull DiscoCache discoCache,
        @Nullable ServicesExchangeActions exchangeActions) {
        if (discoCache.state().transition())
            pendingEvts.add(new PendingEventHolder(evt, discoCache.version(), exchangeActions));
        else if (discoCache.state().active())
            addTask(evt, discoCache.version(), exchangeActions);
        else if (log.isDebugEnabled())
            log.debug("Ignore event, cluster is inactive, evt=" + evt);
    }

    /**
     * Adds exchange task with given exchange id.
     * </p>
     * <b>Should be called from discovery thread.</b>
     *
     * @param evt Discovery event.
     * @param topVer Topology version.
     * @param exchangeActions Services deployment exchange actions.
     */
    private void addTask(@NotNull DiscoveryEvent evt, @NotNull AffinityTopologyVersion topVer,
        @Nullable ServicesExchangeActions exchangeActions) {
        final ServicesDeploymentExchangeId exchId = exchangeId(evt, topVer);

        ServicesDeploymentExchangeTask task = tasks.computeIfAbsent(exchId,
            t -> new ServicesDeploymentExchangeTask(ctx, exchId));

        if (!task.onAdditionInQueue()) {
            log.warning("Do not start service deployment exchange for event: " + evt);

            return;
        }

        assert task.event() == null && task.topologyVersion() == null;

        task.onEvent(evt, topVer, exchangeActions);

        exchWorker.tasksQueue.add(task);
    }

    /**
     * Creates service exchange id.
     *
     * @param evt Discovery event.
     * @param topVer Topology version.
     * @return Services deployment exchange id.
     */
    private ServicesDeploymentExchangeId exchangeId(@NotNull DiscoveryEvent evt,
        @NotNull AffinityTopologyVersion topVer) {
        return evt instanceof DiscoveryCustomEvent ?
            new ServicesDeploymentExchangeId(((DiscoveryCustomEvent)evt).customMessage().id()) :
            new ServicesDeploymentExchangeId(topVer);
    }

    /**
     * Clones some instances of {@link DiscoveryCustomEvent} to capture necessary data, to avoid custom messages's
     * nullifying by {@link GridDhtPartitionsExchangeFuture#onDone}.
     *
     * @param evt Discovery event.
     * @return Discovery event to process.
     */
    private DiscoveryCustomEvent copyIfNeeded(@NotNull DiscoveryCustomEvent evt) {
        DiscoveryCustomMessage msg = evt.customMessage();

        assert msg != null : "DiscoveryCustomMessage has been nullified concurrently, evt=" + evt;

        if (msg instanceof DynamicServicesChangeRequestBatchMessage)
            return evt;

        DiscoveryCustomEvent cp = new DiscoveryCustomEvent();

        cp.node(evt.node());
        cp.customMessage(msg);
        cp.eventNode(evt.eventNode());
        cp.affinityTopologyVersion(evt.affinityTopologyVersion());

        return cp;
    }

    /**
     * Services discovery messages listener.
     */
    private class ServiceDiscoveryListener implements DiscoveryEventListener {
        /** {@inheritDoc} */
        @Override public void onEvent(final DiscoveryEvent evt, final DiscoCache discoCache) {
            if (!enterBusy())
                return;

            final UUID snd = evt.eventNode().id();
            final int evtType = evt.type();

            assert snd != null : "Event's node id shouldn't be null.";
            assert evtType == EVT_NODE_JOINED || evtType == EVT_NODE_LEFT || evtType == EVT_NODE_FAILED
                || evtType == EVT_DISCOVERY_CUSTOM_EVT : "Unexpected event was received, evt=" + evt;

            try {
                if (evtType == EVT_DISCOVERY_CUSTOM_EVT) {
                    DiscoveryCustomMessage msg = ((DiscoveryCustomEvent)evt).customMessage();

                    if (msg instanceof ChangeGlobalStateFinishMessage) {
                        ChangeGlobalStateFinishMessage msg0 = (ChangeGlobalStateFinishMessage)msg;

                        if (msg0.clusterActive())
                            pendingEvts.forEach(t -> addTask(t.evt, t.topVer, t.exchangeActions));
                        else if (log.isDebugEnabled())
                            pendingEvts.forEach(t -> log.debug("Ignore event, cluster is inactive: " + t.evt));

                        pendingEvts.clear();
                    }
                    else {
                        if (msg instanceof ServicesFullMapMessage) {
                            ServicesFullMapMessage msg0 = (ServicesFullMapMessage)msg;

                            if (log.isDebugEnabled()) {
                                log.debug("Received services full map message : " +
                                    "[locId=" + ctx.localNodeId() + ", snd=" + snd + ", msg=" + msg0 + ']');
                            }

                            ServicesDeploymentExchangeId exchId = msg0.exchangeId();

                            assert exchId != null;

                            ServicesDeploymentExchangeTask task = tasks.get(exchId);

                            if (task != null) // May be null in case of double delivering
                                task.onReceiveFullMapMessage(msg0);
                        }
                        else if (msg instanceof CacheAffinityChangeMessage)
                            addTask(copyIfNeeded((DiscoveryCustomEvent)evt), discoCache.version(), null);
                        else {
                            ServicesExchangeActions exchangeActions = null;

                            if (msg instanceof ChangeGlobalStateMessage)
                                exchangeActions = ((ChangeGlobalStateMessage)msg).servicesExchangeActions();
                            else if (msg instanceof DynamicServicesChangeRequestBatchMessage) {
                                exchangeActions = ((DynamicServicesChangeRequestBatchMessage)msg)
                                    .servicesExchangeActions();
                            }
                            else if (msg instanceof DynamicCacheChangeBatch)
                                exchangeActions = ((DynamicCacheChangeBatch)msg).servicesExchangeActions();

                            if (exchangeActions != null)
                                addTask(copyIfNeeded((DiscoveryCustomEvent)evt), discoCache.version(), exchangeActions);
                        }
                    }
                }
                else {
                    if (evtType == EVT_NODE_LEFT || evtType == EVT_NODE_FAILED)
                        tasks.values().forEach(t -> t.onNodeLeft(snd));

                    checkClusterStateAndAddTask(evt, discoCache, null);
                }
            }
            finally {
                leaveBusy();
            }
        }
    }

    /**
     * Pending event's holder.
     */
    private static class PendingEventHolder {
        /** Discovery event. */
        private DiscoveryEvent evt;

        /** Topology version. */
        private AffinityTopologyVersion topVer;

        /** Services deployemnt actions. */
        private ServicesExchangeActions exchangeActions;

        /**
         * @param evt Discovery event.
         * @param topVer Topology version.
         * @param exchangeActions Services deployment exchange actions.
         */
        private PendingEventHolder(DiscoveryEvent evt,
            AffinityTopologyVersion topVer, ServicesExchangeActions exchangeActions) {
            this.evt = evt;
            this.topVer = topVer;
            this.exchangeActions = exchangeActions;
        }
    }

    /**
     * Services messages communication listener.
     */
    private class ServiceCommunicationListener implements GridMessageListener {
        /** {@inheritDoc} */
        @Override public void onMessage(UUID nodeId, Object msg, byte plc) {
            if (!enterBusy())
                return;

            try {
                if (msg instanceof ServicesSingleMapMessage) {
                    ServicesSingleMapMessage msg0 = (ServicesSingleMapMessage)msg;

                    if (log.isDebugEnabled()) {
                        log.debug("Received services single map message : " +
                            "[locId=" + ctx.localNodeId() + ", snd=" + nodeId + ", msg=" + msg0 + ']');
                    }

                    tasks.computeIfAbsent(msg0.exchangeId(),
                        t -> new ServicesDeploymentExchangeTask(ctx, msg0.exchangeId()))
                        .onReceiveSingleMapMessage(nodeId, msg0);
                }
            }
            finally {
                leaveBusy();
            }
        }
    }

    /**
     * Services deployment exchange worker.
     */
    private class ServicesDeploymentExchangeWorker extends GridWorker {
        /** Queue to process. */
        private final LinkedBlockingQueue<ServicesDeploymentExchangeTask> tasksQueue = new LinkedBlockingQueue<>();

        /** {@inheritDoc} */
        private ServicesDeploymentExchangeWorker() {
            super(ctx.igniteInstanceName(), "services-deployment-exchanger",
                ServicesDeploymentExchangeManager.this.log, ctx.workersRegistry());
        }

        /** {@inheritDoc} */
        @Override protected void body() throws InterruptedException, IgniteInterruptedCheckedException {
            Throwable err = null;

            try {
                ServicesDeploymentExchangeTask task;

                while (!isCancelled()) {
                    onIdle();

                    blockingSectionBegin();

                    try {
                        task = tasksQueue.take();
                    }
                    finally {
                        blockingSectionEnd();
                    }

                    if (isCancelled())
                        Thread.currentThread().interrupt();

                    task.init();

                    final long dumpTimeout = 2 * ctx.config().getNetworkTimeout();

                    long dumpCnt = 0;
                    long nextDumpTime = 0;

                    while (true) {
                        try {
                            blockingSectionBegin();

                            try {
                                task.waitForComplete(dumpTimeout);
                            }
                            finally {
                                blockingSectionEnd();
                            }

                            taskPostProcessing(task);

                            break;
                        }
                        catch (IgniteFutureTimeoutCheckedException ignored) {
                            if (isCancelled())
                                return;

                            if (nextDumpTime <= U.currentTimeMillis()) {
                                log.warning("Failed to wait service deployment exchange or timeout had been" +
                                    " reached, timeout=" + dumpTimeout + ", task=" + task);

                                long nextTimeout = dumpTimeout * (2 + dumpCnt++);

                                nextDumpTime = U.currentTimeMillis() + Math.min(nextTimeout, dfltDumpTimeoutLimit);
                            }
                        }
                        catch (ClusterTopologyServerNotFoundException e) {
                            U.error(log, e);

                            taskPostProcessing(task);

                            break;
                        }
                    }
                }
            }
            catch (InterruptedException | IgniteInterruptedCheckedException e) {
                Thread.currentThread().interrupt();

                if (!isCancelled())
                    err = e;
            }
            catch (Throwable t) {
                err = t;
            }
            finally {
                if (err == null && !isCancelled())
                    err = new IllegalStateException("Worker " + name() + " is terminated unexpectedly.");

                if (err instanceof OutOfMemoryError)
                    ctx.failure().process(new FailureContext(CRITICAL_ERROR, err));
                else if (err != null)
                    ctx.failure().process(new FailureContext(SYSTEM_WORKER_TERMINATION, err));
            }
        }

        /**
         * Does additional actions after task's completion.
         */
        private void taskPostProcessing(ServicesDeploymentExchangeTask task) {
            AffinityTopologyVersion readyVer = readyTopVer.get();

            readyTopVer.compareAndSet(readyVer, task.topologyVersion());

            tasks.remove(task.exchangeId());

            task.clear();
        }
    }

    /**
     * Enters busy state.
     *
     * @return {@code true} if entered to busy state.
     */
    private boolean enterBusy() {
        return busyLock.enterBusy();
    }

    /**
     * Leaves busy state.
     */
    private void leaveBusy() {
        busyLock.leaveBusy();
    }
}
