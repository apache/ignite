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
import org.apache.ignite.internal.managers.eventstorage.HighPriorityListener;
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
 * Service deployment manager.
 *
 * @see ServiceDeploymentTask
 * @see ServiceDeploymentActions
 */
public class ServiceDeploymentManager {
    /** Busy lock. */
    private final GridSpinBusyLock busyLock = new GridSpinBusyLock();

    /** Services discovery messages listener. */
    private final DiscoveryEventListener discoLsnr = new ServiceDiscoveryListener();

    /** Services communication messages listener. */
    private final GridMessageListener commLsnr = new ServiceCommunicationListener();

    /** Services deployments tasks. */
    private final Map<ServiceDeploymentProcessId, ServiceDeploymentTask> tasks = new ConcurrentHashMap<>();

    /** Discovery events received while cluster state transition was in progress. */
    private final List<PendingEventHolder> pendingEvts = new ArrayList<>();

    /** Topology version of latest deployment task's event. */
    private final AtomicReference<AffinityTopologyVersion> readyTopVer =
        new AtomicReference<>(AffinityTopologyVersion.NONE);

    /** Kernal context. */
    private final GridKernalContext ctx;

    /** Logger. */
    private final IgniteLogger log;

    /** Deployment worker. */
    private final ServicesDeploymentWorker depWorker;

    /** Default dump operation limit. */
    private final long dfltDumpTimeoutLimit;

    /**
     * @param ctx Grid kernal context.
     */
    ServiceDeploymentManager(@NotNull GridKernalContext ctx) {
        this.ctx = ctx;

        log = ctx.log(getClass());

        ctx.event().addDiscoveryEventListener(discoLsnr,
            EVT_NODE_JOINED, EVT_NODE_LEFT, EVT_NODE_FAILED, EVT_DISCOVERY_CUSTOM_EVT);

        ctx.io().addMessageListener(TOPIC_SERVICES, commLsnr);

        depWorker = new ServicesDeploymentWorker();

        long limit = getLong(IGNITE_LONG_OPERATIONS_DUMP_TIMEOUT_LIMIT, 0);

        dfltDumpTimeoutLimit = limit <= 0 ? 30 * 60_000 : limit;
    }

    /**
     * Starts processing of services deployments tasks.
     */
    void startProcessing() {
        assert depWorker.runner() == null : "Method shouldn't be called twice during lifecycle;";

        new IgniteThread(ctx.igniteInstanceName(), "services-deployment-worker", depWorker).start();
    }

    /**
     * Stops processing of services deployments tasks.
     *
     * @param stopErr Cause error of deployment manager stop.
     */
    void stopProcessing(IgniteCheckedException stopErr) {
        busyLock.block();

        try {
            ctx.event().removeDiscoveryEventListener(discoLsnr);

            ctx.io().removeMessageListener(commLsnr);

            U.cancel(depWorker);

            U.join(depWorker, log);

            depWorker.tasksQueue.clear();

            pendingEvts.clear();

            tasks.values().forEach(t -> t.completeError(stopErr));

            tasks.clear();
        }
        finally {
            busyLock.unblock();
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
     * @param depActions Service deployment actions.
     */
    void onLocalJoin(DiscoveryEvent evt, DiscoCache discoCache, ServiceDeploymentActions depActions) {
        checkClusterStateAndAddTask(evt, discoCache, depActions);
    }

    /**
     * Invokes {@link GridWorker#blockingSectionBegin()} for service deployment worker.
     * <p/>
     * Should be called from service deployment worker thread.
     */
    void deployerBlockingSectionBegin() {
        assert depWorker != null && Thread.currentThread() == depWorker.runner();

        depWorker.blockingSectionBegin();
    }

    /**
     * Invokes {@link GridWorker#blockingSectionEnd()} for service deployment worker.
     * <p/>
     * Should be called from service deployment worker thread.
     */
    void deployerBlockingSectionEnd() {
        assert depWorker != null && Thread.currentThread() == depWorker.runner();

        depWorker.blockingSectionEnd();
    }

    /**
     * Checks cluster state and handles given event.
     * <pre>
     * - if cluster is active, then adds event in deployment queue;
     * - if cluster state in transition, them adds to pending events;
     * - if cluster is inactive, then ignore event;
     * </pre>
     * <b>Should be called from discovery thread.</b>
     *
     * @param evt Discovery event.
     * @param discoCache Discovery cache.
     * @param depActions Services deployment actions.
     */
    private void checkClusterStateAndAddTask(@NotNull DiscoveryEvent evt, @NotNull DiscoCache discoCache,
        @Nullable ServiceDeploymentActions depActions) {
        if (discoCache.state().transition())
            pendingEvts.add(new PendingEventHolder(evt, discoCache.version(), depActions));
        else if (discoCache.state().active())
            addTask(evt, discoCache.version(), depActions);
        else if (log.isDebugEnabled())
            log.debug("Ignore event, cluster is inactive, evt=" + evt);
    }

    /**
     * Adds deployment task with given deployment process id.
     * </p>
     * <b>Should be called from discovery thread.</b>
     *
     * @param evt Discovery event.
     * @param topVer Topology version.
     * @param depActions Services deployment actions.
     */
    private void addTask(@NotNull DiscoveryEvent evt, @NotNull AffinityTopologyVersion topVer,
        @Nullable ServiceDeploymentActions depActions) {
        final ServiceDeploymentProcessId depId = deploymentId(evt, topVer);

        ServiceDeploymentTask task = tasks.computeIfAbsent(depId,
            t -> new ServiceDeploymentTask(ctx, depId));

        if (!task.onEnqueued()) {
            if (log.isDebugEnabled()) {
                log.debug("Service deployment process hasn't been started for discovery event, because of " +
                    "a task with the same deployment process id is already added (possible cause is message's" +
                    " double delivering), evt=" + evt);
            }

            return;
        }

        assert task.event() == null && task.topologyVersion() == null;

        task.onEvent(evt, topVer, depActions);

        depWorker.tasksQueue.add(task);
    }

    /**
     * Creates service deployment process id.
     *
     * @param evt Discovery event.
     * @param topVer Topology version.
     * @return Services deployment process id.
     */
    private ServiceDeploymentProcessId deploymentId(@NotNull DiscoveryEvent evt,
        @NotNull AffinityTopologyVersion topVer) {
        return evt instanceof DiscoveryCustomEvent ?
            new ServiceDeploymentProcessId(((DiscoveryCustomEvent)evt).customMessage().id()) :
            new ServiceDeploymentProcessId(topVer);
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

        if (msg instanceof ServiceChangeBatchRequest)
            return evt;

        DiscoveryCustomEvent cp = new DiscoveryCustomEvent();

        cp.node(evt.node());
        cp.customMessage(msg);
        cp.eventNode(evt.eventNode());
        cp.affinityTopologyVersion(evt.affinityTopologyVersion());

        return cp;
    }

    /**
     * Services discovery messages high priority listener.
     * <p/>
     * The listener should be notified earlier then PME's listener because of a custom message of {@link
     * DiscoveryCustomEvent} may be nullified in PME before the listener will be able to capture it.
     */
    private class ServiceDiscoveryListener implements DiscoveryEventListener, HighPriorityListener {
        /** {@inheritDoc} */
        @Override public void onEvent(final DiscoveryEvent evt, final DiscoCache discoCache) {
            if (!enterBusy())
                return;

            try {
                final UUID snd = evt.eventNode().id();
                final int evtType = evt.type();

                assert snd != null : "Event's node id shouldn't be null.";
                assert evtType == EVT_NODE_JOINED || evtType == EVT_NODE_LEFT || evtType == EVT_NODE_FAILED
                    || evtType == EVT_DISCOVERY_CUSTOM_EVT : "Unexpected event was received, evt=" + evt;

                if (evtType == EVT_DISCOVERY_CUSTOM_EVT) {
                    DiscoveryCustomMessage msg = ((DiscoveryCustomEvent)evt).customMessage();

                    if (msg instanceof ChangeGlobalStateFinishMessage) {
                        ChangeGlobalStateFinishMessage msg0 = (ChangeGlobalStateFinishMessage)msg;

                        if (msg0.clusterActive())
                            pendingEvts.forEach(t -> addTask(t.evt, t.topVer, t.depActions));
                        else if (log.isDebugEnabled())
                            pendingEvts.forEach(t -> log.debug("Ignore event, cluster is inactive: " + t.evt));

                        pendingEvts.clear();
                    }
                    else {
                        if (msg instanceof ServiceClusterDeploymentResultBatch) {
                            ServiceClusterDeploymentResultBatch msg0 = (ServiceClusterDeploymentResultBatch)msg;

                            if (log.isDebugEnabled()) {
                                log.debug("Received services full deployments message : " +
                                    "[locId=" + ctx.localNodeId() + ", snd=" + snd + ", msg=" + msg0 + ']');
                            }

                            ServiceDeploymentProcessId depId = msg0.deploymentId();

                            assert depId != null;

                            ServiceDeploymentTask task = tasks.get(depId);

                            if (task != null) // May be null in case of double delivering
                                task.onReceiveFullDeploymentsMessage(msg0);
                        }
                        else if (msg instanceof CacheAffinityChangeMessage)
                            addTask(copyIfNeeded((DiscoveryCustomEvent)evt), discoCache.version(), null);
                        else {
                            ServiceDeploymentActions depActions = null;

                            if (msg instanceof ChangeGlobalStateMessage)
                                depActions = ((ChangeGlobalStateMessage)msg).servicesDeploymentActions();
                            else if (msg instanceof ServiceChangeBatchRequest) {
                                depActions = ((ServiceChangeBatchRequest)msg)
                                    .servicesDeploymentActions();
                            }
                            else if (msg instanceof DynamicCacheChangeBatch)
                                depActions = ((DynamicCacheChangeBatch)msg).servicesDeploymentActions();

                            if (depActions != null)
                                addTask(copyIfNeeded((DiscoveryCustomEvent)evt), discoCache.version(), depActions);
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

        /** {@inheritDoc} */
        @Override public int order() {
            return 0;
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
        private ServiceDeploymentActions depActions;

        /**
         * @param evt Discovery event.
         * @param topVer Topology version.
         * @param depActions Services deployment actions.
         */
        private PendingEventHolder(DiscoveryEvent evt,
            AffinityTopologyVersion topVer, ServiceDeploymentActions depActions) {
            this.evt = evt;
            this.topVer = topVer;
            this.depActions = depActions;
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
                if (msg instanceof ServiceSingleNodeDeploymentResultBatch) {
                    ServiceSingleNodeDeploymentResultBatch msg0 = (ServiceSingleNodeDeploymentResultBatch)msg;

                    if (log.isDebugEnabled()) {
                        log.debug("Received services single deployments message : " +
                            "[locId=" + ctx.localNodeId() + ", snd=" + nodeId + ", msg=" + msg0 + ']');
                    }

                    tasks.computeIfAbsent(msg0.deploymentId(),
                        t -> new ServiceDeploymentTask(ctx, msg0.deploymentId()))
                        .onReceiveSingleDeploymentsMessage(nodeId, msg0);
                }
            }
            finally {
                leaveBusy();
            }
        }
    }

    /**
     * Services deployment worker.
     */
    private class ServicesDeploymentWorker extends GridWorker {
        /** Queue to process. */
        private final LinkedBlockingQueue<ServiceDeploymentTask> tasksQueue = new LinkedBlockingQueue<>();

        /** {@inheritDoc} */
        private ServicesDeploymentWorker() {
            super(ctx.igniteInstanceName(), "services-deployment-worker",
                ServiceDeploymentManager.this.log, ctx.workersRegistry());
        }

        /** {@inheritDoc} */
        @Override protected void body() throws InterruptedException, IgniteInterruptedCheckedException {
            Throwable err = null;

            try {
                ServiceDeploymentTask task;

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
                                log.warning("Failed to wait service deployment process or timeout had been" +
                                    " reached, timeout=" + dumpTimeout +
                                    (log.isDebugEnabled() ? ", task=" + task : ", taskDepId=" + task.deploymentId()));

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
        private void taskPostProcessing(ServiceDeploymentTask task) {
            AffinityTopologyVersion readyVer = readyTopVer.get();

            readyTopVer.compareAndSet(readyVer, task.topologyVersion());

            tasks.remove(task.deploymentId());
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
