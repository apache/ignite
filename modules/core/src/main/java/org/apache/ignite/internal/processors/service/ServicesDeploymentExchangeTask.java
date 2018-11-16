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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.cluster.ClusterTopologyServerNotFoundException;
import org.apache.ignite.internal.events.DiscoveryCustomEvent;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheAffinityChangeMessage;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.services.ServiceConfiguration;
import org.apache.ignite.services.ServiceDeploymentException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_JOINED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;
import static org.apache.ignite.internal.GridTopic.TOPIC_SERVICES;
import static org.apache.ignite.internal.events.DiscoveryCustomEvent.EVT_DISCOVERY_CUSTOM_EVT;
import static org.apache.ignite.internal.managers.communication.GridIoPolicy.SERVICE_POOL;

/**
 * Services deployment exchange task.
 */
class ServicesDeploymentExchangeTask {
    /** Task's completion future. */
    private final GridFutureAdapter<?> completeStateFut = new GridFutureAdapter<>();

    /** Task's completion of initialization future. */
    private final GridFutureAdapter<?> initTaskFut = new GridFutureAdapter<>();

    /** Task's completion of remaining nodes ids initialization future. */
    private final GridFutureAdapter<?> initCrdFut = new GridFutureAdapter<>();

    /** Coordinator initialization actions mutex. */
    private final Object initCrdMux = new Object();

    /** Remaining nodes to received services single map message. */
    @GridToStringInclude
    private final Set<UUID> remaining = new HashSet<>();

    /** Added in exchange queue flag. */
    private final AtomicBoolean addedInQueue = new AtomicBoolean(false);

    /** Single service messages to process. */
    @GridToStringInclude
    private final Map<UUID, ServicesSingleMapMessage> singleMapMsgs = new HashMap<>();

    /** Expected services assignments. */
    @GridToStringExclude
    private final Map<IgniteUuid, Map<UUID, Integer>> expDeps = new HashMap<>();

    /** Deployment errors. */
    @GridToStringExclude
    private final Map<IgniteUuid, Collection<Throwable>> depErrors = new HashMap<>();

    /** Kernal context. */
    private final GridKernalContext ctx;

    /** Logger. */
    private final IgniteLogger log;

    /** Exchange id. */
    @GridToStringInclude
    private final ServicesDeploymentExchangeId exchId;

    /** Coordinator node id. */
    @GridToStringExclude
    private volatile UUID crdId;

    /** Cause discovery event. */
    @GridToStringInclude
    private volatile DiscoveryEvent evt;

    /** Topology version. */
    @GridToStringInclude
    private volatile AffinityTopologyVersion evtTopVer;

    /** Services deployment actions. */
    private volatile ServicesExchangeActions exchangeActions;

    /**
     * @param ctx Kernal context.
     * @param exchId Service deployment exchange id.
     */
    protected ServicesDeploymentExchangeTask(GridKernalContext ctx, ServicesDeploymentExchangeId exchId) {
        this.exchId = exchId;
        this.ctx = ctx;

        log = ctx.log(getClass());
    }

    /**
     * Handles discovery event receiving.
     *
     * @param evt Discovery event.
     * @param evtTopVer Topology version.
     * @param exchangeActions Services deployment exchange actions.
     */
    protected void onEvent(@NotNull DiscoveryEvent evt, @NotNull AffinityTopologyVersion evtTopVer,
        @Nullable ServicesExchangeActions exchangeActions) {
        assert evt.type() == EVT_NODE_JOINED || evt.type() == EVT_NODE_LEFT || evt.type() == EVT_NODE_FAILED
            || evt.type() == EVT_DISCOVERY_CUSTOM_EVT : "Unexpected event type, evt=" + evt;

        this.evt = evt;
        this.evtTopVer = evtTopVer;
        this.exchangeActions = exchangeActions;
    }

    /**
     * Initializes exchange task.
     *
     * @throws IgniteCheckedException In case of an error.
     */
    protected void init() throws IgniteCheckedException {
        if (isCompleted() || initTaskFut.isDone())
            return;

        assert evt != null && evtTopVer != null : "Illegal state to perform task's initialization :" + this;

        if (log.isDebugEnabled()) {
            log.debug("Started services exchange task init: [exchId=" + exchId +
                ", locId=" + ctx.localNodeId() + ", evt=" + evt + ']');
        }

        try {
            if (exchangeActions != null && exchangeActions.deactivate()) {
                ctx.service().onDeActivate(ctx);

                completeSuccess();

                return;
            }

            if (exchangeActions == null) {
                Map<IgniteUuid, ServiceInfo> toDeploy = new HashMap<>();

                if (evt.type() == EVT_DISCOVERY_CUSTOM_EVT) {
                    DiscoveryCustomMessage msg = ((DiscoveryCustomEvent)evt).customMessage();

                    if (msg instanceof CacheAffinityChangeMessage) {
                        CacheAffinityChangeMessage msg0 = (CacheAffinityChangeMessage)msg;

                        Map<IgniteUuid, ServiceInfo> services = ctx.service().deployedServices();

                        if (!services.isEmpty()) {
                            Map<Integer, Map<Integer, List<UUID>>> change = msg0.assignmentChange();

                            if (change != null) {
                                Set<String> names = new HashSet<>();

                                ctx.cache().cacheDescriptors().forEach((name, desc) -> {
                                    if (change.containsKey(desc.groupId()))
                                        names.add(name);
                                });

                                services.forEach((srvcId, desc) -> {
                                    if (names.contains(desc.cacheName()))
                                        toDeploy.put(srvcId, desc);
                                });
                            }
                        }
                    }
                }
                else {
                    assert evt.type() == EVT_NODE_JOINED || evt.type() == EVT_NODE_LEFT || evt.type() == EVT_NODE_FAILED;

                    toDeploy.putAll(ctx.service().deployedServices());

                    if (evt.type() == EVT_NODE_JOINED)
                        toDeploy.putAll(ctx.service().servicesReceivedFromJoin(evt.eventNode().id()));
                }

                if (toDeploy.isEmpty()) {
                    completeSuccess();

                    if (log.isDebugEnabled())
                        log.debug("No services deployment exchange action required.");

                    return;
                }

                exchangeActions = new ServicesExchangeActions();

                exchangeActions.servicesToDeploy(toDeploy);
            }

            ClusterNode crd = ctx.service().coordinator();

            if (crd == null) {
                onAllServersLeft();

                return;
            }

            crdId = crd.id();

            if (crd.isLocal())
                initCoordinator(evtTopVer);

            processDeploymentActions(exchangeActions);
        }
        catch (Exception e) {
            log.error("Error occurred while initializing exchange task, err=" + e.getMessage(), e);

            completeError(e);

            throw new IgniteCheckedException(e);
        }
        finally {
            if (!initTaskFut.isDone())
                initTaskFut.onDone();

            if (log.isDebugEnabled()) {
                log.debug("Finished services exchange future init: [exchId=" + exchangeId() +
                    ", locId=" + ctx.localNodeId() + ']');
            }
        }
    }

    /**
     * @param exchangeActions Services deployment exchange actions.
     */
    @SuppressWarnings("ErrorNotRethrown")
    private void processDeploymentActions(@NotNull ServicesExchangeActions exchangeActions) {
        final GridServiceProcessor proc = ctx.service();

        proc.updateDeployedServices(exchangeActions);

        exchangeActions.servicesToUndeploy().forEach((srvcId, desc) -> {
            proc.exchange().exchangerBlockingSectionBegin();

            try {
                proc.undeploy(srvcId);
            }
            finally {
                proc.exchange().exchangerBlockingSectionEnd();
            }
        });

        exchangeActions.servicesToDeploy().forEach((srvcId, desc) -> {
            try {
                ServiceConfiguration cfg = desc.configuration();

                Map<UUID, Integer> top = reassign(srvcId, cfg, evtTopVer, filterDeadNodes(desc.topologySnapshot()));

                expDeps.put(srvcId, top);

                Integer expCnt = top.getOrDefault(ctx.localNodeId(), 0);

                if (expCnt > proc.localInstancesCount(srvcId)) {
                    proc.exchange().exchangerBlockingSectionBegin();

                    try {
                        proc.redeploy(srvcId, cfg, top);
                    }
                    finally {
                        proc.exchange().exchangerBlockingSectionEnd();
                    }
                }
            }
            catch (Error | RuntimeException | IgniteCheckedException err) {
                depErrors.computeIfAbsent(srvcId, e -> new ArrayList<>()).add(err);
            }
        });

        createAndSendSingleMapMessage(exchId, depErrors);
    }

    /**
     * Prepares the coordinator to manage exchange.
     *
     * @param topVer Topology version to initialize {@link #remaining} collection.
     */
    private void initCoordinator(AffinityTopologyVersion topVer) {
        synchronized (initCrdMux) {
            if (initCrdFut.isDone())
                return;

            try {
                for (ClusterNode node : ctx.discovery().nodes(topVer)) {
                    if (ctx.discovery().alive(node) && !singleMapMsgs.containsKey(node.id()))
                        remaining.add(node.id());
                }
            }
            catch (Exception e) {
                log.error("Error occurred while initializing remaining collection.", e);

                initCrdFut.onDone(e);
            }
            finally {
                if (!initCrdFut.isDone())
                    initCrdFut.onDone();
            }
        }
    }

    /**
     * @param exchId Exchange id.
     * @param errors Deployment errors.
     */
    private void createAndSendSingleMapMessage(ServicesDeploymentExchangeId exchId,
        final Map<IgniteUuid, Collection<Throwable>> errors) {
        assert crdId != null : "Coordinator should be defined at this point, locId=" + ctx.localNodeId();

        try {
            Map<IgniteUuid, ServiceSingleDeploymentsResults> results = new HashMap<>();

            ctx.service().localInstancesCount().forEach((id, cnt) -> {
                ServiceSingleDeploymentsResults depRes = new ServiceSingleDeploymentsResults(cnt);

                attachDeploymentErrors(depRes, errors.get(id));

                results.put(id, depRes);
            });

            errors.forEach((srvcId, err) -> {
                if (results.containsKey(srvcId))
                    return;

                ServiceSingleDeploymentsResults depRes = new ServiceSingleDeploymentsResults(0);

                attachDeploymentErrors(depRes, err);

                results.put(srvcId, depRes);
            });

            ServicesSingleMapMessage msg = new ServicesSingleMapMessage(exchId, results);

            if (ctx.localNodeId().equals(crdId))
                onReceiveSingleMapMessage(ctx.localNodeId(), msg);
            else
                ctx.io().sendToGridTopic(crdId, TOPIC_SERVICES, msg, SERVICE_POOL);

            if (log.isDebugEnabled())
                log.debug("Send services single map message, msg=" + msg);
        }
        catch (IgniteCheckedException e) {
            log.error("Failed to send services single map message to coordinator over communication spi.", e);
        }
    }

    /**
     * Handles received single node services map message.
     *
     * @param snd Sender node id.
     * @param msg Single services map message.
     */
    protected void onReceiveSingleMapMessage(UUID snd, ServicesSingleMapMessage msg) {
        assert exchId.equals(msg.exchangeId()) : "Wrong message's exchange id, msg=" + msg;

        initCrdFut.listen((IgniteInClosure<IgniteInternalFuture<?>>)fut -> {
            if (isCompleted())
                return;

            synchronized (initCrdMux) {
                if (remaining.remove(snd)) {
                    singleMapMsgs.put(snd, msg);

                    if (remaining.isEmpty())
                        onAllReceived();
                }
                else if (log.isDebugEnabled())
                    log.debug("Unexpected service single map received, msg=" + msg);
            }
        });
    }

    /**
     * Handles received full services map message.
     *
     * @param msg Full services map message.
     */
    protected void onReceiveFullMapMessage(ServicesFullMapMessage msg) {
        assert exchId.equals(msg.exchangeId()) : "Wrong message's exchange id, msg=" + msg;

        initTaskFut.listen((IgniteInClosure<IgniteInternalFuture<?>>)fut -> {
            if (isCompleted())
                return;

            ctx.closure().runLocalSafe(() -> {
                try {
                    ServicesExchangeActions depResults = msg.servicesExchangeActions();

                    assert depResults != null : "Services deployment actions should be attached.";

                    final Map<IgniteUuid, HashMap<UUID, Integer>> fullTops = depResults.deploymentTopologies();
                    final Map<IgniteUuid, Collection<byte[]>> fullErrors = depResults.deploymentErrors();

                    exchangeActions.deploymentTopologies(fullTops);
                    exchangeActions.deploymentErrors(fullErrors);

                    Set<IgniteUuid> toUndeploy = ctx.service().updateServicesTopologies(fullTops, fullErrors);

                    for (IgniteUuid srvcId : toUndeploy)
                        ctx.service().undeploy(srvcId);

                    final Map<IgniteUuid, ServiceInfo> services = ctx.service().deployedServices();

                    fullTops.forEach((srvcId, top) -> {
                        Integer expCnt = top.getOrDefault(ctx.localNodeId(), 0);

                        if (expCnt < ctx.service().localInstancesCount(srvcId)) { // Undeploy exceed instances
                            ServiceInfo desc = services.get(srvcId);

                            assert desc != null;

                            ServiceConfiguration cfg = desc.configuration();

                            ctx.service().redeploy(srvcId, cfg, top);
                        }
                    });

                    completeSuccess();
                }
                catch (Throwable t) {
                    log.error("Failed to process services deployment full map, msg=" + msg, t);

                    completeError(t);
                }
            });
        });
    }

    /**
     * Completes initiating futures.
     *
     * @param err Error to complete initiating.
     */
    private void completeInitiatingFuture(final Throwable err) {
        if (exchangeActions == null)
            return;

        exchangeActions.servicesToDeploy().forEach((srvcId, desc) -> {
            if (err != null) {
                ctx.service().completeInitiatingFuture(true, srvcId, err);

                return;
            }

            Collection<byte[]> errors = exchangeActions.deploymentErrors().get(srvcId);

            if (errors == null) {
                ctx.service().completeInitiatingFuture(true, srvcId, null);

                return;
            }

            ServiceDeploymentException ex = null;

            for (byte[] error : errors) {
                try {
                    Throwable t = U.unmarshal(ctx, error, null);

                    if (ex == null)
                        ex = new ServiceDeploymentException(t, Collections.singleton(desc.configuration()));
                    else
                        ex.addSuppressed(t);
                }
                catch (IgniteCheckedException e) {
                    log.error("Failed to unmarshal deployment exception.", e);
                }
            }

            ctx.service().completeInitiatingFuture(true, srvcId, ex);
        });

        for (IgniteUuid reqSrvcId : exchangeActions.servicesToUndeploy().keySet())
            ctx.service().completeInitiatingFuture(false, reqSrvcId, err);
    }

    /**
     * Creates services full map message and send it over discovery.
     */
    private void onAllReceived() {
        assert !isCompleted();

        Collection<ServiceFullDeploymentsResults> fullResults = buildFullDeploymentsResults(singleMapMsgs);

        try {
            ServicesFullMapMessage msg = new ServicesFullMapMessage(exchId, fullResults);

            ctx.discovery().sendCustomEvent(msg);
        }
        catch (IgniteCheckedException e) {
            log.error("Failed to send services full map message across the ring.", e);
        }
    }

    /**
     * Reassigns service to nodes.
     *
     * @param cfg Service configuration.
     * @param topVer Topology version.
     * @param oldTop Previous topology snapshot.
     * @throws IgniteCheckedException In case of an error.
     */
    private Map<UUID, Integer> reassign(IgniteUuid srvcId, ServiceConfiguration cfg,
        AffinityTopologyVersion topVer, Map<UUID, Integer> oldTop) throws IgniteCheckedException {
        try {
            Map<UUID, Integer> top = ctx.service().reassign(srvcId, cfg, topVer, oldTop);

            if (top.isEmpty())
                throw new IgniteCheckedException("Failed to determine suitable nodes to deploy service.");

            if (log.isDebugEnabled())
                log.debug("Calculated service assignment : [srvcId=" + srvcId + ", srvcTop=" + top + ']');

            return top;
        }
        catch (Throwable e) {
            throw new IgniteCheckedException("Failed to calculate assignments for service, cfg=" + cfg, e);
        }
    }

    /**
     * Filters dead nodes from given service topology snapshot.
     *
     * @param top Service topology snapshot.
     * @return Filtered service topology snapshot.
     */
    private Map<UUID, Integer> filterDeadNodes(Map<UUID, Integer> top) {
        if (F.isEmpty(top))
            return top;

        Map<UUID, Integer> filtered = new HashMap<>();

        top.forEach((nodeId, cnt) -> {
            if (ctx.discovery().alive(nodeId))
                filtered.put(nodeId, cnt);
        });

        return filtered;
    }

    /**
     * Processes single map messages to build full deployment results.
     *
     * @param singleMaps Services single map messages.
     * @return Services full deployments results.
     */
    private Collection<ServiceFullDeploymentsResults> buildFullDeploymentsResults(
        Map<UUID, ServicesSingleMapMessage> singleMaps) {
        final Map<IgniteUuid, Map<UUID, ServiceSingleDeploymentsResults>> singleResults = new HashMap<>();

        singleMaps.forEach((nodeId, msg) -> msg.results().forEach((srvcId, res) -> {
            Map<UUID, ServiceSingleDeploymentsResults> depResults = singleResults
                .computeIfAbsent(srvcId, r -> new HashMap<>());

            int cnt = res.count();

            if (cnt != 0) {
                Map<UUID, Integer> expTop = expDeps.get(srvcId);

                if (expTop != null) {
                    Integer expCnt = expTop.get(nodeId);

                    cnt = expCnt == null ? 0 : Math.min(cnt, expCnt);
                }
            }

            ServiceSingleDeploymentsResults singleDepRes = new ServiceSingleDeploymentsResults(cnt);

            if (!res.errors().isEmpty())
                singleDepRes.errors(res.errors());

            depResults.put(nodeId, singleDepRes);
        }));

        final Collection<ServiceFullDeploymentsResults> fullResults = new ArrayList<>();

        singleResults.forEach((srvcId, dep) -> {
            ServiceFullDeploymentsResults res = new ServiceFullDeploymentsResults(srvcId, dep);

            fullResults.add(res);
        });

        return fullResults;
    }

    /**
     * @param depRes Service single deployments results.
     * @param errors Deployment errors.
     */
    private void attachDeploymentErrors(@NotNull ServiceSingleDeploymentsResults depRes,
        @Nullable Collection<Throwable> errors) {
        if (F.isEmpty(errors))
            return;

        Collection<byte[]> errorsBytes = new ArrayList<>();

        for (Throwable th : errors) {
            try {
                byte[] arr = U.marshal(ctx, th);

                errorsBytes.add(arr);
            }
            catch (IgniteCheckedException e) {
                log.error("Failed to marshal deployment error, err=" + th, e);
            }
        }

        depRes.errors(errorsBytes);
    }

    /**
     * Handles a node leaves topology.
     *
     * @param nodeId Left node id.
     */
    protected void onNodeLeft(UUID nodeId) {
        initTaskFut.listen((IgniteInClosure<IgniteInternalFuture<?>>)fut -> {
            if (isCompleted())
                return;

            final boolean crdChanged = nodeId.equals(crdId);

            if (crdChanged) {
                ClusterNode crd = ctx.service().coordinator();

                if (crd != null) {
                    crdId = crd.id();

                    if (crd.isLocal())
                        initCoordinator(evtTopVer);

                    createAndSendSingleMapMessage(exchId, depErrors);
                }
                else
                    onAllServersLeft();
            }
            else if (ctx.localNodeId().equals(crdId)) {
                synchronized (initCrdMux) {
                    boolean rmvd = remaining.remove(nodeId);

                    if (rmvd && remaining.isEmpty()) {
                        singleMapMsgs.remove(nodeId);

                        onAllReceived();
                    }
                }
            }
        });
    }

    /**
     * Handles case when all server nodes have left the grid.
     */
    private void onAllServersLeft() {
        assert ctx.clientNode();

        completeError(new ClusterTopologyServerNotFoundException("Failed to resolve coordinator to process services " +
            "map exchange: [locId=" + ctx.localNodeId() + "client=" + ctx.clientNode() + "evt=" + evt + ']'));
    }

    /**
     * @return Cause discovery event.
     */
    public DiscoveryEvent event() {
        return evt;
    }

    /**
     * Returns cause of exchange topology version.
     *
     * @return Cause of exchange topology version.
     */
    public AffinityTopologyVersion topologyVersion() {
        return evtTopVer;
    }

    /**
     * Returns services deployment exchange id of the task.
     *
     * @return Services deployment exchange id.
     */
    public ServicesDeploymentExchangeId exchangeId() {
        return exchId;
    }

    /**
     * Completes the task.
     */
    public void completeSuccess() {
        if (!completeStateFut.isDone()) {
            completeInitiatingFuture(null);

            completeStateFut.onDone();
        }

        if (!initTaskFut.isDone())
            initTaskFut.onDone();

        if (!initCrdFut.isDone())
            initCrdFut.onDone();
    }

    /**
     * @param err Error to complete with.
     */
    public void completeError(Throwable err) {
        if (!completeStateFut.isDone()) {
            completeInitiatingFuture(err);

            completeStateFut.onDone(err);
        }

        if (!initTaskFut.isDone())
            initTaskFut.onDone(err);

        if (!initCrdFut.isDone())
            initCrdFut.onDone(err);
    }

    /**
     * Releases resources to reduce memory usages.
     */
    protected void clear() {
        singleMapMsgs.clear();
        expDeps.clear();
        depErrors.clear();
        remaining.clear();

        if (evt instanceof DiscoveryCustomEvent)
            ((DiscoveryCustomEvent)evt).customMessage(null);
    }

    /**
     * Returns if the task completed.
     *
     * @return {@code true} if the task completed, otherwise {@code false}.
     */
    protected boolean isCompleted() {
        return completeStateFut.isDone();
    }

    /**
     * Synchronously waits for completion of the task for up to the given timeout.
     *
     * @param timeout The maximum time to wait in milliseconds.
     * @throws IgniteCheckedException In case of an error.
     */
    protected void waitForComplete(long timeout) throws IgniteCheckedException {
        completeStateFut.get(timeout);
    }

    /**
     * Handles when this task is being added in exchange queue.
     * <p/>
     * Introduced to avoid overhead on calling of {@link Collection#contains(Object)}}.
     *
     * @return {@code true} if task is has not been added previously, otherwise {@code false}.
     */
    protected boolean onAdditionInQueue() {
        return addedInQueue.compareAndSet(false, true);
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        ServicesDeploymentExchangeTask task = (ServicesDeploymentExchangeTask)o;

        return exchId.equals(task.exchId);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return exchId.hashCode();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ServicesDeploymentExchangeTask.class, this,
            "locNodeId", (ctx != null ? ctx.localNodeId() : "unknown"),
            "crdId", crdId);
    }
}