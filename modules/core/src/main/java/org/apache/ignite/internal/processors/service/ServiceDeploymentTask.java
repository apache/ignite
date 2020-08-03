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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
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
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_JOINED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;
import static org.apache.ignite.internal.GridTopic.TOPIC_SERVICES;
import static org.apache.ignite.internal.events.DiscoveryCustomEvent.EVT_DISCOVERY_CUSTOM_EVT;
import static org.apache.ignite.internal.managers.communication.GridIoPolicy.SERVICE_POOL;

/**
 * Services deployment task.
 *
 * @see ServiceDeploymentActions
 * @see ServiceSingleNodeDeploymentResultBatch
 * @see ServiceClusterDeploymentResultBatch
 */
class ServiceDeploymentTask {
    /** Task's completion future. */
    private final GridFutureAdapter<?> completeStateFut = new GridFutureAdapter<>();

    /** Task's completion of initialization future. */
    private final GridFutureAdapter<?> initTaskFut = new GridFutureAdapter<>();

    /** Task's completion of remaining nodes ids initialization future. */
    private final GridFutureAdapter<?> initCrdFut = new GridFutureAdapter<>();

    /** Coordinator initialization actions mutex. */
    private final Object initCrdMux = new Object();

    /** Remaining nodes to received services single deployments message. */
    @GridToStringInclude
    private final Set<UUID> remaining = new HashSet<>();

    /** Added in deployment queue flag. */
    private final AtomicBoolean addedInQueue = new AtomicBoolean(false);

    /** Single deployments messages to process. */
    @GridToStringInclude
    private final Map<UUID, ServiceSingleNodeDeploymentResultBatch> singleDepsMsgs = new HashMap<>();

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

    /** Service processor. */
    private final IgniteServiceProcessor srvcProc;

    /** Deployment process id. */
    @GridToStringInclude
    private final ServiceDeploymentProcessId depId;

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
    private volatile ServiceDeploymentActions depActions;

    /**
     * @param ctx Kernal context.
     * @param depId Service deployment process id.
     */
    protected ServiceDeploymentTask(GridKernalContext ctx, ServiceDeploymentProcessId depId) {
        assert ctx.service() instanceof IgniteServiceProcessor;

        this.depId = depId;
        this.ctx = ctx;

        srvcProc = (IgniteServiceProcessor)ctx.service();
        log = ctx.log(getClass());
    }

    /**
     * Handles discovery event receiving.
     *
     * @param evt Discovery event.
     * @param evtTopVer Topology version.
     * @param depActions Services deployment actions.
     */
    protected void onEvent(@NotNull DiscoveryEvent evt, @NotNull AffinityTopologyVersion evtTopVer,
        @Nullable ServiceDeploymentActions depActions) {
        assert evt.type() == EVT_NODE_JOINED || evt.type() == EVT_NODE_LEFT || evt.type() == EVT_NODE_FAILED
            || evt.type() == EVT_DISCOVERY_CUSTOM_EVT : "Unexpected event type, evt=" + evt;

        this.evt = evt;
        this.evtTopVer = evtTopVer;
        this.depActions = depActions;
    }

    /**
     * Initializes deployment task.
     *
     * @throws IgniteCheckedException In case of an error.
     */
    protected void init() throws IgniteCheckedException {
        if (isCompleted() || initTaskFut.isDone())
            return;

        assert evt != null && evtTopVer != null : "Illegal state to perform task's initialization :" + this;

        if (log.isDebugEnabled()) {
            log.debug("Started services deployment task init: [depId=" + depId +
                ", locId=" + ctx.localNodeId() + ", evt=" + evt + ']');
        }

        try {
            if (depActions != null && depActions.deactivate()) {
                srvcProc.onDeActivate(ctx);

                completeSuccess();

                return;
            }

            if (depActions == null) {
                Map<IgniteUuid, ServiceInfo> toDeploy = new HashMap<>();

                final int evtType = evt.type();

                if (evtType == EVT_DISCOVERY_CUSTOM_EVT) {
                    DiscoveryCustomMessage msg = ((DiscoveryCustomEvent)evt).customMessage();

                    if (msg instanceof CacheAffinityChangeMessage) {
                        CacheAffinityChangeMessage msg0 = (CacheAffinityChangeMessage)msg;

                        Map<IgniteUuid, ServiceInfo> services = srvcProc.deployedServices();

                        if (!services.isEmpty()) {
                            Map<Integer, IgniteUuid> change = msg0.cacheDeploymentIds();

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
                    assert evtType == EVT_NODE_JOINED || evtType == EVT_NODE_LEFT || evtType == EVT_NODE_FAILED;

                    final ClusterNode eventNode = evt.eventNode();

                    final Map<IgniteUuid, ServiceInfo> deployedServices = srvcProc.deployedServices();

                    if (evtType == EVT_NODE_LEFT || evtType == EVT_NODE_FAILED) {
                        deployedServices.forEach((srvcId, desc) -> {
                            if (desc.topologySnapshot().containsKey(eventNode.id()) ||
                                (desc.cacheName() != null && !eventNode.isClient())) // If affinity service
                                toDeploy.put(srvcId, desc);
                        });
                    }
                    else {
                        toDeploy.putAll(deployedServices);

                        toDeploy.putAll(srvcProc.servicesReceivedFromJoin(eventNode.id()));
                    }
                }

                if (toDeploy.isEmpty()) {
                    completeSuccess();

                    if (log.isDebugEnabled())
                        log.debug("No services deployment deployment action required.");

                    return;
                }

                depActions = new ServiceDeploymentActions();

                depActions.servicesToDeploy(toDeploy);
            }

            ClusterNode crd = srvcProc.coordinator();

            if (crd == null) {
                onAllServersLeft();

                return;
            }

            crdId = crd.id();

            if (crd.isLocal())
                initCoordinator(evtTopVer);

            processDeploymentActions(depActions);
        }
        catch (Exception e) {
            log.error("Error occurred while initializing deployment task, err=" + e.getMessage(), e);

            completeError(e);

            throw new IgniteCheckedException(e);
        }
        finally {
            if (!initTaskFut.isDone())
                initTaskFut.onDone();

            if (log.isDebugEnabled()) {
                log.debug("Finished services deployment future init: [depId=" + deploymentId() +
                    ", locId=" + ctx.localNodeId() + ']');
            }
        }
    }

    /**
     * @param depActions Services deployment actions.
     */
    private void processDeploymentActions(@NotNull ServiceDeploymentActions depActions) {
        srvcProc.updateDeployedServices(depActions);

        depActions.servicesToUndeploy().forEach((srvcId, desc) -> {
            srvcProc.deployment().deployerBlockingSectionBegin();

            try {
                srvcProc.undeploy(srvcId);
            }
            finally {
                srvcProc.deployment().deployerBlockingSectionEnd();
            }
        });

        if (!depActions.servicesToDeploy().isEmpty()) {
            final Collection<UUID> evtTopNodes = F.nodeIds(ctx.discovery().nodes(evtTopVer));

            depActions.servicesToDeploy().forEach((srvcId, desc) -> {
                try {
                    ServiceConfiguration cfg = desc.configuration();

                    TreeMap<UUID, Integer> oldTop = filterDeadNodes(evtTopNodes, desc.topologySnapshot());

                    Map<UUID, Integer> top = reassign(srvcId, cfg, evtTopVer, oldTop);

                    expDeps.put(srvcId, top);

                    Integer expCnt = top.getOrDefault(ctx.localNodeId(), 0);

                    if (expCnt > srvcProc.localInstancesCount(srvcId)) {
                        srvcProc.deployment().deployerBlockingSectionBegin();

                        try {
                            srvcProc.redeploy(srvcId, cfg, top);
                        }
                        finally {
                            srvcProc.deployment().deployerBlockingSectionEnd();
                        }
                    }
                }
                catch (IgniteCheckedException e) {
                    depErrors.computeIfAbsent(srvcId, c -> new ArrayList<>()).add(e);
                }
            });
        }

        createAndSendSingleDeploymentsMessage(depId, depErrors);
    }

    /**
     * Prepares the coordinator to manage deployment process.
     *
     * @param topVer Topology version to initialize {@link #remaining} collection.
     */
    private void initCoordinator(AffinityTopologyVersion topVer) {
        synchronized (initCrdMux) {
            if (initCrdFut.isDone())
                return;

            try {
                for (ClusterNode node : ctx.discovery().nodes(topVer)) {
                    if (ctx.discovery().alive(node) && !singleDepsMsgs.containsKey(node.id()))
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
     * @param depId Deployment process id.
     * @param errors Deployment errors.
     */
    private void createAndSendSingleDeploymentsMessage(ServiceDeploymentProcessId depId,
        final Map<IgniteUuid, Collection<Throwable>> errors) {
        assert crdId != null : "Coordinator should be defined at this point, locId=" + ctx.localNodeId();

        try {
            Set<IgniteUuid> depServicesIds = new HashSet<>();

            if (evt.type() == EVT_NODE_JOINED) {
                UUID evtNodeId = evt.eventNode().id();

                expDeps.forEach((srvcId, top) -> {
                    if (top.containsKey(evtNodeId))
                        depServicesIds.add(srvcId);
                });
            }
            else
                depServicesIds.addAll(expDeps.keySet());

            Map<IgniteUuid, ServiceSingleNodeDeploymentResult> results = new HashMap<>();

            for (IgniteUuid srvcId : depServicesIds) {
                ServiceSingleNodeDeploymentResult depRes = new ServiceSingleNodeDeploymentResult(
                    srvcProc.localInstancesCount(srvcId));

                attachDeploymentErrors(depRes, errors.get(srvcId));

                results.put(srvcId, depRes);
            }

            errors.forEach((srvcId, err) -> {
                if (results.containsKey(srvcId))
                    return;

                ServiceSingleNodeDeploymentResult depRes = new ServiceSingleNodeDeploymentResult(
                    srvcProc.localInstancesCount(srvcId));

                attachDeploymentErrors(depRes, err);

                results.put(srvcId, depRes);
            });

            ServiceSingleNodeDeploymentResultBatch msg = new ServiceSingleNodeDeploymentResultBatch(depId, results);

            if (ctx.localNodeId().equals(crdId))
                onReceiveSingleDeploymentsMessage(ctx.localNodeId(), msg);
            else
                ctx.io().sendToGridTopic(crdId, TOPIC_SERVICES, msg, SERVICE_POOL);

            if (log.isDebugEnabled())
                log.debug("Send services single deployments message, msg=" + msg);
        }
        catch (IgniteCheckedException e) {
            log.error("Failed to send services single deployments message to coordinator over communication spi.", e);
        }
    }

    /**
     * Handles received single node services map message.
     *
     * @param snd Sender node id.
     * @param msg Single services map message.
     */
    protected void onReceiveSingleDeploymentsMessage(UUID snd, ServiceSingleNodeDeploymentResultBatch msg) {
        assert depId.equals(msg.deploymentId()) : "Wrong message's deployment process id, msg=" + msg;

        initCrdFut.listen((IgniteInClosure<IgniteInternalFuture<?>>)fut -> {
            if (isCompleted())
                return;

            synchronized (initCrdMux) {
                if (remaining.remove(snd)) {
                    singleDepsMsgs.put(snd, msg);

                    if (remaining.isEmpty())
                        onAllReceived();
                }
                else if (log.isDebugEnabled())
                    log.debug("Unexpected service single deployments received, msg=" + msg);
            }
        });
    }

    /**
     * Handles received full services map message.
     *
     * @param msg Full services map message.
     */
    protected void onReceiveFullDeploymentsMessage(ServiceClusterDeploymentResultBatch msg) {
        assert depId.equals(msg.deploymentId()) : "Wrong message's deployment process id, msg=" + msg;

        initTaskFut.listen((IgniteInClosure<IgniteInternalFuture<?>>)fut -> {
            if (isCompleted())
                return;

            ctx.closure().runLocalSafe(() -> {
                try {
                    ServiceDeploymentActions depResults = msg.servicesDeploymentActions();

                    assert depResults != null : "Services deployment actions should be attached.";

                    final Map<IgniteUuid, Map<UUID, Integer>> fullTops = depResults.deploymentTopologies();
                    final Map<IgniteUuid, Collection<byte[]>> fullErrors = depResults.deploymentErrors();

                    depActions.deploymentTopologies(fullTops);
                    depActions.deploymentErrors(fullErrors);

                    srvcProc.updateServicesTopologies(fullTops);

                    final Map<IgniteUuid, ServiceInfo> services = srvcProc.deployedServices();

                    fullTops.forEach((srvcId, top) -> {
                        Integer expCnt = top.getOrDefault(ctx.localNodeId(), 0);

                        if (expCnt < srvcProc.localInstancesCount(srvcId)) { // Undeploy exceed instances
                            ServiceInfo desc = services.get(srvcId);

                            assert desc != null;

                            ServiceConfiguration cfg = desc.configuration();

                            try {
                                srvcProc.redeploy(srvcId, cfg, top);
                            }
                            catch (IgniteCheckedException e) {
                                log.error("Error occured during cancel exceed service instances: " +
                                    "[srvcId=" + srvcId + ", name=" + desc.name() + ']', e);
                            }
                        }
                    });

                    completeSuccess();
                }
                catch (Throwable t) {
                    log.error("Failed to process services full deployments message, msg=" + msg, t);

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
        if (depActions == null)
            return;

        depActions.servicesToDeploy().forEach((srvcId, desc) -> {
            if (err != null) {
                srvcProc.completeInitiatingFuture(true, srvcId, err);

                return;
            }

            Collection<byte[]> errors = depActions.deploymentErrors().get(srvcId);

            if (errors == null) {
                srvcProc.completeInitiatingFuture(true, srvcId, null);

                return;
            }

            Throwable depErr = null;

            for (byte[] error : errors) {
                try {
                    Throwable t = U.unmarshal(ctx, error, null);

                    if (depErr == null)
                        depErr = t;
                    else
                        depErr.addSuppressed(t);
                }
                catch (IgniteCheckedException e) {
                    log.error("Failed to unmarshal deployment error.", e);
                }
            }

            srvcProc.completeInitiatingFuture(true, srvcId, depErr);
        });

        for (IgniteUuid reqSrvcId : depActions.servicesToUndeploy().keySet())
            srvcProc.completeInitiatingFuture(false, reqSrvcId, err);
    }

    /**
     * Creates services full deployments message and send it over discovery.
     */
    private void onAllReceived() {
        assert !isCompleted();

        Collection<ServiceClusterDeploymentResult> fullResults = buildFullDeploymentsResults(singleDepsMsgs);

        try {
            ServiceClusterDeploymentResultBatch msg = new ServiceClusterDeploymentResultBatch(depId, fullResults);

            ctx.discovery().sendCustomEvent(msg);
        }
        catch (IgniteCheckedException e) {
            log.error("Failed to send services full deployments message across the ring.", e);
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
        AffinityTopologyVersion topVer, TreeMap<UUID, Integer> oldTop) throws IgniteCheckedException {
        try {
            Map<UUID, Integer> top = srvcProc.reassign(srvcId, cfg, topVer, oldTop);

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
     * Filters dead nodes from given service topology snapshot using given ids.
     *
     * @param evtTopNodes Ids being used to filter.
     * @param top Service topology snapshot.
     * @return Filtered service topology snapshot.
     */
    private TreeMap<UUID, Integer> filterDeadNodes(Collection<UUID> evtTopNodes, Map<UUID, Integer> top) {
        TreeMap<UUID, Integer> filtered = new TreeMap<>();

        if (F.isEmpty(top))
            return filtered;

        top.forEach((nodeId, cnt) -> {
            // We can't just use 'ctx.discovery().alive(UUID)', because during the deployment process discovery
            // topology may be changed and results may be different on some set of nodes.
            if (evtTopNodes.contains(nodeId))
                filtered.put(nodeId, cnt);
        });

        return filtered;
    }

    /**
     * Processes single deployments messages to build full deployment results.
     *
     * @param singleDepsMsgs Services single deployments messages.
     * @return Services full deployments results.
     */
    private Collection<ServiceClusterDeploymentResult> buildFullDeploymentsResults(
        Map<UUID, ServiceSingleNodeDeploymentResultBatch> singleDepsMsgs) {
        final Map<IgniteUuid, Map<UUID, ServiceSingleNodeDeploymentResult>> singleResults = new HashMap<>();

        singleDepsMsgs.forEach((nodeId, msg) -> msg.results().forEach((srvcId, res) -> {
            Map<UUID, ServiceSingleNodeDeploymentResult> depResults = singleResults
                .computeIfAbsent(srvcId, r -> new HashMap<>());

            int cnt = res.count();

            if (cnt != 0) {
                Map<UUID, Integer> expTop = expDeps.get(srvcId);

                if (expTop != null) {
                    Integer expCnt = expTop.get(nodeId);

                    cnt = expCnt == null ? 0 : Math.min(cnt, expCnt);
                }
            }

            if (cnt == 0 && res.errors().isEmpty())
                return;

            ServiceSingleNodeDeploymentResult singleDepRes = new ServiceSingleNodeDeploymentResult(cnt);

            if (!res.errors().isEmpty())
                singleDepRes.errors(res.errors());

            depResults.put(nodeId, singleDepRes);
        }));

        final Collection<ServiceClusterDeploymentResult> fullResults = new ArrayList<>();

        singleResults.forEach((srvcId, dep) -> {
            ServiceClusterDeploymentResult res = new ServiceClusterDeploymentResult(srvcId, dep);

            fullResults.add(res);
        });

        return fullResults;
    }

    /**
     * @param depRes Service single deployments results.
     * @param errors Deployment errors.
     */
    private void attachDeploymentErrors(@NotNull ServiceSingleNodeDeploymentResult depRes,
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
                ClusterNode crd = srvcProc.coordinator();

                if (crd != null) {
                    crdId = crd.id();

                    if (crd.isLocal())
                        initCoordinator(evtTopVer);

                    createAndSendSingleDeploymentsMessage(depId, depErrors);
                }
                else
                    onAllServersLeft();
            }
            else if (ctx.localNodeId().equals(crdId)) {
                synchronized (initCrdMux) {
                    boolean rmvd = remaining.remove(nodeId);

                    if (rmvd && remaining.isEmpty()) {
                        singleDepsMsgs.remove(nodeId);

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

        completeError(new ClusterTopologyServerNotFoundException("Failed to resolve coordinator to continue services " +
            "deployment process: [locId=" + ctx.localNodeId() + "client=" + ctx.clientNode() + "evt=" + evt + ']'));
    }

    /**
     * @return Cause discovery event.
     */
    public DiscoveryEvent event() {
        return evt;
    }

    /**
     * Returns cause of deployment process topology version.
     *
     * @return Cause of deployment process topology version.
     */
    public AffinityTopologyVersion topologyVersion() {
        return evtTopVer;
    }

    /**
     * Returns services deployment process id of the task.
     *
     * @return Services deployment process id.
     */
    public ServiceDeploymentProcessId deploymentId() {
        return depId;
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
     * Handles when this task is being added in deployment queue.
     * <p/>
     * Introduced to avoid overhead on calling of {@link Collection#contains(Object)}}.
     *
     * @return {@code true} if task is has not been added previously, otherwise {@code false}.
     */
    protected boolean onEnqueued() {
        return addedInQueue.compareAndSet(false, true);
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        ServiceDeploymentTask task = (ServiceDeploymentTask)o;

        return depId.equals(task.depId);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return depId.hashCode();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        synchronized (initCrdMux) {
            return S.toString(ServiceDeploymentTask.class, this,
                    "locNodeId", (ctx != null ? ctx.localNodeId() : "unknown"),
                    "crdId", crdId);
        }
    }
}
