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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteInterruptedException;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.DeploymentMode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteClientDisconnectedCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.SkipDaemon;
import org.apache.ignite.internal.managers.deployment.GridDeployment;
import org.apache.ignite.internal.managers.discovery.CustomEventListener;
import org.apache.ignite.internal.managers.discovery.DiscoCache;
import org.apache.ignite.internal.managers.systemview.walker.ServiceViewWalker;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.DynamicCacheChangeBatch;
import org.apache.ignite.internal.processors.cache.DynamicCacheChangeRequest;
import org.apache.ignite.internal.processors.cluster.ChangeGlobalStateMessage;
import org.apache.ignite.internal.processors.cluster.DiscoveryDataClusterState;
import org.apache.ignite.internal.processors.cluster.IgniteChangeGlobalStateSupport;
import org.apache.ignite.internal.processors.platform.services.PlatformService;
import org.apache.ignite.internal.util.future.GridCompoundFuture;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.marshaller.jdk.JdkMarshaller;
import org.apache.ignite.plugin.security.SecurityPermission;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceConfiguration;
import org.apache.ignite.services.ServiceDeploymentException;
import org.apache.ignite.services.ServiceDescriptor;
import org.apache.ignite.spi.communication.CommunicationSpi;
import org.apache.ignite.spi.discovery.DiscoveryDataBag;
import org.apache.ignite.spi.discovery.DiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.systemview.view.ServiceView;
import org.apache.ignite.thread.IgniteThreadFactory;
import org.apache.ignite.thread.OomExceptionHandler;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.configuration.DeploymentMode.ISOLATED;
import static org.apache.ignite.configuration.DeploymentMode.PRIVATE;
import static org.apache.ignite.events.EventType.EVT_NODE_JOINED;
import static org.apache.ignite.internal.GridComponent.DiscoveryDataExchangeType.SERVICE_PROC;

/**
 * Ignite service processor.
 * <p/>
 * Event-driven implementation of the service processor. Service deployment is managed via {@link DiscoverySpi} and
 * {@link CommunicationSpi} messages.
 *
 * @see ServiceDeploymentManager
 * @see ServiceDeploymentTask
 * @see ServiceDeploymentActions
 * @see ServiceChangeBatchRequest
 */
@SkipDaemon
@SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
public class IgniteServiceProcessor extends ServiceProcessorAdapter implements IgniteChangeGlobalStateSupport {
    /** */
    public static final String SVCS_VIEW = "services";

    /** */
    public static final String SVCS_VIEW_DESC = "Services";

    /** Local service instances. */
    private final ConcurrentMap<IgniteUuid, Collection<ServiceContextImpl>> locServices = new ConcurrentHashMap<>();

    /**
     * Collection of services information that were registered in the cluster. <b>It is updated from discovery
     * thread</b>. It will be included in the initial data bag to be sent on newly joining node, it means a new node
     * will have all services' information to be able to work with services in the whole cluster.
     * <p/>
     * This collection is used, to make fast verification for requests of change service's state and prepare services
     * deployments actions (possible long-running) which will be processed from a queue by deployment worker.
     * <p/>
     * Collection reflects a services' state which will be reached as soon as a relevant deployment task will be
     * processed.
     *
     * @see ServiceDeploymentActions
     * @see ServiceDeploymentManager
     */
    private final ConcurrentMap<IgniteUuid, ServiceInfo> registeredServices = new ConcurrentHashMap<>();

    /**
     * Collection of services information that were processed by deployment worker. <b>It is updated from deployment
     * worker</b>.
     * <p/>
     * Collection reflects a state of deployed services for a moment of the latest deployment task processed by
     * deployment worker.
     * <p/>
     * It is catching up the state of {@link #registeredServices}.
     *
     * @see ServiceDeploymentManager#readyTopologyVersion()
     * @see ServiceDeploymentTask
     */
    private final ConcurrentMap<IgniteUuid, ServiceInfo> deployedServices = new ConcurrentHashMap<>();

    /** Deployment futures. */
    private final ConcurrentMap<IgniteUuid, GridServiceDeploymentFuture<IgniteUuid>> depFuts = new ConcurrentHashMap<>();

    /** Undeployment futures. */
    private final ConcurrentMap<IgniteUuid, GridFutureAdapter<?>> undepFuts = new ConcurrentHashMap<>();

    /** Thread factory. */
    private final ThreadFactory threadFactory = new IgniteThreadFactory(ctx.igniteInstanceName(), "service",
        new OomExceptionHandler(ctx));

    /** Marshaller for serialization/deserialization of service's instance. */
    private final Marshaller marsh = new JdkMarshaller();

    /** Services deployment manager. */
    private volatile ServiceDeploymentManager depMgr = new ServiceDeploymentManager(ctx);

    /** Services topologies update mutex. */
    private final Object servicesTopsUpdateMux = new Object();

    /**
     * Operations lock. The main purpose is to avoid a hang of users operation futures.
     * <p/>
     * Read lock is being acquired on users operations (deploy, cancel).
     * <p/>
     * Write lock is being acquired on change service processor's state: {@link #onKernalStop}, {@link #onDisconnected),
     * {@link #onDeActivate(GridKernalContext)}} to guarantee that deployed services will be cancelled only once, also
     * it protects from registering new operations futures which may be missed during completion collections of users
     * futures.
     * <pre>
     * {@link #enterBusy()} and {@link #leaveBusy()} are being used to protect modification of shared collections during
     * changing service processor state. If a call can't enter in the busy state a default value will be returned (a
     * value which will be reached by the time when write lock will be released).
     * These methods can't be used for users operations (deploy, undeploy) because if the processor will become
     * disconnected or stopped we should return different types of exceptions (it's not about just a errors message,
     * the disconnected exception also contains reconnect future).
     * </pre>
     */
    private final ReentrantReadWriteLock opsLock = new ReentrantReadWriteLock();

    /** Disconnected flag. */
    private volatile boolean disconnected;

    /**
     * @param ctx Kernal context.
     */
    public IgniteServiceProcessor(GridKernalContext ctx) {
        super(ctx);

        ctx.systemView().registerView(SVCS_VIEW, SVCS_VIEW_DESC,
            new ServiceViewWalker(),
            registeredServices.values(),
            ServiceView::new);
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteCheckedException {
        IgniteConfiguration cfg = ctx.config();

        DeploymentMode depMode = cfg.getDeploymentMode();

        if (cfg.isPeerClassLoadingEnabled() && (depMode == PRIVATE || depMode == ISOLATED) &&
            !F.isEmpty(cfg.getServiceConfiguration()))
            throw new IgniteCheckedException("Cannot deploy services in PRIVATE or ISOLATED deployment mode: " + depMode);

        ctx.discovery().setCustomEventListener(ServiceChangeBatchRequest.class,
            new CustomEventListener<ServiceChangeBatchRequest>() {
                @Override public void onCustomEvent(AffinityTopologyVersion topVer, ClusterNode snd,
                    ServiceChangeBatchRequest msg) {
                    processServicesChangeRequest(snd, msg);
                }
            });

        ctx.discovery().setCustomEventListener(ChangeGlobalStateMessage.class,
            new CustomEventListener<ChangeGlobalStateMessage>() {
                @Override public void onCustomEvent(AffinityTopologyVersion topVer, ClusterNode snd,
                    ChangeGlobalStateMessage msg) {
                    processChangeGlobalStateRequest(msg);
                }
            });

        ctx.discovery().setCustomEventListener(DynamicCacheChangeBatch.class,
            new CustomEventListener<DynamicCacheChangeBatch>() {
                @Override public void onCustomEvent(AffinityTopologyVersion topVer, ClusterNode snd,
                    DynamicCacheChangeBatch msg) {
                    processDynamicCacheChangeRequest(msg);
                }
            });

        ctx.discovery().setCustomEventListener(ServiceClusterDeploymentResultBatch.class,
            new CustomEventListener<ServiceClusterDeploymentResultBatch>() {
                @Override public void onCustomEvent(AffinityTopologyVersion topVer, ClusterNode snd,
                    ServiceClusterDeploymentResultBatch msg) {
                    processServicesFullDeployments(msg);
                }
            });
    }

    /** {@inheritDoc} */
    @Override public void onKernalStart(boolean active) throws IgniteCheckedException {
        depMgr.startProcessing();

        if (log.isDebugEnabled())
            log.debug("Started service processor.");
    }

    /** {@inheritDoc} */
    @Override public void onKernalStop(boolean cancel) {
        opsLock.writeLock().lock();

        try {
            if (disconnected)
                return;

            stopProcessor(new IgniteCheckedException("Operation has been cancelled (node is stopping)."));
        }
        finally {
            opsLock.writeLock().unlock();
        }
    }

    /**
     * @param stopError Error to shutdown resources.
     */
    private void stopProcessor(IgniteCheckedException stopError) {
        assert opsLock.isWriteLockedByCurrentThread();

        depMgr.stopProcessing(stopError);

        cancelDeployedServices();

        registeredServices.clear();

        // If user requests sent to network but not received back to handle in deployment manager.
        Stream.concat(depFuts.values().stream(), undepFuts.values().stream()).forEach(fut -> {
            try {
                fut.onDone(stopError);
            }
            catch (Exception ignore) {
                // No-op.
            }
        });

        depFuts.clear();
        undepFuts.clear();

        if (log.isDebugEnabled())
            log.debug("Stopped service processor.");
    }

    /**
     * Cancels deployed services.
     */
    private void cancelDeployedServices() {
        assert opsLock.isWriteLockedByCurrentThread();

        deployedServices.clear();

        locServices.values().stream().flatMap(Collection::stream).forEach(srvcCtx -> {
            cancel(srvcCtx);

            if (ctx.isStopping()) {
                try {
                    if (log.isInfoEnabled()) {
                        log.info("Shutting down distributed service [name=" + srvcCtx.name() + ", execId8=" +
                            U.id8(srvcCtx.executionId()) + ']');
                    }

                    srvcCtx.executor().awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
                }
                catch (InterruptedException ignore) {
                    Thread.currentThread().interrupt();

                    U.error(log, "Got interrupted while waiting for service to shutdown (will continue " +
                        "stopping node): " + srvcCtx.name());
                }
            }
        });

        locServices.clear();
    }

    /** {@inheritDoc} */
    @Override public void collectGridNodeData(DiscoveryDataBag dataBag) {
        if (dataBag.commonDataCollectedFor(SERVICE_PROC.ordinal()))
            return;

        ServiceProcessorCommonDiscoveryData clusterData = new ServiceProcessorCommonDiscoveryData(
            new ArrayList<>(registeredServices.values())
        );

        dataBag.addGridCommonData(SERVICE_PROC.ordinal(), clusterData);
    }

    /** {@inheritDoc} */
    @Override public void onGridDataReceived(DiscoveryDataBag.GridDiscoveryData data) {
        if (data.commonData() == null)
            return;

        ServiceProcessorCommonDiscoveryData clusterData = (ServiceProcessorCommonDiscoveryData)data.commonData();

        for (ServiceInfo desc : clusterData.registeredServices())
            registeredServices.put(desc.serviceId(), desc);
    }

    /** {@inheritDoc} */
    @Override public void collectJoiningNodeData(DiscoveryDataBag dataBag) {
        ArrayList<ServiceInfo> staticServicesInfo = staticallyConfiguredServices(true);

        dataBag.addJoiningNodeData(SERVICE_PROC.ordinal(), new ServiceProcessorJoinNodeDiscoveryData(staticServicesInfo));
    }

    /** {@inheritDoc} */
    @Override public void onJoiningNodeDataReceived(DiscoveryDataBag.JoiningNodeDiscoveryData data) {
        if (data.joiningNodeData() == null)
            return;

        ServiceProcessorJoinNodeDiscoveryData joinData = (ServiceProcessorJoinNodeDiscoveryData)data.joiningNodeData();

        for (ServiceInfo desc : joinData.services()) {
            assert desc.topologySnapshot().isEmpty();

            ServiceInfo oldDesc = registeredServices.get(desc.serviceId());

            if (oldDesc != null) { // In case of a collision of IgniteUuid.randomUuid() (almost impossible case)
                U.warn(log, "Failed to register service configuration received from joining node : " +
                    "[nodeId=" + data.joiningNodeId() + ", cfgName=" + desc.name() + "]. " +
                    "Service with the same service id already exists, cfg=" + oldDesc.configuration());

                continue;
            }

            oldDesc = lookupInRegisteredServices(desc.name());

            if (oldDesc == null) {
                registeredServices.put(desc.serviceId(), desc);

                continue;
            }

            if (oldDesc.configuration().equalsIgnoreNodeFilter(desc.configuration())) {
                if (log.isDebugEnabled()) {
                    log.debug("Ignore service configuration received from joining node : " +
                        "[nodeId=" + data.joiningNodeId() + ", cfgName=" + desc.name() + "]. " +
                        "The same service configuration already registered.");
                }
            }
            else {
                U.warn(log, "Failed to register service configuration received from joining node : " +
                    "[nodeId=" + data.joiningNodeId() + ", cfgName=" + desc.name() + "]. " +
                    "Service already exists with different configuration, cfg=" + desc.configuration());
            }
        }
    }

    /** {@inheritDoc} */
    @Nullable @Override public DiscoveryDataExchangeType discoveryDataType() {
        return SERVICE_PROC;
    }

    /** {@inheritDoc} */
    @Override public void onActivate(GridKernalContext kctx) {
        // No-op.
    }

    /**
     * Invokes from services deployment worker.
     * <p/>
     * {@inheritDoc}
     */
    @Override public void onDeActivate(GridKernalContext kctx) {
        try {
            opsLock.writeLock().lockInterruptibly();
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            throw new IgniteInterruptedException(e);
        }

        try {
            if (log.isDebugEnabled()) {
                log.debug("DeActivate service processor [nodeId=" + ctx.localNodeId() +
                    " topVer=" + ctx.discovery().topologyVersionEx() + " ]");
            }

            cancelDeployedServices();
        }
        finally {
            opsLock.writeLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public void onDisconnected(IgniteFuture<?> reconnectFut) {
        assert !disconnected;

        opsLock.writeLock().lock();

        try {
            if (ctx.isStopping())
                return;

            disconnected = true;

            stopProcessor(new IgniteClientDisconnectedCheckedException(
                ctx.cluster().clientReconnectFuture(), "Client node disconnected, the operation's result is unknown."));
        }
        finally {
            opsLock.writeLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> onReconnected(boolean active) throws IgniteCheckedException {
        assert disconnected;

        opsLock.writeLock().lock();

        try {
            disconnected = false;

            depMgr = new ServiceDeploymentManager(ctx);

            onKernalStart(active);

            return null;
        }
        finally {
            opsLock.writeLock().unlock();
        }
    }

    /**
     * Validates service configuration.
     *
     * @param c Service configuration.
     * @throws IgniteException If validation failed.
     */
    private void validate(ServiceConfiguration c) throws IgniteException {
        IgniteConfiguration cfg = ctx.config();

        DeploymentMode depMode = cfg.getDeploymentMode();

        if (cfg.isPeerClassLoadingEnabled() && (depMode == PRIVATE || depMode == ISOLATED))
            throw new IgniteException("Cannot deploy services in PRIVATE or ISOLATED deployment mode: " + depMode);

        ensure(c.getName() != null, "getName() != null", null);
        ensure(c.getTotalCount() >= 0, "getTotalCount() >= 0", c.getTotalCount());
        ensure(c.getMaxPerNodeCount() >= 0, "getMaxPerNodeCount() >= 0", c.getMaxPerNodeCount());
        ensure(c.getService() != null, "getService() != null", c.getService());
        ensure(c.getTotalCount() > 0 || c.getMaxPerNodeCount() > 0,
            "c.getTotalCount() > 0 || c.getMaxPerNodeCount() > 0", null);
    }

    /**
     * @param cond Condition.
     * @param desc Description.
     * @param v Value.
     */
    private void ensure(boolean cond, String desc, @Nullable Object v) {
        if (!cond)
            if (v != null)
                throw new IgniteException("Service configuration check failed (" + desc + "): " + v);
            else
                throw new IgniteException("Service configuration check failed (" + desc + ")");
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> deployNodeSingleton(ClusterGroup prj, String name, Service srvc) {
        return deployMultiple(prj, name, srvc, 0, 1);
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> deployClusterSingleton(ClusterGroup prj, String name, Service srvc) {
        return deployMultiple(prj, name, srvc, 1, 1);
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> deployMultiple(ClusterGroup prj, String name, Service srvc, int totalCnt,
        int maxPerNodeCnt) {
        ServiceConfiguration cfg = new ServiceConfiguration();

        cfg.setName(name);
        cfg.setService(srvc);
        cfg.setTotalCount(totalCnt);
        cfg.setMaxPerNodeCount(maxPerNodeCnt);

        return deployAll(prj, Collections.singleton(cfg));
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> deployKeyAffinitySingleton(String name, Service srvc, String cacheName,
        Object affKey) {
        A.notNull(affKey, "affKey");

        ServiceConfiguration cfg = new ServiceConfiguration();

        cfg.setName(name);
        cfg.setService(srvc);
        cfg.setCacheName(cacheName);
        cfg.setAffinityKey(affKey);
        cfg.setTotalCount(1);
        cfg.setMaxPerNodeCount(1);

        // Ignore projection here.
        return deployAll(Collections.singleton(cfg), null);
    }

    /**
     * @param cfgs Service configurations.
     * @param dfltNodeFilter Default NodeFilter.
     * @return Configurations to deploy.
     */
    private PreparedConfigurations<IgniteUuid> prepareServiceConfigurations(Collection<ServiceConfiguration> cfgs,
        IgnitePredicate<ClusterNode> dfltNodeFilter) {
        List<ServiceConfiguration> cfgsCp = new ArrayList<>(cfgs.size());

        List<GridServiceDeploymentFuture<IgniteUuid>> failedFuts = null;

        for (ServiceConfiguration cfg : cfgs) {
            Exception err = null;

            // Deploy to projection node by default
            // or only on server nodes if no projection.
            if (cfg.getNodeFilter() == null && dfltNodeFilter != null)
                cfg.setNodeFilter(dfltNodeFilter);

            try {
                validate(cfg);
            }
            catch (Exception e) {
                U.error(log, "Failed to validate service configuration [name=" + cfg.getName() +
                    ", srvc=" + cfg.getService() + ']', e);

                err = e;
            }

            if (err == null)
                err = checkPermissions(cfg.getName(), SecurityPermission.SERVICE_DEPLOY);

            if (err == null) {
                try {
                    byte[] srvcBytes = U.marshal(marsh, cfg.getService());

                    cfgsCp.add(new LazyServiceConfiguration(cfg, srvcBytes));
                }
                catch (Exception e) {
                    U.error(log, "Failed to marshal service with configured marshaller " +
                        "[name=" + cfg.getName() + ", srvc=" + cfg.getService() + ", marsh=" + marsh + "]", e);

                    err = e;
                }
            }

            if (err != null) {
                if (failedFuts == null)
                    failedFuts = new ArrayList<>();

                GridServiceDeploymentFuture<IgniteUuid> fut = new GridServiceDeploymentFuture<>(cfg, null);

                fut.onDone(err);

                failedFuts.add(fut);
            }
        }

        return new PreparedConfigurations<>(cfgsCp, failedFuts);
    }

    /**
     * Checks security permissions for service with given name.
     *
     * @param name Service name.
     * @param perm Security permissions.
     * @return {@code null} if success, otherwise instance of {@link SecurityException}.
     */
    private SecurityException checkPermissions(String name, SecurityPermission perm) {
        try {
            ctx.security().authorize(name, perm);

            return null;
        }
        catch (SecurityException e) {
            U.error(log, "Failed to authorize service access [name=" + name + ", perm=" + perm + ']', e);

            return e;
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> deployAll(ClusterGroup prj, Collection<ServiceConfiguration> cfgs) {
        if (prj == null)
            // Deploy to servers by default if no projection specified.
            return deployAll(cfgs, ctx.cluster().get().forServers().predicate());
        else if (prj.predicate() == F.<ClusterNode>alwaysTrue())
            return deployAll(cfgs, null);
        else
            // Deploy to predicate nodes by default.
            return deployAll(cfgs, prj.predicate());
    }

    /**
     * @param cfgs Service configurations.
     * @param dfltNodeFilter Default NodeFilter.
     * @return Future for deployment.
     */
    private IgniteInternalFuture<?> deployAll(@NotNull Collection<ServiceConfiguration> cfgs,
        @Nullable IgnitePredicate<ClusterNode> dfltNodeFilter) {
        opsLock.readLock().lock();

        try {
            if (disconnected) {
                return new GridFinishedFuture<>(new IgniteClientDisconnectedCheckedException(
                    ctx.cluster().clientReconnectFuture(), "Failed to deploy services, " +
                    "client node disconnected: " + cfgs));
            }

            if (ctx.isStopping()) {
                return new GridFinishedFuture<>(new IgniteCheckedException("Failed to deploy services, " +
                    "node is stopping: " + cfgs));
            }

            if (cfgs.isEmpty())
                return new GridFinishedFuture<>();

            PreparedConfigurations<IgniteUuid> srvcCfg = prepareServiceConfigurations(cfgs, dfltNodeFilter);

            List<ServiceConfiguration> cfgsCp = srvcCfg.cfgs;

            List<GridServiceDeploymentFuture<IgniteUuid>> failedFuts = srvcCfg.failedFuts;

            GridServiceDeploymentCompoundFuture<IgniteUuid> res = new GridServiceDeploymentCompoundFuture<>();

            if (!cfgsCp.isEmpty()) {
                try {
                    Collection<ServiceChangeAbstractRequest> reqs = new ArrayList<>();

                    for (ServiceConfiguration cfg : cfgsCp) {
                        IgniteUuid srvcId = IgniteUuid.randomUuid();

                        GridServiceDeploymentFuture<IgniteUuid> fut = new GridServiceDeploymentFuture<>(cfg, srvcId);

                        res.add(fut, true);

                        reqs.add(new ServiceDeploymentRequest(srvcId, cfg));

                        depFuts.put(srvcId, fut);
                    }

                    ServiceChangeBatchRequest msg = new ServiceChangeBatchRequest(reqs);

                    ctx.discovery().sendCustomEvent(msg);

                    if (log.isDebugEnabled())
                        log.debug("Services have been sent to deploy, req=" + msg);
                }
                catch (IgniteException | IgniteCheckedException e) {
                    for (IgniteUuid id : res.servicesToRollback())
                        depFuts.remove(id).onDone(e);

                    res.onDone(new IgniteCheckedException(
                        new ServiceDeploymentException("Failed to deploy provided services.", e, cfgs)));

                    return res;
                }
            }

            if (failedFuts != null) {
                for (GridServiceDeploymentFuture<IgniteUuid> fut : failedFuts)
                    res.add(fut, false);
            }

            res.markInitialized();

            return res;
        }
        finally {
            opsLock.readLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> cancel(String name) {
        return cancelAll(Collections.singleton(name));
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> cancelAll() {
        return cancelAll(deployedServices.values().stream().map(ServiceInfo::name).collect(Collectors.toSet()));
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public IgniteInternalFuture<?> cancelAll(@NotNull Collection<String> servicesNames) {
        opsLock.readLock().lock();

        try {
            if (disconnected) {
                return new GridFinishedFuture<>(new IgniteClientDisconnectedCheckedException(
                    ctx.cluster().clientReconnectFuture(), "Failed to undeploy services, " +
                    "client node disconnected: " + servicesNames));
            }

            if (ctx.isStopping()) {
                return new GridFinishedFuture<>(new IgniteCheckedException("Failed to undeploy services, " +
                    "node is stopping: " + servicesNames));
            }

            if (servicesNames.isEmpty())
                return new GridFinishedFuture<>();

            GridCompoundFuture res = new GridCompoundFuture<>();

            Set<IgniteUuid> toRollback = new HashSet<>();

            List<ServiceChangeAbstractRequest> reqs = new ArrayList<>();

            try {
                for (String name : servicesNames) {
                    IgniteUuid srvcId = lookupDeployedServiceId(name);

                    if (srvcId == null)
                        continue;

                    Exception err = checkPermissions(name, SecurityPermission.SERVICE_CANCEL);

                    if (err != null) {
                        res.add(new GridFinishedFuture<>(err));

                        continue;
                    }

                    GridFutureAdapter<?> fut = new GridFutureAdapter<>();

                    GridFutureAdapter<?> old = undepFuts.putIfAbsent(srvcId, fut);

                    if (old != null) {
                        res.add(old);

                        continue;
                    }

                    res.add(fut);

                    toRollback.add(srvcId);

                    reqs.add(new ServiceUndeploymentRequest(srvcId));
                }

                if (!reqs.isEmpty()) {
                    ServiceChangeBatchRequest msg = new ServiceChangeBatchRequest(reqs);

                    ctx.discovery().sendCustomEvent(msg);

                    if (log.isDebugEnabled())
                        log.debug("Services have been sent to cancel, msg=" + msg);
                }
            }
            catch (IgniteException | IgniteCheckedException e) {
                for (IgniteUuid id : toRollback)
                    undepFuts.remove(id).onDone(e);

                U.error(log, "Failed to undeploy services: " + servicesNames, e);

                res.onDone(e);

                return res;
            }

            res.markInitialized();

            return res;
        }
        finally {
            opsLock.readLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public Map<UUID, Integer> serviceTopology(String name, long timeout) throws IgniteCheckedException {
        assert timeout >= 0;

        long startTime = U.currentTimeMillis();

        ServiceInfo desc;

        while (true) {
            synchronized (servicesTopsUpdateMux) {
                desc = lookupInRegisteredServices(name);

                if (timeout == 0 && desc == null)
                    return null;

                if (desc != null && desc.topologyInitialized())
                    return desc.topologySnapshot();

                long wait = 0;

                if (timeout != 0) {
                    wait = timeout - (U.currentTimeMillis() - startTime);

                    if (wait <= 0)
                        return desc == null ? null : desc.topologySnapshot();
                }

                try {
                    servicesTopsUpdateMux.wait(wait);
                }
                catch (InterruptedException e) {
                    throw new IgniteInterruptedCheckedException(e);
                }
            }
        }
    }

    /** {@inheritDoc} */
    @Override public Collection<ServiceDescriptor> serviceDescriptors() {
        return new ArrayList<>(registeredServices.values());
    }

    /** {@inheritDoc} */
    @Override public <T> T service(String name) {
        if (!enterBusy())
            return null;

        try {
            ctx.security().authorize(name, SecurityPermission.SERVICE_INVOKE);

            Collection<ServiceContextImpl> ctxs = serviceContexts(name);

            if (ctxs == null)
                return null;

            synchronized (ctxs) {
                if (F.isEmpty(ctxs))
                    return null;

                for (ServiceContextImpl ctx : ctxs) {
                    Service srvc = ctx.service();

                    if (srvc != null)
                        return (T)srvc;
                }

                return null;
            }
        }
        finally {
            leaveBusy();
        }
    }

    /** {@inheritDoc} */
    @Override public ServiceContextImpl serviceContext(String name) {
        if (!enterBusy())
            return null;

        try {
            Collection<ServiceContextImpl> ctxs = serviceContexts(name);

            if (ctxs == null)
                return null;

            synchronized (ctxs) {
                if (F.isEmpty(ctxs))
                    return null;

                for (ServiceContextImpl ctx : ctxs) {
                    if (ctx.service() != null)
                        return ctx;
                }
            }

            return null;
        }
        finally {
            leaveBusy();
        }
    }

    /**
     * @param name Service name.
     * @return Collection of locally deployed instance if present.
     */
    @Nullable private Collection<ServiceContextImpl> serviceContexts(String name) {
        IgniteUuid srvcId = lookupDeployedServiceId(name);

        if (srvcId == null)
            return null;

        return locServices.get(srvcId);
    }

    /** {@inheritDoc} */
    @Override public <T> T serviceProxy(ClusterGroup prj, String name, Class<? super T> srvcCls, boolean sticky,
        long timeout)
        throws IgniteException {
        ctx.security().authorize(name, SecurityPermission.SERVICE_INVOKE);

        if (hasLocalNode(prj)) {
            ServiceContextImpl ctx = serviceContext(name);

            if (ctx != null) {
                Service srvc = ctx.service();

                if (srvc != null) {
                    if (srvcCls.isAssignableFrom(srvc.getClass()))
                        return (T)srvc;
                    else if (!PlatformService.class.isAssignableFrom(srvc.getClass())) {
                        throw new IgniteException("Service does not implement specified interface [srvcCls="
                                + srvcCls.getName() + ", srvcCls=" + srvc.getClass().getName() + ']');
                    }
                }
            }
        }

        return new GridServiceProxy<T>(prj, name, srvcCls, sticky, timeout, ctx).proxy();
    }

    /**
     * @param prj Grid nodes projection.
     * @return Whether given projection contains any local node.
     */
    private boolean hasLocalNode(ClusterGroup prj) {
        for (ClusterNode n : prj.nodes()) {
            if (n.isLocal())
                return true;
        }

        return false;
    }

    /** {@inheritDoc} */
    @Override public <T> Collection<T> services(String name) {
        if (!enterBusy())
            return null;

        try {
            ctx.security().authorize(name, SecurityPermission.SERVICE_INVOKE);

            Collection<ServiceContextImpl> ctxs = serviceContexts(name);

            if (ctxs == null)
                return null;

            synchronized (ctxs) {
                if (F.isEmpty(ctxs))
                    return null;

                Collection<T> res = new ArrayList<>(ctxs.size());

                for (ServiceContextImpl ctx : ctxs) {
                    Service srvc = ctx.service();

                    if (srvc != null)
                        res.add((T)srvc);
                }

                return res;
            }
        }
        finally {
            leaveBusy();
        }
    }

    /**
     * Reassigns service to nodes.
     *
     * @param srvcId Service id.
     * @param cfg Service configuration.
     * @param topVer Topology version.
     * @param oldTop Previous topology snapshot. Will be ignored for affinity service.
     * @throws IgniteCheckedException If failed.
     */
    Map<UUID, Integer> reassign(@NotNull IgniteUuid srvcId, @NotNull ServiceConfiguration cfg,
        @NotNull AffinityTopologyVersion topVer,
        @Nullable TreeMap<UUID, Integer> oldTop) throws IgniteCheckedException {
        Object nodeFilter = cfg.getNodeFilter();

        if (nodeFilter != null)
            ctx.resource().injectGeneric(nodeFilter);

        int totalCnt = cfg.getTotalCount();
        int maxPerNodeCnt = cfg.getMaxPerNodeCount();
        String cacheName = cfg.getCacheName();
        Object affKey = cfg.getAffinityKey();

        Map<UUID, Integer> cnts = new TreeMap<>();

        if (affKey != null && cacheName != null) { // Affinity service
            ClusterNode n = ctx.affinity().mapKeyToNode(cacheName, affKey, topVer);

            if (n != null) {
                int cnt = maxPerNodeCnt == 0 ? totalCnt == 0 ? 1 : totalCnt : maxPerNodeCnt;

                cnts.put(n.id(), cnt);
            }
        }
        else {
            Collection<ClusterNode> nodes = ctx.discovery().nodes(topVer);

            if (cfg.getNodeFilter() != null) {
                Collection<ClusterNode> nodes0 = new ArrayList<>();

                for (ClusterNode node : nodes) {
                    if (cfg.getNodeFilter().apply(node))
                        nodes0.add(node);
                }

                nodes = nodes0;
            }

            if (!nodes.isEmpty()) {
                int size = nodes.size();

                int perNodeCnt = totalCnt != 0 ? totalCnt / size : maxPerNodeCnt;
                int remainder = totalCnt != 0 ? totalCnt % size : 0;

                if (perNodeCnt >= maxPerNodeCnt && maxPerNodeCnt != 0) {
                    perNodeCnt = maxPerNodeCnt;
                    remainder = 0;
                }

                for (ClusterNode n : nodes)
                    cnts.put(n.id(), perNodeCnt);

                assert perNodeCnt >= 0;
                assert remainder >= 0;

                if (remainder > 0) {
                    int cnt = perNodeCnt + 1;

                    Random rnd = new Random(srvcId.localId());

                    if (oldTop != null && !oldTop.isEmpty()) {
                        Collection<UUID> used = new TreeSet<>();

                        // Avoid redundant moving of services.
                        for (Map.Entry<UUID, Integer> e : oldTop.entrySet()) {
                            // If old count and new count match, then reuse the assignment.
                            if (e.getValue() == cnt) {
                                cnts.put(e.getKey(), cnt);

                                used.add(e.getKey());

                                if (--remainder == 0)
                                    break;
                            }
                        }

                        if (remainder > 0) {
                            List<Map.Entry<UUID, Integer>> entries = new ArrayList<>(cnts.entrySet());

                            // Randomize.
                            Collections.shuffle(entries, rnd);

                            for (Map.Entry<UUID, Integer> e : entries) {
                                // Assign only the ones that have not been reused from previous assignments.
                                if (!used.contains(e.getKey())) {
                                    if (e.getValue() < maxPerNodeCnt || maxPerNodeCnt == 0) {
                                        e.setValue(e.getValue() + 1);

                                        if (--remainder == 0)
                                            break;
                                    }
                                }
                            }
                        }
                    }
                    else {
                        List<Map.Entry<UUID, Integer>> entries = new ArrayList<>(cnts.entrySet());

                        // Randomize.
                        Collections.shuffle(entries, rnd);

                        for (Map.Entry<UUID, Integer> e : entries) {
                            e.setValue(e.getValue() + 1);

                            if (--remainder == 0)
                                break;
                        }
                    }
                }
            }
        }

        return cnts;
    }

    /**
     * Redeploys local services based on assignments.
     * <p/>
     * Invokes from services deployment worker.
     *
     * @param srvcId Service id.
     * @param cfg Service configuration.
     * @param top Service topology.
     * @throws IgniteCheckedException In case of deployment errors.
     */
    void redeploy(IgniteUuid srvcId, ServiceConfiguration cfg,
        Map<UUID, Integer> top) throws IgniteCheckedException {
        String name = cfg.getName();
        String cacheName = cfg.getCacheName();
        Object affKey = cfg.getAffinityKey();

        int assignCnt = top.getOrDefault(ctx.localNodeId(), 0);

        Collection<ServiceContextImpl> ctxs = locServices.computeIfAbsent(srvcId, c -> new ArrayList<>());

        Collection<ServiceContextImpl> toInit = new ArrayList<>();

        synchronized (ctxs) {
            if (ctxs.size() > assignCnt) {
                int cancelCnt = ctxs.size() - assignCnt;

                cancel(ctxs, cancelCnt);
            }
            else if (ctxs.size() < assignCnt) {
                int createCnt = assignCnt - ctxs.size();

                for (int i = 0; i < createCnt; i++) {
                    ServiceContextImpl srvcCtx = new ServiceContextImpl(name,
                        UUID.randomUUID(),
                        cacheName,
                        affKey,
                        Executors.newSingleThreadExecutor(threadFactory));

                    ctxs.add(srvcCtx);

                    toInit.add(srvcCtx);
                }
            }
        }

        for (final ServiceContextImpl srvcCtx : toInit) {
            final Service srvc;

            try {
                srvc = copyAndInject(cfg);

                // Initialize service.
                srvc.init(srvcCtx);

                srvcCtx.service(srvc);
            }
            catch (Throwable e) {
                U.error(log, "Failed to initialize service (service will not be deployed): " + name, e);

                synchronized (ctxs) {
                    ctxs.removeAll(toInit);
                }

                throw new IgniteCheckedException("Error occured during service initialization: " +
                    "[locId=" + ctx.localNodeId() + ", name=" + name + ']', e);
            }

            if (log.isInfoEnabled())
                log.info("Starting service instance [name=" + srvcCtx.name() + ", execId=" +
                    srvcCtx.executionId() + ']');

            // Start service in its own thread.
            final ExecutorService exe = srvcCtx.executor();

            exe.execute(new Runnable() {
                @Override public void run() {
                    try {
                        srvc.execute(srvcCtx);
                    }
                    catch (InterruptedException | IgniteInterruptedCheckedException ignore) {
                        if (log.isDebugEnabled())
                            log.debug("Service thread was interrupted [name=" + srvcCtx.name() + ", execId=" +
                                srvcCtx.executionId() + ']');
                    }
                    catch (IgniteException e) {
                        if (e.hasCause(InterruptedException.class) ||
                            e.hasCause(IgniteInterruptedCheckedException.class)) {
                            if (log.isDebugEnabled())
                                log.debug("Service thread was interrupted [name=" + srvcCtx.name() +
                                    ", execId=" + srvcCtx.executionId() + ']');
                        }
                        else {
                            U.error(log, "Service execution stopped with error [name=" + srvcCtx.name() +
                                ", execId=" + srvcCtx.executionId() + ']', e);
                        }
                    }
                    catch (Throwable e) {
                        U.error(log, "Service execution stopped with error [name=" + srvcCtx.name() +
                            ", execId=" + srvcCtx.executionId() + ']', e);

                        if (e instanceof Error)
                            throw (Error)e;
                    }
                    finally {
                        // Suicide.
                        exe.shutdownNow();
                    }
                }
            });
        }
    }

    /**
     * @param cfg Service configuration.
     * @return Copy of service.
     * @throws IgniteCheckedException If failed.
     */
    private Service copyAndInject(ServiceConfiguration cfg) throws IgniteCheckedException {
        if (cfg instanceof LazyServiceConfiguration) {
            LazyServiceConfiguration srvcCfg = (LazyServiceConfiguration)cfg;

            GridDeployment srvcDep = ctx.deploy().getDeployment(srvcCfg.serviceClassName());

            byte[] bytes = ((LazyServiceConfiguration)cfg).serviceBytes();

            Service srvc = U.unmarshal(marsh, bytes,
                U.resolveClassLoader(srvcDep != null ? srvcDep.classLoader() : null, ctx.config()));

            ctx.resource().inject(srvc);

            return srvc;
        }
        else {
            Service srvc = cfg.getService();

            try {
                byte[] bytes = U.marshal(marsh, srvc);

                Service cp = U.unmarshal(marsh, bytes, U.resolveClassLoader(srvc.getClass().getClassLoader(), ctx.config()));

                ctx.resource().inject(cp);

                return cp;
            }
            catch (IgniteCheckedException e) {
                U.error(log, "Failed to copy service (will reuse same instance): " + srvc.getClass(), e);

                return srvc;
            }
        }
    }

    /**
     * @param ctxs Contexts to cancel.
     * @param cancelCnt Number of contexts to cancel.
     */
    private void cancel(Iterable<ServiceContextImpl> ctxs, int cancelCnt) {
        for (Iterator<ServiceContextImpl> it = ctxs.iterator(); it.hasNext(); ) {
            cancel(it.next());

            it.remove();

            if (--cancelCnt == 0)
                break;
        }
    }

    /**
     * Perform cancelation on given service context.
     *
     * @param ctx Service context.
     */
    private void cancel(ServiceContextImpl ctx) {
        // Flip cancelled flag.
        ctx.setCancelled(true);

        // Notify service about cancellation.
        Service srvc = ctx.service();

        if (srvc != null) {
            try {
                srvc.cancel(ctx);
            }
            catch (Throwable e) {
                U.error(log, "Failed to cancel service (ignoring) [name=" + ctx.name() +
                    ", execId=" + ctx.executionId() + ']', e);

                if (e instanceof Error)
                    throw e;
            }
            finally {
                try {
                    this.ctx.resource().cleanup(srvc);
                }
                catch (IgniteCheckedException e) {
                    U.error(log, "Failed to clean up service (will ignore): " + ctx.name(), e);
                }
            }
        }

        // Close out executor thread for the service.
        // This will cause the thread to be interrupted.
        ctx.executor().shutdownNow();

        if (log.isInfoEnabled()) {
            log.info("Cancelled service instance [name=" + ctx.name() + ", execId=" +
                ctx.executionId() + ']');
        }
    }

    /**
     * Undeployes service with given id.
     * <p/>
     * Invokes from services deployment worker.
     *
     * @param srvcId Service id.
     */
    void undeploy(@NotNull IgniteUuid srvcId) {
        Collection<ServiceContextImpl> ctxs = locServices.remove(srvcId);

        if (ctxs != null) {
            synchronized (ctxs) {
                cancel(ctxs, ctxs.size());
            }
        }
    }

    /**
     * @param deploy {@code true} if complete deployment requests, otherwise complete undeployment request will be
     * completed.
     * @param reqSrvcId Request's service id.
     * @param err Error to complete with. If {@code null} a future will be completed successfully.
     */
    void completeInitiatingFuture(boolean deploy, IgniteUuid reqSrvcId, Throwable err) {
        GridFutureAdapter<?> fut = deploy ? depFuts.remove(reqSrvcId) : undepFuts.remove(reqSrvcId);

        if (fut == null)
            return;

        if (err != null) {
            fut.onDone(err);

            if (deploy) {
                U.warn(log, "Failed to deploy service, cfg=" +
                    ((GridServiceDeploymentFuture)fut).configuration(), err);
            }
            else
                U.warn(log, "Failed to undeploy service, srvcId=" + reqSrvcId, err);
        }
        else
            fut.onDone();
    }

    /**
     * Processes deployment result.
     *
     * @param fullTops Deployment topologies.
     */
    void updateServicesTopologies(@NotNull final Map<IgniteUuid, Map<UUID, Integer>> fullTops) {
        if (!enterBusy())
            return;

        try {
            updateServicesMap(deployedServices, fullTops);
        }
        finally {
            leaveBusy();
        }
    }

    /**
     * @param name Service name;
     * @return @return Service's id if exists, otherwise {@code null};
     */
    @Nullable private IgniteUuid lookupDeployedServiceId(String name) {
        for (ServiceInfo desc : deployedServices.values()) {
            if (desc.name().equals(name))
                return desc.serviceId();
        }

        return null;
    }

    /**
     * @param srvcId Service id.
     * @return Count of locally deployed service with given id.
     */
    int localInstancesCount(IgniteUuid srvcId) {
        Collection<ServiceContextImpl> ctxs = locServices.get(srvcId);

        if (ctxs == null)
            return 0;

        synchronized (ctxs) {
            return ctxs.size();
        }
    }

    /**
     * Updates deployed services map according to deployment task.
     * <p/>
     * Invokes from services deployment worker.
     *
     * @param depActions Service deployment actions.
     */
    void updateDeployedServices(final ServiceDeploymentActions depActions) {
        if (!enterBusy())
            return;

        try {
            depActions.servicesToDeploy().forEach(deployedServices::putIfAbsent);

            depActions.servicesToUndeploy().forEach((srvcId, desc) -> {
                ServiceInfo rmv = deployedServices.remove(srvcId);

                assert rmv != null && rmv == desc : "Concurrent map modification.";
            });
        }
        finally {
            leaveBusy();
        }
    }

    /**
     * @return Deployed services information.
     */
    Map<IgniteUuid, ServiceInfo> deployedServices() {
        return new HashMap<>(deployedServices);
    }

    /**
     * Gets services received to deploy from node with given id on joining.
     *
     * @param nodeId Joined node id.
     * @return Services to deploy.
     */
    @NotNull Map<IgniteUuid, ServiceInfo> servicesReceivedFromJoin(UUID nodeId) {
        Map<IgniteUuid, ServiceInfo> descs = new HashMap<>();

        registeredServices.forEach((srvcId, desc) -> {
            if (desc.staticallyConfigured() && desc.originNodeId().equals(nodeId))
                descs.put(srvcId, desc);
        });

        return descs;
    }

    /**
     * @return Cluster coordinator, {@code null} if failed to determine.
     */
    @Nullable ClusterNode coordinator() {
        return U.oldest(ctx.discovery().aliveServerNodes(), null);
    }

    /**
     * @return {@code true} if local node is coordinator.
     */
    private boolean isLocalNodeCoordinator() {
        DiscoverySpi spi = ctx.discovery().getInjectedDiscoverySpi();

        return spi instanceof TcpDiscoverySpi ?
            ((TcpDiscoverySpi)spi).isLocalNodeCoordinator() :
            F.eq(ctx.discovery().localNode(), coordinator());
    }

    /** {@inheritDoc} */
    @Override public void onLocalJoin(DiscoveryEvent evt, DiscoCache discoCache) {
        assert ctx.localNodeId().equals(evt.eventNode().id());
        assert evt.type() == EVT_NODE_JOINED;

        if (isLocalNodeCoordinator()) {
            // First node start, method onGridDataReceived(DiscoveryDataBag.GridDiscoveryData) has not been called.
            ArrayList<ServiceInfo> staticServicesInfo = staticallyConfiguredServices(false);

            staticServicesInfo.forEach(desc -> registeredServices.put(desc.serviceId(), desc));
        }

        ServiceDeploymentActions depActions = null;

        if (!registeredServices.isEmpty()) {
            depActions = new ServiceDeploymentActions();

            depActions.servicesToDeploy(new HashMap<>(registeredServices));
        }

        depMgr.onLocalJoin(evt, discoCache, depActions);
    }

    /**
     * @return Services deployment manager.
     */
    public ServiceDeploymentManager deployment() {
        return depMgr;
    }

    /**
     * @param logErrors Whenever it's necessary to log validation failures.
     * @return Statically configured services.
     */
    @NotNull private ArrayList<ServiceInfo> staticallyConfiguredServices(boolean logErrors) {
        ServiceConfiguration[] cfgs = ctx.config().getServiceConfiguration();

        ArrayList<ServiceInfo> staticServicesInfo = new ArrayList<>();

        if (cfgs != null) {
            PreparedConfigurations<IgniteUuid> prepCfgs = prepareServiceConfigurations(Arrays.asList(cfgs),
                node -> !node.isClient());

            if (logErrors) {
                if (prepCfgs.failedFuts != null) {
                    for (GridServiceDeploymentFuture<IgniteUuid> fut : prepCfgs.failedFuts) {
                        U.warn(log, "Failed to validate static service configuration (won't be deployed), " +
                            "cfg=" + fut.configuration() + ", err=" + fut.result());
                    }
                }
            }

            for (ServiceConfiguration srvcCfg : prepCfgs.cfgs)
                staticServicesInfo.add(new ServiceInfo(ctx.localNodeId(), IgniteUuid.randomUuid(), srvcCfg, true));
        }

        return staticServicesInfo;
    }

    /**
     * @param snd Sender.
     * @param msg Message.
     */
    private void processServicesChangeRequest(ClusterNode snd, ServiceChangeBatchRequest msg) {
        DiscoveryDataClusterState state = ctx.state().clusterState();

        if (!state.active() || state.transition()) {
            for (ServiceChangeAbstractRequest req : msg.requests()) {
                GridFutureAdapter<?> fut = null;

                if (req instanceof ServiceDeploymentRequest)
                    fut = depFuts.remove(req.serviceId());
                else if (req instanceof ServiceUndeploymentRequest)
                    fut = undepFuts.remove(req.serviceId());

                if (fut != null) {
                    fut.onDone(new IgniteCheckedException("Operation has been canceled, cluster state " +
                        "change is in progress."));
                }
            }

            return;
        }

        Map<IgniteUuid, ServiceInfo> toDeploy = new HashMap<>();
        Map<IgniteUuid, ServiceInfo> toUndeploy = new HashMap<>();

        for (ServiceChangeAbstractRequest req : msg.requests()) {
            IgniteUuid reqSrvcId = req.serviceId();
            ServiceInfo oldDesc = registeredServices.get(reqSrvcId);

            if (req instanceof ServiceDeploymentRequest) {
                IgniteCheckedException err = null;

                if (oldDesc != null) { // In case of a collision of IgniteUuid.randomUuid() (almost impossible case)
                    err = new IgniteCheckedException("Failed to deploy service. Service with generated id already" +
                        "exists : [" + "srvcId" + reqSrvcId + ", srvcTop=" + oldDesc.topologySnapshot() + ']');
                }
                else {
                    ServiceConfiguration cfg = ((ServiceDeploymentRequest)req).configuration();

                    oldDesc = lookupInRegisteredServices(cfg.getName());

                    if (oldDesc == null) {
                        if (cfg.getCacheName() != null && ctx.cache().cacheDescriptor(cfg.getCacheName()) == null) {
                            err = new IgniteCheckedException("Failed to deploy service, " +
                                "affinity cache is not found, cfg=" + cfg);
                        }
                        else {
                            ServiceInfo desc = new ServiceInfo(snd.id(), reqSrvcId, cfg);

                            registeredServices.put(reqSrvcId, desc);

                            toDeploy.put(reqSrvcId, desc);
                        }
                    }
                    else {
                        if (!oldDesc.configuration().equalsIgnoreNodeFilter(cfg)) {
                            err = new IgniteCheckedException("Failed to deploy service " +
                                "(service already exists with different configuration) : " +
                                "[deployed=" + oldDesc.configuration() + ", new=" + cfg + ']');
                        }
                        else {
                            GridServiceDeploymentFuture<IgniteUuid> fut = depFuts.remove(reqSrvcId);

                            if (fut != null) {
                                fut.onDone();

                                if (log.isDebugEnabled()) {
                                    log.debug("Service sent to deploy is already deployed : " +
                                        "[srvcId=" + oldDesc.serviceId() + ", cfg=" + oldDesc.configuration());
                                }
                            }
                        }
                    }
                }

                if (err != null) {
                    completeInitiatingFuture(true, reqSrvcId, err);

                    U.warn(log, err.getMessage(), err);
                }
            }
            else if (req instanceof ServiceUndeploymentRequest) {
                ServiceInfo rmv = registeredServices.remove(reqSrvcId);

                assert oldDesc == rmv : "Concurrent map modification.";

                toUndeploy.put(reqSrvcId, rmv);
            }
        }

        if (!toDeploy.isEmpty() || !toUndeploy.isEmpty()) {
            ServiceDeploymentActions depActions = new ServiceDeploymentActions();

            if (!toDeploy.isEmpty())
                depActions.servicesToDeploy(toDeploy);

            if (!toUndeploy.isEmpty())
                depActions.servicesToUndeploy(toUndeploy);

            msg.servicesDeploymentActions(depActions);
        }
    }

    /**
     * @param msg Message.
     */
    private void processChangeGlobalStateRequest(ChangeGlobalStateMessage msg) {
        if (msg.activate() && registeredServices.isEmpty())
            return;

        ServiceDeploymentActions depActions = new ServiceDeploymentActions();

        if (msg.activate())
            depActions.servicesToDeploy(new HashMap<>(registeredServices));
        else
            depActions.deactivate(true);

        msg.servicesDeploymentActions(depActions);
    }

    /**
     * @param msg Message.
     */
    private void processDynamicCacheChangeRequest(DynamicCacheChangeBatch msg) {
        Map<IgniteUuid, ServiceInfo> toUndeploy = new HashMap<>();

        for (DynamicCacheChangeRequest chReq : msg.requests()) {
            if (chReq.stop()) {
                registeredServices.entrySet().removeIf(e -> {
                    ServiceInfo desc = e.getValue();

                    if (Objects.equals(desc.cacheName(), chReq.cacheName())) {
                        toUndeploy.put(desc.serviceId(), desc);

                        return true;
                    }

                    return false;
                });
            }
        }

        if (!toUndeploy.isEmpty()) {
            ServiceDeploymentActions depActions = new ServiceDeploymentActions();

            depActions.servicesToUndeploy(toUndeploy);

            msg.servicesDeploymentActions(depActions);
        }
    }

    /**
     * @param msg Message.
     */
    private void processServicesFullDeployments(ServiceClusterDeploymentResultBatch msg) {
        final Map<IgniteUuid, Map<UUID, Integer>> fullTops = new HashMap<>();
        final Map<IgniteUuid, Collection<byte[]>> fullErrors = new HashMap<>();

        for (ServiceClusterDeploymentResult depRes : msg.results()) {
            final IgniteUuid srvcId = depRes.serviceId();
            final Map<UUID, ServiceSingleNodeDeploymentResult> deps = depRes.results();

            final Map<UUID, Integer> top = new HashMap<>();
            final Collection<byte[]> errors = new ArrayList<>();

            deps.forEach((nodeId, res) -> {
                int cnt = res.count();

                if (cnt > 0)
                    top.put(nodeId, cnt);

                if (!res.errors().isEmpty())
                    errors.addAll(res.errors());
            });

            if (!errors.isEmpty())
                fullErrors.computeIfAbsent(srvcId, e -> new ArrayList<>()).addAll(errors);

            fullTops.put(srvcId, top);
        }

        synchronized (servicesTopsUpdateMux) {
            updateServicesMap(registeredServices, fullTops);

            servicesTopsUpdateMux.notifyAll();
        }

        ServiceDeploymentActions depActions = new ServiceDeploymentActions();

        depActions.deploymentTopologies(fullTops);
        depActions.deploymentErrors(fullErrors);

        msg.servicesDeploymentActions(depActions);
    }

    /**
     * @param name Service name.
     * @return Mapped service descriptor. Possibly {@code null} if not found.
     */
    @Nullable private ServiceInfo lookupInRegisteredServices(String name) {
        for (ServiceInfo desc : registeredServices.values()) {
            if (desc.name().equals(name))
                return desc;
        }

        return null;
    }

    /**
     * Updates services info according to given arguments.
     *
     * @param services Services info to update.
     * @param tops Deployment topologies.
     */
    private void updateServicesMap(Map<IgniteUuid, ServiceInfo> services,
        Map<IgniteUuid, Map<UUID, Integer>> tops) {

        tops.forEach((srvcId, top) -> {
            ServiceInfo desc = services.get(srvcId);

            if (desc != null)
                desc.topologySnapshot(top);
        });
    }

    /**
     * Enters busy state.
     *
     * @return {@code true} if entered to busy state.
     */
    private boolean enterBusy() {
        return opsLock.readLock().tryLock();
    }

    /**
     * Leaves busy state.
     */
    private void leaveBusy() {
        opsLock.readLock().unlock();
    }
}
