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
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
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
import org.apache.ignite.internal.managers.discovery.DiscoCache;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.DynamicCacheChangeBatch;
import org.apache.ignite.internal.processors.cache.DynamicCacheChangeRequest;
import org.apache.ignite.internal.processors.cluster.ChangeGlobalStateMessage;
import org.apache.ignite.internal.processors.cluster.DiscoveryDataClusterState;
import org.apache.ignite.internal.processors.cluster.IgniteChangeGlobalStateSupport;
import org.apache.ignite.internal.processors.marshaller.GridMarshallerMappingProcessor;
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
import org.apache.ignite.spi.discovery.DiscoveryDataBag;
import org.apache.ignite.spi.discovery.DiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
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
 * Event driven implementation of service processor. Services deployment are managed by messages across discovery spi
 * and communication spi.
 */
@SkipDaemon
public class IgniteServiceProcessor extends IgniteServiceProcessorAdapter implements IgniteChangeGlobalStateSupport {
    /** Local service instances. */
    private final ConcurrentMap<IgniteUuid, Collection<ServiceContextImpl>> locServices = new ConcurrentHashMap<>();

    /** Cluster services info <b>updated from discovery thread</b>. */
    private final ConcurrentMap<IgniteUuid, ServiceInfo> registeredServices = new ConcurrentHashMap<>();

    /** Cluster services info <b>updated from deployer thread</b>. */
    private final ConcurrentMap<IgniteUuid, ServiceInfo> deployedServices = new ConcurrentHashMap<>();

    /** Deployment futures. */
    private final ConcurrentMap<IgniteUuid, GridServiceDeploymentFuture> depFuts = new ConcurrentHashMap<>();

    /** Undeployment futures. */
    private final ConcurrentMap<IgniteUuid, GridFutureAdapter<?>> undepFuts = new ConcurrentHashMap<>();

    /** Thread factory. */
    private final ThreadFactory threadFactory = new IgniteThreadFactory(ctx.igniteInstanceName(), "service",
        new OomExceptionHandler(ctx));

    /** Services deployment manager. */
    private volatile ServicesDeploymentManager depMgr = new ServicesDeploymentManager(ctx);

    /** Services topologies update mutex. */
    private final Object servicesTopsUpdateMux = new Object();

    /** Operations lock. */
    private final ReentrantReadWriteLock opsLock = new ReentrantReadWriteLock();

    /** Disconnected flag. */
    private volatile boolean disconnected;

    /**
     * @param ctx Kernal context.
     */
    public IgniteServiceProcessor(GridKernalContext ctx) {
        super(ctx);

    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteCheckedException {
        IgniteConfiguration cfg = ctx.config();

        DeploymentMode depMode = cfg.getDeploymentMode();

        if (cfg.isPeerClassLoadingEnabled() && (depMode == PRIVATE || depMode == ISOLATED) &&
            !F.isEmpty(cfg.getServiceConfiguration()))
            throw new IgniteCheckedException("Cannot deploy services in PRIVATE or ISOLATED deployment mode: " + depMode);
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
        depFuts.values().forEach(fut -> fut.onDone(stopError));
        depFuts.clear();

        undepFuts.values().forEach(fut -> fut.onDone(stopError));
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

        ServicesCommonDiscoveryData clusterData = new ServicesCommonDiscoveryData(
            new ArrayList<>(registeredServices.values())
        );

        dataBag.addGridCommonData(SERVICE_PROC.ordinal(), clusterData);
    }

    /** {@inheritDoc} */
    @Override public void onGridDataReceived(DiscoveryDataBag.GridDiscoveryData data) {
        if (data.commonData() == null)
            return;

        ServicesCommonDiscoveryData clusterData = (ServicesCommonDiscoveryData)data.commonData();

        for (ServiceInfo desc : clusterData.registeredServices())
            registeredServices.put(desc.serviceId(), desc);
    }

    /** {@inheritDoc} */
    @Override public void collectJoiningNodeData(DiscoveryDataBag dataBag) {
        ArrayList<ServiceInfo> staticServicesInfo = staticallyConfiguredServices(true);

        dataBag.addJoiningNodeData(SERVICE_PROC.ordinal(), new ServicesJoinNodeDiscoveryData(staticServicesInfo));
    }

    /** {@inheritDoc} */
    @Override public void onJoiningNodeDataReceived(DiscoveryDataBag.JoiningNodeDiscoveryData data) {
        if (data.joiningNodeData() == null)
            return;

        ServicesJoinNodeDiscoveryData joinData = (ServicesJoinNodeDiscoveryData)data.joiningNodeData();

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
                U.warn(log, "Ignore service configuration received from joining node : " +
                    "[nodeId=" + data.joiningNodeId() + ", cfgName=" + desc.name() + "]. " +
                    "The same service configuration already registered.");
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
     * Invokes from deployer worker.
     * <p/>
     * {@inheritDoc}
     */
    @Override public void onDeActivate(GridKernalContext kctx) {
        if (!opsLock.writeLock().tryLock())
            return;

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

            depMgr = new ServicesDeploymentManager(ctx);

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
     * @param staticCfgs {@code true} to check statically defined services during node startup process, in this case
     * {@link JdkMarshaller} (common marshaller for discovery layer) will be used to check services serialization
     * because of {@link GridMarshallerMappingProcessor} is not started at this point, otherwise {@code false}.
     * @return Configurations to deploy.
     */
    private PreparedConfigurations prepareServiceConfigurations(Collection<ServiceConfiguration> cfgs,
        IgnitePredicate<ClusterNode> dfltNodeFilter, boolean staticCfgs) {
        List<ServiceConfiguration> cfgsCp = new ArrayList<>(cfgs.size());

        Marshaller marsh = !staticCfgs ? ctx.config().getMarshaller() : new JdkMarshaller();

        List<GridServiceDeploymentFuture> failedFuts = null;

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

                    if (!staticCfgs)
                        cfgsCp.add(new LazyServiceConfiguration(cfg, srvcBytes));
                    else
                        cfgsCp.add(cfg);
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

                GridServiceDeploymentFuture fut = new GridServiceDeploymentFuture(cfg, null);

                fut.onDone(err);

                failedFuts.add(fut);
            }
        }

        return new PreparedConfigurations(cfgsCp, failedFuts);
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
            ctx.security().authorize(name, perm, null);

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

            PreparedConfigurations srvCfg = prepareServiceConfigurations(cfgs, dfltNodeFilter, false);

            List<ServiceConfiguration> cfgsCp = srvCfg.cfgs;

            List<GridServiceDeploymentFuture> failedFuts = srvCfg.failedFuts;

            GridServiceDeploymentCompoundFuture<IgniteUuid> res = new GridServiceDeploymentCompoundFuture();

            if (!cfgsCp.isEmpty()) {
                try {
                    Collection<DynamicServiceChangeRequest> reqs = new ArrayList<>();

                    for (ServiceConfiguration cfg : cfgsCp) {
                        IgniteUuid srvcId = IgniteUuid.randomUuid();

                        GridServiceDeploymentFuture fut = new GridServiceDeploymentFuture(cfg, srvcId);

                        res.add(fut, true);

                        DynamicServiceChangeRequest req = DynamicServiceChangeRequest.deploymentRequest(srvcId, cfg);

                        reqs.add(req);

                        depFuts.put(srvcId, fut);
                    }

                    DynamicServicesChangeRequestBatchMessage msg = new DynamicServicesChangeRequestBatchMessage(reqs);

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
                for (GridServiceDeploymentFuture fut : failedFuts)
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

            List<DynamicServiceChangeRequest> reqs = new ArrayList<>();

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

                    DynamicServiceChangeRequest req = DynamicServiceChangeRequest.undeploymentRequest(srvcId);

                    reqs.add(req);
                }

                if (!reqs.isEmpty()) {
                    DynamicServicesChangeRequestBatchMessage msg = new DynamicServicesChangeRequestBatchMessage(reqs);

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

        Map<UUID, Integer> top;

        while (true) {
            top = serviceTopology(name);

            if (timeout == 0 || (top != null && !top.isEmpty()))
                return top;

            synchronized (servicesTopsUpdateMux) {
                long wait = timeout - (U.currentTimeMillis() - startTime);

                if (wait <= 0)
                    return top;

                try {
                    servicesTopsUpdateMux.wait(wait);
                }
                catch (InterruptedException e) {
                    throw new IgniteInterruptedCheckedException(e);
                }
            }
        }
    }

    /**
     * @param name Service name.
     * @return Service topology.
     */
    private Map<UUID, Integer> serviceTopology(String name) {
        for (ServiceInfo desc : registeredServices.values()) {
            if (desc.name().equals(name))
                return desc.topologySnapshot();
        }

        return null;
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
            ctx.security().authorize(name, SecurityPermission.SERVICE_INVOKE, null);

            Collection<ServiceContextImpl> ctxs = serviceContexts(name);

            if (F.isEmpty(ctxs))
                return null;

            for (ServiceContextImpl ctx : ctxs) {
                Service srvc = ctx.service();

                if (srvc != null)
                    return (T)srvc;
            }

            return null;
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

            if (F.isEmpty(ctxs))
                return null;

            for (ServiceContextImpl ctx : ctxs) {
                if (ctx.service() != null)
                    return ctx;
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
        ctx.security().authorize(name, SecurityPermission.SERVICE_INVOKE, null);

        if (hasLocalNode(prj)) {
            ServiceContextImpl ctx = serviceContext(name);

            if (ctx != null) {
                Service srvc = ctx.service();

                if (srvc != null) {
                    if (!srvcCls.isAssignableFrom(srvc.getClass()))
                        throw new IgniteException("Service does not implement specified interface [srvcCls=" +
                            srvcCls.getName() + ", srvcCls=" + srvc.getClass().getName() + ']');

                    return (T)srvc;
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
            ctx.security().authorize(name, SecurityPermission.SERVICE_INVOKE, null);

            Collection<ServiceContextImpl> ctxs = serviceContexts(name);

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
    protected Map<UUID, Integer> reassign(@NotNull IgniteUuid srvcId, @NotNull ServiceConfiguration cfg,
        @NotNull AffinityTopologyVersion topVer, @Nullable Map<UUID, Integer> oldTop) throws IgniteCheckedException {
        Object nodeFilter = cfg.getNodeFilter();

        if (nodeFilter != null)
            ctx.resource().injectGeneric(nodeFilter);

        int totalCnt = cfg.getTotalCount();
        int maxPerNodeCnt = cfg.getMaxPerNodeCount();
        String cacheName = cfg.getCacheName();
        Object affKey = cfg.getAffinityKey();

        Map<UUID, Integer> cnts = new HashMap<>();

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
                        Collection<UUID> used = new HashSet<>();

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
     * Invokes from deployer worker.
     *
     * @param srvcId Service id.
     * @param cfg Service configuration.
     * @param top Service topology.
     */
    protected void redeploy(IgniteUuid srvcId, ServiceConfiguration cfg, Map<UUID, Integer> top) {
        String name = cfg.getName();
        String cacheName = cfg.getCacheName();
        Object affKey = cfg.getAffinityKey();

        int assignCnt = top.getOrDefault(ctx.localNodeId(), 0);

        Collection<ServiceContextImpl> ctxs = locServices.computeIfAbsent(srvcId, c -> new ArrayList<>());

        Collection<ServiceContextImpl> toInit = new ArrayList<>();

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

                ctxs.removeAll(toInit);

                if (e instanceof Error)
                    throw (Error)e;

                if (e instanceof RuntimeException)
                    throw (RuntimeException)e;

                return;
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
    @SuppressWarnings("deprecation")
    private Service copyAndInject(ServiceConfiguration cfg) throws IgniteCheckedException {
        Marshaller m = ctx.config().getMarshaller();

        if (cfg instanceof LazyServiceConfiguration) {
            byte[] bytes = ((LazyServiceConfiguration)cfg).serviceBytes();

            Service srvc = U.unmarshal(m, bytes, U.resolveClassLoader(null, ctx.config()));

            ctx.resource().inject(srvc);

            return srvc;
        }
        else {
            Service srvc = cfg.getService();

            try {
                byte[] bytes = U.marshal(m, srvc);

                Service cp = U.unmarshal(m, bytes, U.resolveClassLoader(srvc.getClass().getClassLoader(), ctx.config()));

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
     * Invokes from deployer worker.
     *
     * @param srvcId Service id.
     */
    protected void undeploy(@NotNull IgniteUuid srvcId) {
        Collection<ServiceContextImpl> ctxs = locServices.remove(srvcId);

        if (ctxs != null)
            cancel(ctxs, ctxs.size());
    }

    /**
     * @param deploy {@code true} if complete deployment requests, otherwise complete undeployment request will be
     * completed.
     * @param reqSrvcId Request's service id.
     * @param err Error to complete with. If {@code null} a future will be completed successfully.
     */
    protected void completeInitiatingFuture(boolean deploy, IgniteUuid reqSrvcId, Throwable err) {
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
     * @param fullErrors Deployment errors.
     */
    protected void updateServicesTopologies(@NotNull final Map<IgniteUuid, HashMap<UUID, Integer>> fullTops,
        @NotNull final Map<IgniteUuid, Collection<byte[]>> fullErrors) {
        if (!enterBusy())
            return;

        try {
            updateServicesMap(deployedServices, fullTops, fullErrors);
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
    protected int localInstancesCount(IgniteUuid srvcId) {
        Collection<ServiceContextImpl> ctxs = locServices.get(srvcId);

        return ctxs != null ? ctxs.size() : 0;
    }

    /**
     * Updates deployed services map according to deployment task.
     * <p/>
     * Invokes from deployer worker.
     *
     * @param depActions Service deployment actions.
     */
    protected void updateDeployedServices(final ServicesDeploymentActions depActions) {
        if (!enterBusy())
            return;

        try {
            depActions.servicesToDeploy().forEach((srvcId, desc) -> {
                ServiceInfo old = deployedServices.putIfAbsent(srvcId, desc);

                assert old == desc || old == null : "Concurrent map modification.";
            });

            depActions.servicesToUndeploy().forEach((srvcId, desc) -> {
                ServiceInfo rmv = deployedServices.remove(srvcId);

                assert rmv == desc || rmv == null : "Concurrent map modification.";
            });
        }
        finally {
            leaveBusy();
        }
    }

    /**
     * @return Deployed services information.
     */
    protected Map<IgniteUuid, ServiceInfo> deployedServices() {
        return new HashMap<>(deployedServices);
    }

    /**
     * Gets services received to deploy from node with given id on joining.
     *
     * @param nodeId Joined node id.
     * @return Services to deploy.
     */
    @NotNull protected Map<IgniteUuid, ServiceInfo> servicesReceivedFromJoin(UUID nodeId) {
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
    @Nullable protected ClusterNode coordinator() {
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

    /**
     * Special handler for local join events for which the regular events are not generated.
     * <p/>
     * Local join event is expected in cases of joining to topology or client reconnect.
     *
     * @param evt Discovery event.
     * @param discoCache Discovery cache.
     */
    public void onLocalJoin(DiscoveryEvent evt, DiscoCache discoCache) {
        assert ctx.localNodeId().equals(evt.eventNode().id());
        assert evt.type() == EVT_NODE_JOINED;

        if (isLocalNodeCoordinator()) {
            // First node start, {@link #onGridDataReceived(DiscoveryDataBag.GridDiscoveryData)} has not been called
            ArrayList<ServiceInfo> staticServicesInfo = staticallyConfiguredServices(false);

            staticServicesInfo.forEach(desc -> registeredServices.put(desc.serviceId(), desc));
        }

        ServicesDeploymentActions depActions = null;

        if (!registeredServices.isEmpty()) {
            depActions = new ServicesDeploymentActions();

            depActions.servicesToDeploy(new HashMap<>(registeredServices));
        }

        depMgr.onLocalJoin(evt, discoCache, depActions);
    }

    /**
     * @param logErrors Whenever it's necessary to log validation failures.
     * @return Statically configured services.
     */
    @NotNull private ArrayList<ServiceInfo> staticallyConfiguredServices(boolean logErrors) {
        ServiceConfiguration[] cfgs = ctx.config().getServiceConfiguration();

        ArrayList<ServiceInfo> staticServicesInfo = new ArrayList<>();

        if (cfgs != null) {
            PreparedConfigurations prepCfgs = prepareServiceConfigurations(Arrays.asList(cfgs),
                node -> !node.isClient(), true);

            if (logErrors) {
                if (prepCfgs.failedFuts != null) {
                    for (GridServiceDeploymentFuture fut : prepCfgs.failedFuts) {
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
     * @return Services deployment manager.
     */
    public ServicesDeploymentManager deployment() {
        return depMgr;
    }

    /**
     * Callback invoked from discovery thread when discovery custom message is received, before discovery listeners
     * notification.
     *
     * @param msg Discovery custom message.
     * @param node Event node.
     * @param state Current cluster state.
     */
    public void onCustomEvent(DiscoveryCustomMessage msg, ClusterNode node, DiscoveryDataClusterState state) {
        assert msg != null;

        if (msg instanceof DynamicServicesChangeRequestBatchMessage) {
            DynamicServicesChangeRequestBatchMessage msg0 = ((DynamicServicesChangeRequestBatchMessage)msg);

            if (!state.active() || state.transition()) {
                for (DynamicServiceChangeRequest req : msg0.requests()) {
                    GridFutureAdapter<?> fut = req.deploy() ?
                        depFuts.remove(req.serviceId()) :
                        undepFuts.remove(req.serviceId());

                    if (fut != null) {
                        fut.onDone(new IgniteCheckedException("Operation has been cancelled " +
                            "cluster state change is in progress."));
                    }
                }

                return;
            }

            Map<IgniteUuid, ServiceInfo> toDeploy = new HashMap<>();
            Map<IgniteUuid, ServiceInfo> toUndeploy = new HashMap<>();

            for (DynamicServiceChangeRequest req : msg0.requests()) {
                IgniteUuid reqSrvcId = req.serviceId();
                ServiceInfo oldDesc = registeredServices.get(reqSrvcId);

                if (req.deploy()) {
                    IgniteCheckedException err = null;

                    if (oldDesc != null) { // In case of a collision of IgniteUuid.randomUuid() (almost impossible case)
                        err = new IgniteCheckedException("Failed to deploy service. Service with generated id already" +
                            "exists : [" + "srvcId" + reqSrvcId + ", srvcTop=" + oldDesc.topologySnapshot() + ']');
                    }
                    else {
                        ServiceConfiguration cfg = req.configuration();

                        oldDesc = lookupInRegisteredServices(cfg.getName());

                        if (oldDesc == null) {
                            if (cfg.getCacheName() != null && ctx.cache().cacheDescriptor(cfg.getCacheName()) == null) {
                                err = new IgniteCheckedException("Failed to deploy service, " +
                                    "affinity cache is not found, cfg=" + cfg);
                            }
                            else {
                                ServiceInfo desc = new ServiceInfo(node.id(), reqSrvcId, cfg);

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
                                GridServiceDeploymentFuture fut = depFuts.remove(reqSrvcId);

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
                else if (req.undeploy()) {
                    ServiceInfo rmv = registeredServices.remove(reqSrvcId);

                    assert oldDesc == rmv : "Concurrent map modification.";

                    toUndeploy.put(reqSrvcId, rmv);
                }
            }

            if (!toDeploy.isEmpty() || !toUndeploy.isEmpty()) {
                ServicesDeploymentActions depActions = new ServicesDeploymentActions();

                if (!toDeploy.isEmpty())
                    depActions.servicesToDeploy(toDeploy);

                if (!toUndeploy.isEmpty())
                    depActions.servicesToUndeploy(toUndeploy);

                msg0.servicesDeploymentActions(depActions);
            }
        }
        else if (msg instanceof ChangeGlobalStateMessage) {
            ChangeGlobalStateMessage msg0 = (ChangeGlobalStateMessage)msg;

            if (msg0.activate() && registeredServices.isEmpty())
                return;

            ServicesDeploymentActions depActions = new ServicesDeploymentActions();

            if (msg0.activate())
                depActions.servicesToDeploy(new HashMap<>(registeredServices));
            else
                depActions.deactivate(true);

            msg0.servicesDeploymentActions(depActions);
        }
        else if (msg instanceof DynamicCacheChangeBatch) {
            DynamicCacheChangeBatch msg0 = (DynamicCacheChangeBatch)msg;
            Map<IgniteUuid, ServiceInfo> toUndeploy = new HashMap<>();

            for (DynamicCacheChangeRequest chReq : msg0.requests()) {
                if (chReq.stop()) {
                    registeredServices.entrySet().removeIf(e -> {
                        ServiceInfo desc = e.getValue();

                        if (desc.cacheName().equals(chReq.cacheName())) {
                            toUndeploy.put(desc.serviceId(), desc);

                            return true;
                        }

                        return false;
                    });
                }
            }

            if (!toUndeploy.isEmpty()) {
                ServicesDeploymentActions depActions = new ServicesDeploymentActions();

                depActions.servicesToUndeploy(toUndeploy);

                msg0.servicesDeploymentActions(depActions);
            }
        }
        else if (msg instanceof ServicesFullDeploymentsMessage) {
            ServicesFullDeploymentsMessage msg0 = (ServicesFullDeploymentsMessage)msg;

            final Map<IgniteUuid, HashMap<UUID, Integer>> fullTops = new HashMap<>();
            final Map<IgniteUuid, Collection<byte[]>> fullErrors = new HashMap<>();

            for (ServiceFullDeploymentsResults depRes : msg0.results()) {
                final IgniteUuid srvcId = depRes.serviceId();
                final Map<UUID, ServiceSingleDeploymentsResults> deps = depRes.results();

                final HashMap<UUID, Integer> top = new HashMap<>();
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
                updateServicesMap(registeredServices, fullTops, fullErrors);

                servicesTopsUpdateMux.notifyAll();
            }

            ServicesDeploymentActions depActions = new ServicesDeploymentActions();

            depActions.deploymentTopologies(fullTops);
            depActions.deploymentErrors(fullErrors);

            msg0.servicesDeploymentActions(depActions);
        }
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
     * @param errors Deployment errors.
     */
    private void updateServicesMap(Map<IgniteUuid, ServiceInfo> services,
        Map<IgniteUuid, HashMap<UUID, Integer>> tops, Map<IgniteUuid, Collection<byte[]>> errors) {

        tops.forEach((srvcId, top) -> {
            ServiceInfo desc = services.get(srvcId);

            if (desc != null)
                desc.topologySnapshot(top);
        });
    }

    /** {@inheritDoc} */
    @Override public boolean eventDrivenServiceProcessorEnabled() {
        return true;
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
