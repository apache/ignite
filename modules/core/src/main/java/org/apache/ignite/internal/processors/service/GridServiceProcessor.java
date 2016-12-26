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
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import javax.cache.Cache;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryUpdatedListener;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJobContext;
import org.apache.ignite.configuration.DeploymentMode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.GridClosureCallMode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteClientDisconnectedCheckedException;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.events.DiscoveryCustomEvent;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.managers.eventstorage.GridLocalEventListener;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheAffinityChangeMessage;
import org.apache.ignite.internal.processors.cache.CacheEntryImpl;
import org.apache.ignite.internal.processors.cache.CacheIteratorConverter;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.query.CacheQuery;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryManager;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.processors.timeout.GridTimeoutObject;
import org.apache.ignite.internal.util.GridEmptyIterator;
import org.apache.ignite.internal.util.GridSpinBusyLock;
import org.apache.ignite.internal.util.future.GridCompoundFuture;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.lang.GridCloseableIterator;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteProductVersion;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.internal.util.SerializableTransient;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.JobContextResource;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceConfiguration;
import org.apache.ignite.services.ServiceDescriptor;
import org.apache.ignite.spi.IgniteNodeValidationResult;
import org.apache.ignite.thread.IgniteThreadFactory;
import org.jetbrains.annotations.Nullable;
import org.jsr166.ConcurrentHashMap8;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_SERVICES_COMPATIBILITY_MODE;
import static org.apache.ignite.IgniteSystemProperties.getString;
import static org.apache.ignite.configuration.DeploymentMode.ISOLATED;
import static org.apache.ignite.configuration.DeploymentMode.PRIVATE;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_SERVICES_COMPATIBILITY_MODE;
import static org.apache.ignite.internal.processors.cache.GridCacheUtils.UTILITY_CACHE_NAME;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * Grid service processor.
 */
@SuppressWarnings({"SynchronizationOnLocalVariableOrMethodParameter", "ConstantConditions"})
public class GridServiceProcessor extends GridProcessorAdapter {
    /** */
    public static final IgniteProductVersion LAZY_SERVICES_CFG_SINCE = IgniteProductVersion.fromString("1.5.22");

    /** Versions that only compatible with each other, and from 1.5.33. */
    private static final Set<IgniteProductVersion> SERVICE_TOP_CALLABLE_VER1;

    /** */
    private final Boolean srvcCompatibilitySysProp;

    /** Time to wait before reassignment retries. */
    private static final long RETRY_TIMEOUT = 1000;

    /** */
    private static final int[] EVTS = {
        EventType.EVT_NODE_JOINED,
        EventType.EVT_NODE_LEFT,
        EventType.EVT_NODE_FAILED,
        DiscoveryCustomEvent.EVT_DISCOVERY_CUSTOM_EVT
    };

    /** */
    private final AtomicReference<ServicesCompatibilityState> compatibilityState;

    /** Local service instances. */
    private final Map<String, Collection<ServiceContextImpl>> locSvcs = new HashMap<>();

    /** Deployment futures. */
    private final ConcurrentMap<String, GridServiceDeploymentFuture> depFuts = new ConcurrentHashMap8<>();

    /** Deployment futures. */
    private final ConcurrentMap<String, GridFutureAdapter<?>> undepFuts = new ConcurrentHashMap8<>();

    /** Pending compute job contexts that waiting for utility cache initialization. */
    private final List<ComputeJobContext> pendingJobCtxs = new ArrayList<>(0);

    /** Deployment executor service. */
    private final ExecutorService depExe;

    /** Busy lock. */
    private final GridSpinBusyLock busyLock = new GridSpinBusyLock();

    /** Thread factory. */
    private ThreadFactory threadFactory = new IgniteThreadFactory(ctx.gridName());

    /** Thread local for service name. */
    private ThreadLocal<String> svcName = new ThreadLocal<>();

    /** Service cache. */
    private IgniteInternalCache<Object, Object> cache;

    /** Topology listener. */
    private GridLocalEventListener topLsnr = new TopologyListener();

    static {
        Set<IgniteProductVersion> versions = new TreeSet<>(new Comparator<IgniteProductVersion>() {
            @Override public int compare(final IgniteProductVersion o1, final IgniteProductVersion o2) {
                return o1.compareToIgnoreTimestamp(o2);
            }
        });

        versions.add(IgniteProductVersion.fromString("1.5.30"));
        versions.add(IgniteProductVersion.fromString("1.5.31"));
        versions.add(IgniteProductVersion.fromString("1.5.32"));
        versions.add(IgniteProductVersion.fromString("1.6.3"));
        versions.add(IgniteProductVersion.fromString("1.6.4"));
        versions.add(IgniteProductVersion.fromString("1.6.5"));
        versions.add(IgniteProductVersion.fromString("1.6.6"));
        versions.add(IgniteProductVersion.fromString("1.6.7"));
        versions.add(IgniteProductVersion.fromString("1.6.8"));
        versions.add(IgniteProductVersion.fromString("1.6.9"));
        versions.add(IgniteProductVersion.fromString("1.6.10"));
        versions.add(IgniteProductVersion.fromString("1.7.0"));
        versions.add(IgniteProductVersion.fromString("1.7.1"));
        versions.add(IgniteProductVersion.fromString("1.7.2"));

        SERVICE_TOP_CALLABLE_VER1 = Collections.unmodifiableSet(versions);
    }

    /**
     * @param ctx Kernal context.
     */
    public GridServiceProcessor(GridKernalContext ctx) {
        super(ctx);

        depExe = Executors.newSingleThreadExecutor(new IgniteThreadFactory(ctx.gridName(), "srvc-deploy"));

        String servicesCompatibilityMode = getString(IGNITE_SERVICES_COMPATIBILITY_MODE);

        srvcCompatibilitySysProp = servicesCompatibilityMode == null ? null : Boolean.valueOf(servicesCompatibilityMode);

        compatibilityState = new AtomicReference<>(
            new ServicesCompatibilityState(srvcCompatibilitySysProp != null ? srvcCompatibilitySysProp : false, false));
    }

    /**
     * @param ctx Context.
     * @throws IgniteCheckedException If failed.
     */
    public void onContinuousProcessorStarted(GridKernalContext ctx) throws IgniteCheckedException {
        if (ctx.clientNode()) {
            assert !ctx.isDaemon();

            ctx.continuous().registerStaticRoutine(
                CU.UTILITY_CACHE_NAME,
                new ServiceEntriesListener(),
                null,
                null);
        }
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteCheckedException {
        ctx.addNodeAttribute(ATTR_SERVICES_COMPATIBILITY_MODE, srvcCompatibilitySysProp);

        if (ctx.isDaemon())
            return;

        IgniteConfiguration cfg = ctx.config();

        DeploymentMode depMode = cfg.getDeploymentMode();

        if (cfg.isPeerClassLoadingEnabled() && (depMode == PRIVATE || depMode == ISOLATED) &&
            !F.isEmpty(cfg.getServiceConfiguration()))
            throw new IgniteCheckedException("Cannot deploy services in PRIVATE or ISOLATED deployment mode: " + depMode);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public void onKernalStart() throws IgniteCheckedException {
        if (ctx.isDaemon())
            return;

        cache = ctx.cache().utilityCache();

        if (!ctx.clientNode())
            ctx.event().addLocalEventListener(topLsnr, EVTS);

        try {
            if (ctx.deploy().enabled())
                ctx.cache().context().deploy().ignoreOwnership(true);

            if (!ctx.clientNode()) {
                assert cache.context().affinityNode();

                cache.context().continuousQueries().executeInternalQuery(new ServiceEntriesListener(),
                    null,
                    true,
                    true,
                    false);
            }
            else {
                assert !ctx.isDaemon();

                ctx.closure().runLocalSafe(new Runnable() {
                    @Override public void run() {
                        try {
                            Iterable<CacheEntryEvent<?, ?>> entries =
                                cache.context().continuousQueries().existingEntries(false, null);

                            onSystemCacheUpdated(entries);
                        }
                        catch (IgniteCheckedException e) {
                            U.error(log, "Failed to load service entries: " + e, e);
                        }
                    }
                });
            }
        }
        finally {
            if (ctx.deploy().enabled())
                ctx.cache().context().deploy().ignoreOwnership(false);
        }

        ServiceConfiguration[] cfgs = ctx.config().getServiceConfiguration();

        if (cfgs != null) {
            Collection<IgniteInternalFuture<?>> futs = new ArrayList<>();

            for (ServiceConfiguration c : ctx.config().getServiceConfiguration())
                futs.add(deploy(c));

            // Await for services to deploy.
            for (IgniteInternalFuture<?> f : futs)
                f.get();
        }

        if (log.isDebugEnabled())
            log.debug("Started service processor.");
    }

    /** {@inheritDoc} */
    @Override public void onKernalStop(boolean cancel) {
        if (ctx.isDaemon())
            return;

        busyLock.block();

        if (!ctx.clientNode())
            ctx.event().removeLocalEventListener(topLsnr);

        Collection<ServiceContextImpl> ctxs = new ArrayList<>();

        synchronized (locSvcs) {
            for (Collection<ServiceContextImpl> ctxs0 : locSvcs.values())
                ctxs.addAll(ctxs0);
        }

        for (ServiceContextImpl ctx : ctxs) {
            ctx.setCancelled(true);

            Service svc = ctx.service();

            if (svc != null)
                svc.cancel(ctx);

            ctx.executor().shutdownNow();
        }

        for (ServiceContextImpl ctx : ctxs) {
            try {
                if (log.isInfoEnabled() && !ctxs.isEmpty())
                    log.info("Shutting down distributed service [name=" + ctx.name() + ", execId8=" +
                        U.id8(ctx.executionId()) + ']');

                ctx.executor().awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
            }
            catch (InterruptedException ignore) {
                Thread.currentThread().interrupt();

                U.error(log, "Got interrupted while waiting for service to shutdown (will continue stopping node): " +
                    ctx.name());
            }
        }

        U.shutdownNow(GridServiceProcessor.class, depExe, log);

        Exception err = new IgniteCheckedException("Operation has been cancelled (node is stopping).");

        cancelFutures(depFuts, err);
        cancelFutures(undepFuts, err);

        if (log.isDebugEnabled())
            log.debug("Stopped service processor.");
    }

    /** {@inheritDoc} */
    @Override public void onDisconnected(IgniteFuture<?> reconnectFut) throws IgniteCheckedException {
        cancelFutures(depFuts, new IgniteClientDisconnectedCheckedException(ctx.cluster().clientReconnectFuture(),
            "Failed to deploy service, client node disconnected."));

        cancelFutures(undepFuts, new IgniteClientDisconnectedCheckedException(ctx.cluster().clientReconnectFuture(),
            "Failed to undeploy service, client node disconnected."));
    }

    /**
     * @param futs Futs.
     * @param err Exception.
     */
    private void cancelFutures(ConcurrentMap<String, ? extends GridFutureAdapter<?>> futs, Exception err) {
        for (Map.Entry<String, ? extends GridFutureAdapter<?>> entry : futs.entrySet()) {
            GridFutureAdapter fut = entry.getValue();

            fut.onDone(err);

            futs.remove(entry.getKey(), fut);
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

    /**
     * @param name Service name.
     * @param svc Service.
     * @return Future.
     */
    public IgniteInternalFuture<?> deployNodeSingleton(ClusterGroup prj, String name, Service svc) {
        return deployMultiple(prj, name, svc, 0, 1);
    }

    /**
     * @param name Service name.
     * @param svc Service.
     * @return Future.
     */
    public IgniteInternalFuture<?> deployClusterSingleton(ClusterGroup prj, String name, Service svc) {
        return deployMultiple(prj, name, svc, 1, 1);
    }

    /**
     * @param name Service name.
     * @param svc Service.
     * @param totalCnt Total count.
     * @param maxPerNodeCnt Max per-node count.
     * @return Future.
     */
    public IgniteInternalFuture<?> deployMultiple(ClusterGroup prj, String name, Service svc, int totalCnt,
        int maxPerNodeCnt) {
        ServiceConfiguration cfg = new ServiceConfiguration();

        cfg.setName(name);
        cfg.setService(svc);
        cfg.setTotalCount(totalCnt);
        cfg.setMaxPerNodeCount(maxPerNodeCnt);
        cfg.setNodeFilter(F.<ClusterNode>alwaysTrue() == prj.predicate() ? null : prj.predicate());

        return deploy(cfg);
    }

    /**
     * @param name Service name.
     * @param svc Service.
     * @param cacheName Cache name.
     * @param  affKey Affinity key.
     * @return Future.
     */
    public IgniteInternalFuture<?> deployKeyAffinitySingleton(String name, Service svc, String cacheName, Object affKey) {
        A.notNull(affKey, "affKey");

        ServiceConfiguration cfg = new ServiceConfiguration();

        cfg.setName(name);
        cfg.setService(svc);
        cfg.setCacheName(cacheName);
        cfg.setAffinityKey(affKey);
        cfg.setTotalCount(1);
        cfg.setMaxPerNodeCount(1);

        return deploy(cfg);
    }

    /**
     * @param cfg Service configuration.
     * @return Future for deployment.
     */
    public IgniteInternalFuture<?> deploy(ServiceConfiguration cfg) {
        A.notNull(cfg, "cfg");

        ServicesCompatibilityState state = markCompatibilityStateAsUsed();

        validate(cfg);

        if (!state.srvcCompatibility) {
            Marshaller marsh = ctx.config().getMarshaller();

            LazyServiceConfiguration cfg0;

            try {
                byte[] srvcBytes = U.marshal(marsh, cfg.getService());

                cfg0 = new LazyServiceConfiguration(cfg, srvcBytes);
            }
            catch (IgniteCheckedException e) {
                U.error(log, "Failed to marshal service with configured marshaller [srvc=" + cfg.getService()
                    + ", marsh=" + marsh + "]", e);

                return new GridFinishedFuture<>(e);
            }

            cfg = cfg0;
        }

        GridServiceDeploymentFuture fut = new GridServiceDeploymentFuture(cfg);

        GridServiceDeploymentFuture old = depFuts.putIfAbsent(cfg.getName(), fut);

        if (old != null) {
            if (!old.configuration().equalsIgnoreNodeFilter(cfg)) {
                fut.onDone(new IgniteCheckedException("Failed to deploy service (service already exists with " +
                    "different configuration) [deployed=" + old.configuration() + ", new=" + cfg + ']'));

                return fut;
            }

            return old;
        }

        if (ctx.clientDisconnected()) {
            fut.onDone(new IgniteClientDisconnectedCheckedException(ctx.cluster().clientReconnectFuture(),
                "Failed to deploy service, client node disconnected."));

            depFuts.remove(cfg.getName(), fut);
        }

        while (true) {
            try {
                GridServiceDeploymentKey key = new GridServiceDeploymentKey(cfg.getName());

                if (ctx.deploy().enabled())
                    ctx.cache().context().deploy().ignoreOwnership(true);

                try {
                    GridServiceDeployment dep = (GridServiceDeployment)cache.getAndPutIfAbsent(key,
                        new GridServiceDeployment(ctx.localNodeId(), cfg));

                    if (dep != null) {
                        if (!dep.configuration().equalsIgnoreNodeFilter(cfg)) {
                            // Remove future from local map.
                            depFuts.remove(cfg.getName(), fut);

                            fut.onDone(new IgniteCheckedException("Failed to deploy service (service already exists with " +
                                "different configuration) [deployed=" + dep.configuration() + ", new=" + cfg + ']'));
                        }
                        else {
                            Iterator<Cache.Entry<Object, Object>> it = serviceEntries(
                                ServiceAssignmentsPredicate.INSTANCE);

                            while (it.hasNext()) {
                                Cache.Entry<Object, Object> e = it.next();

                                if (e.getKey() instanceof GridServiceAssignmentsKey) {
                                    GridServiceAssignments assigns = (GridServiceAssignments)e.getValue();

                                    if (assigns.name().equals(cfg.getName())) {
                                        // Remove future from local map.
                                        depFuts.remove(cfg.getName(), fut);

                                        fut.onDone();

                                        break;
                                    }
                                }
                            }

                            if (!dep.configuration().equalsIgnoreNodeFilter(cfg))
                                U.warn(log, "Service already deployed with different configuration (will ignore) " +
                                    "[deployed=" + dep.configuration() + ", new=" + cfg + ']');
                        }
                    }
                }
                finally {
                    if (ctx.deploy().enabled())
                        ctx.cache().context().deploy().ignoreOwnership(false);
                }

                return fut;
            }
            catch (ClusterTopologyCheckedException e) {
                if (log.isDebugEnabled())
                    log.debug("Topology changed while deploying service (will retry): " + e.getMessage());
            }
            catch (IgniteCheckedException e) {
                if (e.hasCause(ClusterTopologyCheckedException.class)) {
                    if (log.isDebugEnabled())
                        log.debug("Topology changed while deploying service (will retry): " + e.getMessage());

                    continue;
                }

                U.error(log, "Failed to deploy service: " + cfg.getName(), e);

                return new GridFinishedFuture<>(e);
            }
        }
    }

    /**
     * @return Compatibility state.
     */
    private ServicesCompatibilityState markCompatibilityStateAsUsed() {
        while (true) {
            ServicesCompatibilityState state = compatibilityState.get();

            if (state.used)
                return state;

            ServicesCompatibilityState newState = new ServicesCompatibilityState(state.srvcCompatibility, true);

            if (compatibilityState.compareAndSet(state, newState))
                return newState;
        }
    }

    /**
     * @param name Service name.
     * @return Future.
     */
    public IgniteInternalFuture<?> cancel(String name) {
        while (true) {
            try {
                GridFutureAdapter<?> fut = new GridFutureAdapter<>();

                GridFutureAdapter<?> old;

                if ((old = undepFuts.putIfAbsent(name, fut)) != null)
                    fut = old;
                else {
                    GridServiceDeploymentKey key = new GridServiceDeploymentKey(name);

                    if (cache.getAndRemove(key) == null) {
                        // Remove future from local map if service was not deployed.
                        undepFuts.remove(name);

                        fut.onDone();
                    }
                }

                return fut;
            }
            catch (ClusterTopologyCheckedException e) {
                if (log.isDebugEnabled())
                    log.debug("Topology changed while deploying service (will retry): " + e.getMessage());
            }
            catch (IgniteCheckedException e) {
                log.error("Failed to undeploy service: " + name, e);

                return new GridFinishedFuture<>(e);
            }
        }
    }

    /**
     * @return Future.
     */
    @SuppressWarnings("unchecked")
    public IgniteInternalFuture<?> cancelAll() {
        Iterator<Cache.Entry<Object, Object>> it = serviceEntries(ServiceDeploymentPredicate.INSTANCE);

        GridCompoundFuture res = null;

        while (it.hasNext()) {
            Cache.Entry<Object, Object> e = it.next();

            if (!(e.getKey() instanceof GridServiceDeploymentKey))
                continue;

            GridServiceDeployment dep = (GridServiceDeployment)e.getValue();

            if (res == null)
                res = new GridCompoundFuture<>();

            // Cancel each service separately.
            res.add(cancel(dep.configuration().getName()));
        }

        if (res != null) {
            res.markInitialized();

            return res;
        }
        else
            return new GridFinishedFuture<>();
    }

    /**
     * @param name Service name.
     * @param timeout If greater than 0 limits task execution time. Cannot be negative.
     * @return Service topology.
     */
    public Map<UUID, Integer> serviceTopology(String name, long timeout) throws IgniteCheckedException {
        ClusterNode node = cache.affinity().mapKeyToNode(name);

        if (node.version().compareTo(ServiceTopologyCallable.SINCE_VER) >= 0) {
            final ServiceTopologyCallable call = new ServiceTopologyCallable(name);

            call.serialize = SERVICE_TOP_CALLABLE_VER1.contains(node.version());

            return ctx.closure().callAsyncNoFailover(
                GridClosureCallMode.BROADCAST,
                call,
                Collections.singletonList(node),
                false,
                timeout
            ).get();
        }
        else
            return serviceTopology(cache, name);
    }

    /**
     * @param cache Utility cache.
     * @param svcName Service name.
     * @return Service topology.
     * @throws IgniteCheckedException In case of error.
     */
    private static Map<UUID, Integer> serviceTopology(IgniteInternalCache<Object, Object> cache, String svcName)
        throws IgniteCheckedException {
        GridServiceAssignments val = (GridServiceAssignments)cache.get(new GridServiceAssignmentsKey(svcName));

        return val != null ? val.assigns() : null;
    }

    /**
     * @return Collection of service descriptors.
     */
    public Collection<ServiceDescriptor> serviceDescriptors() {
        Collection<ServiceDescriptor> descs = new ArrayList<>();

        Iterator<Cache.Entry<Object, Object>> it = serviceEntries(ServiceDeploymentPredicate.INSTANCE);

        while (it.hasNext()) {
            Cache.Entry<Object, Object> e = it.next();

            if (!(e.getKey() instanceof GridServiceDeploymentKey))
                continue;

            GridServiceDeployment dep = (GridServiceDeployment)e.getValue();

            ServiceDescriptorImpl desc = new ServiceDescriptorImpl(dep);

            try {
                GridServiceAssignments assigns = (GridServiceAssignments)cache.getForcePrimary(
                    new GridServiceAssignmentsKey(dep.configuration().getName()));

                if (assigns != null) {
                    desc.topologySnapshot(assigns.assigns());

                    descs.add(desc);
                }
            }
            catch (IgniteCheckedException ex) {
                log.error("Failed to get assignments from replicated cache for service: " +
                    dep.configuration().getName(), ex);
            }
        }

        return descs;
    }

    /**
     * @param name Service name.
     * @param <T> Service type.
     * @return Service by specified service name.
     */
    @SuppressWarnings("unchecked")
    public <T> T service(String name) {
        Collection<ServiceContextImpl> ctxs;

        synchronized (locSvcs) {
            ctxs = locSvcs.get(name);
        }

        if (ctxs == null)
            return null;

        synchronized (ctxs) {
            if (ctxs.isEmpty())
                return null;

            for (ServiceContextImpl ctx : ctxs) {
                Service svc = ctx.service();

                if (svc != null)
                    return (T)svc;
            }

            return null;
        }
    }

    /**
     * @param name Service name.
     * @return Service by specified service name.
     */
    public ServiceContextImpl serviceContext(String name) {
        Collection<ServiceContextImpl> ctxs;

        synchronized (locSvcs) {
            ctxs = locSvcs.get(name);
        }

        if (ctxs == null)
            return null;

        synchronized (ctxs) {
            if (ctxs.isEmpty())
                return null;

            for (ServiceContextImpl ctx : ctxs) {
                if (ctx.service() != null)
                    return ctx;
            }

            return null;
        }
    }

    /**
     * @param prj Grid projection.
     * @param name Service name.
     * @param svcItf Service class.
     * @param sticky Whether multi-node request should be done.
     * @param timeout If greater than 0 limits service acquire time. Cannot be negative.
     * @param <T> Service interface type.
     * @return The proxy of a service by its name and class.
     * @throws IgniteException If failed to create proxy.
     */
    @SuppressWarnings("unchecked")
    public <T> T serviceProxy(ClusterGroup prj, String name, Class<? super T> svcItf, boolean sticky, long timeout)
        throws IgniteException {
        if (hasLocalNode(prj)) {
            ServiceContextImpl ctx = serviceContext(name);

            if (ctx != null) {
                Service svc = ctx.service();

                if (svc != null) {
                    if (!svcItf.isAssignableFrom(svc.getClass()))
                        throw new IgniteException("Service does not implement specified interface [svcItf=" +
                            svcItf.getName() + ", svcCls=" + svc.getClass().getName() + ']');

                    return (T)svc;
                }
            }
        }

        return new GridServiceProxy<T>(prj, name, svcItf, sticky, timeout, ctx).proxy();
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

    /**
     * @param name Service name.
     * @param <T> Service type.
     * @return Services by specified service name.
     */
    @SuppressWarnings("unchecked")
    public <T> Collection<T> services(String name) {
        Collection<ServiceContextImpl> ctxs;

        synchronized (locSvcs) {
             ctxs = locSvcs.get(name);
        }

        if (ctxs == null)
            return null;

        synchronized (ctxs) {
            Collection<T> res = new ArrayList<>(ctxs.size());

            for (ServiceContextImpl ctx : ctxs) {
                Service svc = ctx.service();

                if (svc != null)
                    res.add((T)svc);
            }

            return res;
        }
    }

    /**
     * Reassigns service to nodes.
     *
     * @param dep Service deployment.
     * @param topVer Topology version.
     * @throws IgniteCheckedException If failed.
     */
    private void reassign(GridServiceDeployment dep, AffinityTopologyVersion topVer) throws IgniteCheckedException {
        ServiceConfiguration cfg = dep.configuration();

        Object nodeFilter = cfg.getNodeFilter();

        if (nodeFilter != null)
            ctx.resource().injectGeneric(nodeFilter);

        int totalCnt = cfg.getTotalCount();
        int maxPerNodeCnt = cfg.getMaxPerNodeCount();
        String cacheName = cfg.getCacheName();
        Object affKey = cfg.getAffinityKey();

        while (true) {
            GridServiceAssignments assigns = new GridServiceAssignments(cfg, dep.nodeId(), topVer.topologyVersion());

             Collection<ClusterNode> nodes;

             // Call node filter outside of transaction.
            if (affKey == null) {
                nodes = ctx.discovery().nodes(topVer);

                if (assigns.nodeFilter() != null) {
                    Collection<ClusterNode> nodes0 = new ArrayList<>();

                    for (ClusterNode node : nodes) {
                        if (assigns.nodeFilter().apply(node))
                            nodes0.add(node);
                    }

                    nodes = nodes0;
                }
            }
            else
                nodes = null;

            try (IgniteInternalTx tx = cache.txStartEx(PESSIMISTIC, REPEATABLE_READ)) {
                GridServiceAssignmentsKey key = new GridServiceAssignmentsKey(cfg.getName());

                GridServiceAssignments oldAssigns = (GridServiceAssignments)cache.get(key);

                Map<UUID, Integer> cnts = new HashMap<>();

                if (affKey != null) {
                    ClusterNode n = ctx.affinity().mapKeyToNode(cacheName, affKey, topVer);

                    if (n != null) {
                        int cnt = maxPerNodeCnt == 0 ? totalCnt == 0 ? 1 : totalCnt : maxPerNodeCnt;

                        cnts.put(n.id(), cnt);
                    }
                }
                else {
                    if (!nodes.isEmpty()) {
                        int size = nodes.size();

                        int perNodeCnt = totalCnt != 0 ? totalCnt / size : maxPerNodeCnt;
                        int remainder = totalCnt != 0 ? totalCnt % size : 0;

                        if (perNodeCnt > maxPerNodeCnt && maxPerNodeCnt != 0) {
                            perNodeCnt = maxPerNodeCnt;
                            remainder = 0;
                        }

                        for (ClusterNode n : nodes)
                            cnts.put(n.id(), perNodeCnt);

                        assert perNodeCnt >= 0;
                        assert remainder >= 0;

                        if (remainder > 0) {
                            int cnt = perNodeCnt + 1;

                            if (oldAssigns != null) {
                                Collection<UUID> used = new HashSet<>();

                                // Avoid redundant moving of services.
                                for (Map.Entry<UUID, Integer> e : oldAssigns.assigns().entrySet()) {
                                    // Do not assign services to left nodes.
                                    if (ctx.discovery().node(e.getKey()) == null)
                                        continue;

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
                                    Collections.shuffle(entries);

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
                                Collections.shuffle(entries);

                                for (Map.Entry<UUID, Integer> e : entries) {
                                    e.setValue(e.getValue() + 1);

                                    if (--remainder == 0)
                                        break;
                                }
                            }
                        }
                    }
                }

                assigns.assigns(cnts);

                cache.put(key, assigns);

                tx.commit();

                break;
            }
            catch (ClusterTopologyCheckedException e) {
                if (log.isDebugEnabled())
                    log.debug("Topology changed while reassigning (will retry): " + e.getMessage());

                U.sleep(10);
            }
        }
    }

    /**
     * Redeploys local services based on assignments.
     *
     * @param assigns Assignments.
     */
    private void redeploy(GridServiceAssignments assigns) {
        String svcName = assigns.name();

        Integer assignCnt = assigns.assigns().get(ctx.localNodeId());

        if (assignCnt == null)
            assignCnt = 0;

        Collection<ServiceContextImpl> ctxs;

        synchronized (locSvcs) {
            ctxs = locSvcs.get(svcName);

            if (ctxs == null)
                locSvcs.put(svcName, ctxs = new ArrayList<>());
        }

        Collection<ServiceContextImpl> toInit = new ArrayList<>();

        synchronized (ctxs) {
            if (ctxs.size() > assignCnt) {
                int cancelCnt = ctxs.size() - assignCnt;

                cancel(ctxs, cancelCnt);
            }
            else if (ctxs.size() < assignCnt) {
                int createCnt = assignCnt - ctxs.size();

                for (int i = 0; i < createCnt; i++) {
                    ServiceContextImpl svcCtx = new ServiceContextImpl(assigns.name(),
                        UUID.randomUUID(),
                        assigns.cacheName(),
                        assigns.affinityKey(),
                        Executors.newSingleThreadExecutor(threadFactory));

                    ctxs.add(svcCtx);

                    toInit.add(svcCtx);
                }
            }
        }

        for (final ServiceContextImpl svcCtx : toInit) {
            final Service svc;

            try {
                svc = copyAndInject(assigns.configuration());

                // Initialize service.
                svc.init(svcCtx);

                svcCtx.service(svc);
            }
            catch (Throwable e) {
                U.error(log, "Failed to initialize service (service will not be deployed): " + assigns.name(), e);

                synchronized (ctxs) {
                    ctxs.removeAll(toInit);
                }

                if (e instanceof Error)
                    throw (Error)e;

                if (e instanceof RuntimeException)
                    throw (RuntimeException)e;

                return;
            }

            if (log.isInfoEnabled())
                log.info("Starting service instance [name=" + svcCtx.name() + ", execId=" +
                    svcCtx.executionId() + ']');

            // Start service in its own thread.
            final ExecutorService exe = svcCtx.executor();

            exe.execute(new Runnable() {
                @Override public void run() {
                    try {
                        svc.execute(svcCtx);
                    }
                    catch (InterruptedException | IgniteInterruptedCheckedException ignore) {
                        if (log.isDebugEnabled())
                            log.debug("Service thread was interrupted [name=" + svcCtx.name() + ", execId=" +
                                svcCtx.executionId() + ']');
                    }
                    catch (IgniteException e) {
                        if (e.hasCause(InterruptedException.class) ||
                            e.hasCause(IgniteInterruptedCheckedException.class)) {
                            if (log.isDebugEnabled())
                                log.debug("Service thread was interrupted [name=" + svcCtx.name() +
                                    ", execId=" + svcCtx.executionId() + ']');
                        }
                        else {
                            U.error(log, "Service execution stopped with error [name=" + svcCtx.name() +
                                ", execId=" + svcCtx.executionId() + ']', e);
                        }
                    }
                    catch (Throwable e) {
                        log.error("Service execution stopped with error [name=" + svcCtx.name() +
                            ", execId=" + svcCtx.executionId() + ']', e);

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
        Marshaller m = ctx.config().getMarshaller();

        if (cfg instanceof LazyServiceConfiguration) {
            byte[] bytes = ((LazyServiceConfiguration)cfg).serviceBytes();

            Service srvc = U.unmarshal(m, bytes, U.resolveClassLoader(null, ctx.config()));

            ctx.resource().inject(srvc);

            return srvc;
        }
        else {
            Service svc = cfg.getService();

            try {
                byte[] bytes = U.marshal(m, svc);

                Service cp = U.unmarshal(m, bytes, U.resolveClassLoader(svc.getClass().getClassLoader(), ctx.config()));

                ctx.resource().inject(cp);

                return cp;
            }
            catch (IgniteCheckedException e) {
                U.error(log, "Failed to copy service (will reuse same instance): " + svc.getClass(), e);

                return svc;
            }
        }
    }

    /**
     * @param ctxs Contexts to cancel.
     * @param cancelCnt Number of contexts to cancel.
     */
    private void cancel(Iterable<ServiceContextImpl> ctxs, int cancelCnt) {
        for (Iterator<ServiceContextImpl> it = ctxs.iterator(); it.hasNext();) {
            ServiceContextImpl svcCtx = it.next();

            // Flip cancelled flag.
            svcCtx.setCancelled(true);

            // Notify service about cancellation.
            Service svc = svcCtx.service();

            if (svc != null) {
                try {
                    svc.cancel(svcCtx);
                }
                catch (Throwable e) {
                    log.error("Failed to cancel service (ignoring) [name=" + svcCtx.name() +
                        ", execId=" + svcCtx.executionId() + ']', e);

                    if (e instanceof Error)
                        throw e;
                }
                finally {
                    try {
                        ctx.resource().cleanup(svc);
                    }
                    catch (IgniteCheckedException e) {
                        U.error(log, "Failed to clean up service (will ignore): " + svcCtx.name(), e);
                    }
                }
            }

            // Close out executor thread for the service.
            // This will cause the thread to be interrupted.
            svcCtx.executor().shutdownNow();

            it.remove();

            if (log.isInfoEnabled())
                log.info("Cancelled service instance [name=" + svcCtx.name() + ", execId=" +
                    svcCtx.executionId() + ']');

            if (--cancelCnt == 0)
                break;
        }
    }

    /**
     * @param p Entry predicate used to execute query from client node.
     * @return Service deployment entries.
     */
    @SuppressWarnings("unchecked")
    private Iterator<Cache.Entry<Object, Object>> serviceEntries(IgniteBiPredicate<Object, Object> p) {
        try {
            if (!cache.context().affinityNode()) {
                ClusterNode oldestSrvNode =
                    ctx.discovery().oldestAliveCacheServerNode(AffinityTopologyVersion.NONE);

                if (oldestSrvNode == null)
                    return new GridEmptyIterator<>();

                GridCacheQueryManager qryMgr = cache.context().queries();

                CacheQuery<Map.Entry<Object, Object>> qry = qryMgr.createScanQuery(p, null, false);

                qry.keepAll(false);

                qry.projection(ctx.cluster().get().forNode(oldestSrvNode));

                GridCloseableIterator<Map.Entry<Object, Object>> iter = qry.executeScanQuery();

                return cache.context().itHolder().iterator(iter,
                    new CacheIteratorConverter<Cache.Entry<Object, Object>, Map.Entry<Object,Object>>() {
                        @Override protected Cache.Entry<Object, Object> convert(Map.Entry<Object, Object> e) {
                            return new CacheEntryImpl<>(e.getKey(), e.getValue());
                        }

                        @Override protected void remove(Cache.Entry<Object, Object> item) {
                            throw new UnsupportedOperationException();
                        }
                    });
            }
            else
                return cache.entrySetx().iterator();
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /** {@inheritDoc} */
    @Nullable @Override public IgniteNodeValidationResult validateNode(ClusterNode node) {
        IgniteNodeValidationResult res = super.validateNode(node);

        if (res != null)
            return res;

        boolean rmtNodeIsOld = node.version().compareToIgnoreTimestamp(LAZY_SERVICES_CFG_SINCE) < 0;

        if (!rmtNodeIsOld)
            return null;

        while (true) {
            ServicesCompatibilityState state = compatibilityState.get();

            if (state.srvcCompatibility)
                return null;

            // Remote node is old and services are in not compatible mode.
            if (!state.used) {
                if (!compatibilityState.compareAndSet(state, new ServicesCompatibilityState(true, false)))
                    continue;

                return null;
            }

            ClusterNode locNode = ctx.discovery().localNode();

            return new IgniteNodeValidationResult(node.id(), "Local node uses IgniteServices and works in not " +
                "compatible mode with old nodes (" + IGNITE_SERVICES_COMPATIBILITY_MODE + " system property can be " +
                "set explicitly) [locNodeId=" + locNode.id() + ", rmtNodeId=" + node.id() + "]",
                "Remote node uses IgniteServices and works in not compatible mode with old nodes " +
                    IGNITE_SERVICES_COMPATIBILITY_MODE + " system property can be set explicitly" +
                    "[locNodeId=" + node.id() + ", rmtNodeId=" + locNode.id() + "]");
        }
    }

    /**
     * @param nodes Remote nodes.
     */
    public void initCompatibilityMode(Collection<ClusterNode> nodes) {
        boolean mode;

        if (srvcCompatibilitySysProp == null) {
            boolean clusterHasOldNode = false;

            for (ClusterNode n : nodes) {
                if (n.version().compareToIgnoreTimestamp(LAZY_SERVICES_CFG_SINCE) < 0) {
                    clusterHasOldNode = true;

                    break;
                }
            }

            mode = clusterHasOldNode;
        }
        else
            mode = srvcCompatibilitySysProp;

        while (true) {
            ServicesCompatibilityState state = compatibilityState.get();

            if (compatibilityState.compareAndSet(state, new ServicesCompatibilityState(mode, state.used)))
                return;
        }
    }

    /**
     * Called right after utility cache is started and ready for the usage.
     */
    public void onUtilityCacheStarted() {
        synchronized (pendingJobCtxs) {
            if (pendingJobCtxs.size() == 0)
                return;

            Iterator<ComputeJobContext> iter = pendingJobCtxs.iterator();

            while (iter.hasNext()) {
                iter.next().callcc();
                iter.remove();
            }
        }
    }

    /**
     * Service deployment listener.
     */
    @SuppressWarnings("unchecked")
    private class ServiceEntriesListener implements CacheEntryUpdatedListener<Object, Object> {
        /** {@inheritDoc} */
        @Override public void onUpdated(final Iterable<CacheEntryEvent<?, ?>> deps) {
            if (!busyLock.enterBusy())
                return;

            try {
                depExe.execute(new BusyRunnable() {
                    @Override public void run0() {
                        onSystemCacheUpdated(deps);
                    }
                });
            }
            finally {
                busyLock.leaveBusy();
            }
        }
    }

    /**
     * @param evts Update events.
     */
    private void onSystemCacheUpdated(final Iterable<CacheEntryEvent<?, ?>> evts) {
        boolean firstTime = true;

        for (CacheEntryEvent<?, ?> e : evts) {
            if (e.getKey() instanceof GridServiceDeploymentKey) {
                if (firstTime) {
                    markCompatibilityStateAsUsed();

                    firstTime = false;
                }

                processDeployment((CacheEntryEvent)e);
            }
            else if (e.getKey() instanceof GridServiceAssignmentsKey) {
                if (firstTime) {
                    markCompatibilityStateAsUsed();

                    firstTime = false;
                }

                processAssignment((CacheEntryEvent)e);
            }
        }
    }

    /**
     * @param e Entry.
     */
    private void processDeployment(CacheEntryEvent<GridServiceDeploymentKey, GridServiceDeployment> e) {
        GridServiceDeployment dep;

        try {
            dep = e.getValue();
        }
        catch (IgniteException ex) {
            if (X.hasCause(ex, ClassNotFoundException.class))
                return;
            else
                throw ex;
        }

        if (dep != null) {
            svcName.set(dep.configuration().getName());

            // Ignore other utility cache events.
            AffinityTopologyVersion topVer = ctx.discovery().topologyVersionEx();

            ClusterNode oldest = U.oldest(ctx.discovery().nodes(topVer), null);

            if (oldest.isLocal())
                onDeployment(dep, topVer);
        }
        // Handle undeployment.
        else {
            String name = e.getKey().name();

            svcName.set(name);

            Collection<ServiceContextImpl> ctxs;

            synchronized (locSvcs) {
                ctxs = locSvcs.remove(name);
            }

            if (ctxs != null) {
                synchronized (ctxs) {
                    cancel(ctxs, ctxs.size());
                }
            }

            // Finish deployment futures if undeployment happened.
            GridFutureAdapter<?> fut = depFuts.remove(name);

            if (fut != null)
                fut.onDone();

            // Complete undeployment future.
            fut = undepFuts.remove(name);

            if (fut != null)
                fut.onDone();

            GridServiceAssignmentsKey key = new GridServiceAssignmentsKey(name);

            // Remove assignment on primary node in case of undeploy.
            if (cache.cache().affinity().isPrimary(ctx.discovery().localNode(), key)) {
                try {
                    cache.getAndRemove(key);
                }
                catch (IgniteCheckedException ex) {
                    U.error(log, "Failed to remove assignments for undeployed service: " + name, ex);
                }
            }
        }
    }

        /**
         * Deployment callback.
         *
         * @param dep Service deployment.
         * @param topVer Topology version.
         */
        private void onDeployment(final GridServiceDeployment dep, final AffinityTopologyVersion topVer) {
            // Retry forever.
            try {
                AffinityTopologyVersion newTopVer = ctx.discovery().topologyVersionEx();

                // If topology version changed, reassignment will happen from topology event.
                if (newTopVer.equals(topVer))
                    reassign(dep, topVer);
            }
            catch (IgniteCheckedException e) {
                if (!(e instanceof ClusterTopologyCheckedException))
                    log.error("Failed to do service reassignment (will retry): " + dep.configuration().getName(), e);

                AffinityTopologyVersion newTopVer = ctx.discovery().topologyVersionEx();

                if (!newTopVer.equals(topVer)) {
                    assert newTopVer.compareTo(topVer) > 0;

                    // Reassignment will happen from topology event.
                    return;
                }

                ctx.timeout().addTimeoutObject(new GridTimeoutObject() {
                    private IgniteUuid id = IgniteUuid.randomUuid();

                    private long start = System.currentTimeMillis();

                    @Override public IgniteUuid timeoutId() {
                        return id;
                    }

                    @Override public long endTime() {
                        return start + RETRY_TIMEOUT;
                    }

                    @Override public void onTimeout() {
                        if (!busyLock.enterBusy())
                            return;

                        try {
                            // Try again.
                            onDeployment(dep, topVer);
                        }
                        finally {
                            busyLock.leaveBusy();
                        }
                    }
                });
        }
    }

    /**
     * Topology listener.
     */
    private class TopologyListener implements GridLocalEventListener {
        /** {@inheritDoc} */
        @Override public void onEvent(Event evt) {
            if (!busyLock.enterBusy())
                return;

            try {
                final AffinityTopologyVersion topVer;

                if (evt instanceof DiscoveryCustomEvent) {
                    DiscoveryCustomMessage msg = ((DiscoveryCustomEvent)evt).customMessage();

                    topVer = ((DiscoveryCustomEvent)evt).affinityTopologyVersion();

                    if (msg instanceof CacheAffinityChangeMessage) {
                        if (!((CacheAffinityChangeMessage)msg).exchangeNeeded())
                            return;
                    }
                }
                else
                    topVer = new AffinityTopologyVersion(((DiscoveryEvent)evt).topologyVersion(), 0);

                depExe.execute(new BusyRunnable() {
                    @Override public void run0() {
                        ClusterNode oldest = ctx.discovery().oldestAliveCacheServerNode(topVer);

                        if (oldest != null && oldest.isLocal()) {
                            final Collection<GridServiceDeployment> retries = new ConcurrentLinkedQueue<>();

                            if (ctx.deploy().enabled())
                                ctx.cache().context().deploy().ignoreOwnership(true);

                            try {
                                Iterator<Cache.Entry<Object, Object>> it = serviceEntries(
                                    ServiceDeploymentPredicate.INSTANCE);

                                boolean firstTime = true;

                                while (it.hasNext()) {
                                    Cache.Entry<Object, Object> e = it.next();

                                    if (!(e.getKey() instanceof GridServiceDeploymentKey))
                                        continue;

                                    if (firstTime) {
                                        markCompatibilityStateAsUsed();

                                        firstTime = false;
                                    }

                                    GridServiceDeployment dep = (GridServiceDeployment)e.getValue();

                                    try {
                                        svcName.set(dep.configuration().getName());

                                        ctx.cache().internalCache(UTILITY_CACHE_NAME).context().affinity().
                                            affinityReadyFuture(topVer).get();

                                        reassign(dep, topVer);
                                    }
                                    catch (IgniteCheckedException ex) {
                                        if (!(e instanceof ClusterTopologyCheckedException))
                                            LT.error(log, ex, "Failed to do service reassignment (will retry): " +
                                                dep.configuration().getName());

                                        retries.add(dep);
                                    }
                                }
                            }
                            finally {
                                if (ctx.deploy().enabled())
                                    ctx.cache().context().deploy().ignoreOwnership(false);
                            }

                            if (!retries.isEmpty())
                                onReassignmentFailed(topVer, retries);
                        }

                        // Clean up zombie assignments.
                        for (Cache.Entry<Object, Object> e :
                            cache.entrySetx(CU.cachePrimary(ctx.grid().affinity(cache.name()), ctx.grid().localNode()))) {
                            if (!(e.getKey() instanceof GridServiceAssignmentsKey))
                                continue;

                            String name = ((GridServiceAssignmentsKey)e.getKey()).name();

                            try {
                                if (cache.get(new GridServiceDeploymentKey(name)) == null) {
                                    if (log.isDebugEnabled())
                                        log.debug("Removed zombie assignments: " + e.getValue());

                                    cache.getAndRemove(e.getKey());
                                }
                            }
                            catch (IgniteCheckedException ex) {
                                U.error(log, "Failed to clean up zombie assignments for service: " + name, ex);
                            }
                        }
                    }
                });
            }
            finally {
                busyLock.leaveBusy();
            }
        }

        /**
         * Handler for reassignment failures.
         *
         * @param topVer Topology version.
         * @param retries Retries.
         */
        private void onReassignmentFailed(final AffinityTopologyVersion topVer,
            final Collection<GridServiceDeployment> retries) {
            if (!busyLock.enterBusy())
                return;

            try {
                // If topology changed again, let next event handle it.
                if (ctx.discovery().topologyVersionEx().equals(topVer))
                    return;

                for (Iterator<GridServiceDeployment> it = retries.iterator(); it.hasNext(); ) {
                    GridServiceDeployment dep = it.next();

                    try {
                        svcName.set(dep.configuration().getName());

                        reassign(dep, topVer);

                        it.remove();
                    }
                    catch (IgniteCheckedException e) {
                        if (!(e instanceof ClusterTopologyCheckedException))
                            LT.error(log, e, "Failed to do service reassignment (will retry): " +
                                dep.configuration().getName());
                    }
                }

                if (!retries.isEmpty()) {
                    ctx.timeout().addTimeoutObject(new GridTimeoutObject() {
                        private IgniteUuid id = IgniteUuid.randomUuid();

                        private long start = System.currentTimeMillis();

                        @Override public IgniteUuid timeoutId() {
                            return id;
                        }

                        @Override public long endTime() {
                            return start + RETRY_TIMEOUT;
                        }

                        @Override public void onTimeout() {
                            onReassignmentFailed(topVer, retries);
                        }
                    });
                }
            }
            finally {
                busyLock.leaveBusy();
            }
        }
    }

    /**
     * @param e Entry.
     */
    private void processAssignment(CacheEntryEvent<GridServiceAssignmentsKey, GridServiceAssignments> e) {
        GridServiceAssignments assigns;

        try {
            assigns = e.getValue();
        }
        catch (IgniteException ex) {
            if (X.hasCause(ex, ClassNotFoundException.class))
                return;
            else
                throw ex;
        }

        if (assigns != null) {
            svcName.set(assigns.name());

            Throwable t = null;

            try {
                redeploy(assigns);
            }
            catch (Error | RuntimeException th) {
                t = th;
            }

            GridServiceDeploymentFuture fut = depFuts.get(assigns.name());

            if (fut != null && fut.configuration().equalsIgnoreNodeFilter(assigns.configuration())) {
                depFuts.remove(assigns.name(), fut);

                // Complete deployment futures once the assignments have been stored in cache.
                fut.onDone(null, t);
            }
        }
        // Handle undeployment.
        else {
            String name = e.getKey().name();

            svcName.set(name);

            Collection<ServiceContextImpl> ctxs;

            synchronized (locSvcs) {
                ctxs = locSvcs.remove(name);
            }

            if (ctxs != null) {
                synchronized (ctxs) {
                    cancel(ctxs, ctxs.size());
                }
            }
        }
    }

    /**
     *
     */
    private abstract class BusyRunnable implements Runnable {
        /** {@inheritDoc} */
        @Override public void run() {
            if (!busyLock.enterBusy())
                return;

            svcName.set(null);

            try {
                run0();
            }
            catch (Throwable t) {
                log.error("Error when executing service: " + svcName.get(), t);

                if (t instanceof Error)
                    throw t;
            }
            finally {
                busyLock.leaveBusy();

                svcName.set(null);
            }
        }

        /**
         * Abstract run method protected by busy lock.
         */
        public abstract void run0();
    }

    /**
     *
     */
    static class ServiceDeploymentPredicate implements IgniteBiPredicate<Object, Object> {
        /** */
        static final ServiceDeploymentPredicate INSTANCE = new ServiceDeploymentPredicate();

        /** */
        private static final long serialVersionUID = 0L;

        /** {@inheritDoc} */
        @Override public boolean apply(Object key, Object val) {
            return key instanceof GridServiceDeploymentKey;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(ServiceDeploymentPredicate.class, this);
        }
    }

    /**
     *
     */
    static class ServiceAssignmentsPredicate implements IgniteBiPredicate<Object, Object> {
        /** */
        static final ServiceAssignmentsPredicate INSTANCE = new ServiceAssignmentsPredicate();

        /** */
        private static final long serialVersionUID = 0L;

        /** {@inheritDoc} */
        @Override public boolean apply(Object key, Object val) {
            return key instanceof GridServiceAssignmentsKey;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(ServiceAssignmentsPredicate.class, this);
        }
    }

    /**
     */
    @GridInternal
    @SerializableTransient(methodName = "serializableTransient")
    private static class ServiceTopologyCallable implements IgniteCallable<Map<UUID, Integer>> {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private static final IgniteProductVersion SINCE_VER = IgniteProductVersion.fromString("1.5.7");

        /** */
        private static final String[] SER_FIELDS = {"waitedCacheInit", "jCtx", "log"};

        /** */
        private final String svcName;

        /** */
        private transient boolean waitedCacheInit;

        /** */
        @IgniteInstanceResource
        private IgniteEx ignite;

        /** */
        @JobContextResource
        private transient ComputeJobContext jCtx;

        /** */
        @LoggerResource
        private transient IgniteLogger log;

        /** */
        transient boolean serialize;

        /**
         * @param svcName Service name.
         */
        public ServiceTopologyCallable(String svcName) {
            this.svcName = svcName;
        }

        /** {@inheritDoc} */
        @Override public Map<UUID, Integer> call() throws Exception {
            IgniteInternalCache<Object, Object> cache = ignite.context().cache().utilityCache();

            if (cache == null) {
                List<ComputeJobContext> pendingCtxs = ignite.context().service().pendingJobCtxs;

                synchronized (pendingCtxs) {
                    // Double check cache reference after lock acqusition.
                    cache = ignite.context().cache().utilityCache();

                    if (cache == null) {
                        if (!waitedCacheInit) {
                            log.debug("Utility cache hasn't been initialized yet. Waiting.");

                            // waiting for a minute for cache initialization.
                            jCtx.holdcc(60 * 1000);

                            pendingCtxs.add(jCtx);

                            waitedCacheInit = true;

                            return null;
                        }
                        else {
                            log.error("Failed to gather service topology. Utility " +
                                "cache initialization is stuck.");

                            throw new IgniteCheckedException("Failed to gather service topology. Utility " +
                                "cache initialization is stuck.");
                        }
                    }
                }
            }

            return serviceTopology(cache, svcName);
        }

        /**
         * @param self Instance of current class before serialization.
         * @param ver Sender job version.
         * @return List of serializable transient fields.
         */
        @SuppressWarnings("unused")
        private static String[] serializableTransient(ServiceTopologyCallable self, IgniteProductVersion ver) {
            return (self != null && self.serialize) || (ver != null && SERVICE_TOP_CALLABLE_VER1.contains(ver)) ? SER_FIELDS : null;
        }
    }

    /**
     *
     */
    private static class ServicesCompatibilityState {
        /** */
        private final boolean srvcCompatibility;

        /** */
        private final boolean used;

        /**
         * @param srvcCompatibility Services compatibility mode ({@code true} if compatible with old nodes).
         * @param used Services has been used.
         */
        ServicesCompatibilityState(boolean srvcCompatibility, boolean used) {
            this.srvcCompatibility = srvcCompatibility;
            this.used = used;
        }
    }
}
