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

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
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
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.GridClosureCallMode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteClientDisconnectedCheckedException;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.events.DiscoveryCustomEvent;
import org.apache.ignite.internal.managers.discovery.DiscoCache;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.managers.eventstorage.DiscoveryEventListener;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheAffinityChangeMessage;
import org.apache.ignite.internal.processors.cache.CacheIteratorConverter;
import org.apache.ignite.internal.processors.cache.DynamicCacheChangeBatch;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.binary.MetadataUpdateAcceptedMessage;
import org.apache.ignite.internal.processors.cache.binary.MetadataUpdateProposedMessage;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxLocal;
import org.apache.ignite.internal.processors.cache.query.CacheQuery;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryManager;
import org.apache.ignite.internal.processors.cluster.DiscoveryDataClusterState;
import org.apache.ignite.internal.processors.cluster.IgniteChangeGlobalStateSupport;
import org.apache.ignite.internal.processors.platform.services.PlatformService;
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
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.plugin.security.SecurityException;
import org.apache.ignite.plugin.security.SecurityPermission;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.JobContextResource;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceConfiguration;
import org.apache.ignite.services.ServiceDeploymentException;
import org.apache.ignite.services.ServiceDescriptor;
import org.apache.ignite.thread.IgniteThreadFactory;
import org.apache.ignite.thread.OomExceptionHandler;
import org.apache.ignite.transactions.Transaction;
import org.jetbrains.annotations.Nullable;

import static javax.cache.event.EventType.REMOVED;
import static org.apache.ignite.configuration.DeploymentMode.ISOLATED;
import static org.apache.ignite.configuration.DeploymentMode.PRIVATE;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.READ_COMMITTED;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * Grid service processor.
 * <p/>
 * Obsolete implementation of service processor, based on replicated system cache.
 * <p/>
 * NOTE: if you fix a bug in this class, please take a look in {@link IgniteServiceProcessor}, perhaps the class
 * contains a similar block of code which also should be updated.
 *
 * @see IgniteServiceProcessor
 * @deprecated Here is improved, but uncompatible implementation {@link IgniteServiceProcessor}, see IEP-17 for details.
 */
@Deprecated
@SuppressWarnings({"SynchronizationOnLocalVariableOrMethodParameter", "ConstantConditions"})
public class GridServiceProcessor extends ServiceProcessorAdapter implements IgniteChangeGlobalStateSupport {
    /** Time to wait before reassignment retries. */
    private static final long RETRY_TIMEOUT = 1000;

    /** */
    private static final int[] EVTS = {
        EventType.EVT_NODE_JOINED,
        EventType.EVT_NODE_LEFT,
        EventType.EVT_NODE_FAILED,
        DiscoveryCustomEvent.EVT_DISCOVERY_CUSTOM_EVT
    };

    /** Local service instances. */
    private final Map<String, Collection<ServiceContextImpl>> locSvcs = new HashMap<>();

    /** Deployment futures. */
    private final ConcurrentMap<String, GridServiceDeploymentFuture<String>> depFuts = new ConcurrentHashMap<>();

    /** Deployment futures. */
    private final ConcurrentMap<String, GridFutureAdapter<?>> undepFuts = new ConcurrentHashMap<>();

    /** Pending compute job contexts that waiting for utility cache initialization. */
    private final List<ComputeJobContext> pendingJobCtxs = new ArrayList<>(0);

    /** Deployment executor service. */
    private volatile ExecutorService depExe;

    /** Busy lock. */
    private volatile GridSpinBusyLock busyLock = new GridSpinBusyLock();

    /** Uncaught exception handler for thread pools. */
    private final UncaughtExceptionHandler oomeHnd = new OomExceptionHandler(ctx);

    /** Thread factory. */
    private ThreadFactory threadFactory = new IgniteThreadFactory(ctx.igniteInstanceName(), "service",
        oomeHnd);

    /** Thread local for service name. */
    private ThreadLocal<String> svcName = new ThreadLocal<>();

    /** Service cache. */
    private volatile IgniteInternalCache<Object, Object> serviceCache;

    /** Topology listener. */
    private DiscoveryEventListener topLsnr = new TopologyListener();

    /** */
    private final CountDownLatch startLatch = new CountDownLatch(1);

    /**
     * @param ctx Kernal context.
     */
    public GridServiceProcessor(GridKernalContext ctx) {
        super(ctx);

        depExe = Executors.newSingleThreadExecutor(new IgniteThreadFactory(ctx.igniteInstanceName(),
            "srvc-deploy", oomeHnd));
    }

    /**
     * @param ctx Context.
     * @throws IgniteCheckedException If failed.
     */
    public void onContinuousProcessorStarted(GridKernalContext ctx) throws IgniteCheckedException {
        if (ctx.clientNode()) {
            assert !ctx.isDaemon();

            ctx.continuous().registerStaticRoutine(
                CU.UTILITY_CACHE_NAME, new ServiceEntriesListener(), null, null
            );
        }
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteCheckedException {
        if (ctx.isDaemon())
            return;

        IgniteConfiguration cfg = ctx.config();

        DeploymentMode depMode = cfg.getDeploymentMode();

        if (cfg.isPeerClassLoadingEnabled() && (depMode == PRIVATE || depMode == ISOLATED) &&
            !F.isEmpty(cfg.getServiceConfiguration()))
            throw new IgniteCheckedException("Cannot deploy services in PRIVATE or ISOLATED deployment mode: " + depMode);
    }

    /** {@inheritDoc} */
    @Override public void onKernalStart(boolean active) throws IgniteCheckedException {
        if (ctx.isDaemon() || !active)
            return;

        onKernalStart0();
    }

    /**
     * Do kernal start.
     *
     * @throws IgniteCheckedException If failed.
     */
    private void onKernalStart0() throws IgniteCheckedException {
        if (!ctx.clientNode())
            ctx.event().addDiscoveryEventListener(topLsnr, EVTS);

        updateUtilityCache();

        startLatch.countDown();

        try {
            if (ctx.deploy().enabled())
                ctx.cache().context().deploy().ignoreOwnership(true);

            if (!ctx.clientNode()) {
                DiscoveryDataClusterState clusterState = ctx.state().clusterState();

                boolean isLocLsnr = !clusterState.hasBaselineTopology() ||
                    CU.baselineNode(ctx.cluster().get().localNode(), clusterState);

                // Register query listener and run it for local entries, if data is available locally.
                // It is also invoked on rebalancing.
                // Otherwise remote listener is registered.
                serviceCache.context().continuousQueries().executeInternalQuery(
                    new ServiceEntriesListener(), null, isLocLsnr, true, false, false
                );
            }
            else { // Listener for client nodes is registered in onContinuousProcessorStarted method.
                assert !ctx.isDaemon();

                ctx.closure().runLocalSafe(new Runnable() {
                    @Override public void run() {
                        try {
                            Iterable<CacheEntryEvent<?, ?>> entries =
                                serviceCache.context().continuousQueries().existingEntries(false, null);

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

        if (cfgs != null)
            deployAll(Arrays.asList(cfgs), ctx.cluster().get().forServers().predicate()).get();

        if (log.isDebugEnabled())
            log.debug("Started service processor.");
    }

    /**
     *
     */
    public void updateUtilityCache() {
        serviceCache = ctx.cache().utilityCache();
    }

    /**
     * @return Service cache.
     */
    private IgniteInternalCache<Object, Object> serviceCache() {
        if (serviceCache == null)
            U.awaitQuiet(startLatch);

        return serviceCache;
    }

    /** {@inheritDoc} */
    @Override public void onKernalStop(boolean cancel) {
        if (ctx.isDaemon())
            return;

        GridSpinBusyLock busyLock = this.busyLock;

        // Will not release it.
        if (busyLock != null) {
            busyLock.block();

            this.busyLock = null;
        }

        startLatch.countDown();

        U.shutdownNow(GridServiceProcessor.class, depExe, log);

        if (!ctx.clientNode())
            ctx.event().removeDiscoveryEventListener(topLsnr);

        Collection<ServiceContextImpl> ctxs = new ArrayList<>();

        synchronized (locSvcs) {
            for (Collection<ServiceContextImpl> ctxs0 : locSvcs.values())
                ctxs.addAll(ctxs0);

            locSvcs.clear();
        }

        for (ServiceContextImpl ctx : ctxs) {
            ctx.setCancelled(true);

            Service svc = ctx.service();

            if (svc != null)
                try {
                    svc.cancel(ctx);
                }
                catch (Throwable e) {
                    log.error("Failed to cancel service (ignoring) [name=" + ctx.name() +
                        ", execId=" + ctx.executionId() + ']', e);

                    if (e instanceof Error)
                        throw e;
                }

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

        Exception err = new IgniteCheckedException("Operation has been cancelled (node is stopping).");

        cancelFutures(depFuts, err);
        cancelFutures(undepFuts, err);

        if (log.isDebugEnabled())
            log.debug("Stopped service processor.");
    }

    /** {@inheritDoc} */
    @Override public void onActivate(GridKernalContext kctx) throws IgniteCheckedException {
        if (log.isDebugEnabled())
            log.debug("Activate service processor [nodeId=" + ctx.localNodeId() +
                " topVer=" + ctx.discovery().topologyVersionEx() + " ]");

        busyLock = new GridSpinBusyLock();

        depExe = Executors.newSingleThreadExecutor(new IgniteThreadFactory(ctx.igniteInstanceName(),
            "srvc-deploy", oomeHnd));

        start();

        onKernalStart0();
    }

    /** {@inheritDoc} */
    @Override public void onDeActivate(GridKernalContext kctx) {
        if (log.isDebugEnabled())
            log.debug("DeActivate service processor [nodeId=" + ctx.localNodeId() +
                " topVer=" + ctx.discovery().topologyVersionEx() + " ]");

        cancelFutures(depFuts, new IgniteCheckedException("Failed to deploy service, cluster in active."));

        cancelFutures(undepFuts, new IgniteCheckedException("Failed to undeploy service, cluster in active."));

        onKernalStop(true);
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
    private PreparedConfigurations<String> prepareServiceConfigurations(Collection<ServiceConfiguration> cfgs,
        IgnitePredicate<ClusterNode> dfltNodeFilter) {
        List<ServiceConfiguration> cfgsCp = new ArrayList<>(cfgs.size());

        Marshaller marsh = ctx.config().getMarshaller();

        List<GridServiceDeploymentFuture<String>> failedFuts = null;

        for (ServiceConfiguration cfg : cfgs) {
            Exception err = null;

            // Deploy to projection node by default
            // or only on server nodes if no projection .
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

            if (err == null) {
                try {
                    ctx.security().authorize(cfg.getName(), SecurityPermission.SERVICE_DEPLOY);
                }
                catch (Exception e) {
                    U.error(log, "Failed to authorize service creation [name=" + cfg.getName() +
                        ", srvc=" + cfg.getService() + ']', e);

                    err = e;
                }
            }

            if (err == null) {
                try {
                    byte[] srvcBytes = U.marshal(marsh, cfg.getService());

                    cfgsCp.add(new LazyServiceConfiguration(cfg, srvcBytes));
                }
                catch (Exception e) {
                    U.error(log, "Failed to marshal service with configured marshaller [name=" + cfg.getName() +
                        ", srvc=" + cfg.getService() + ", marsh=" + marsh + "]", e);

                    err = e;
                }
            }

            if (err != null) {
                if (failedFuts == null)
                    failedFuts = new ArrayList<>();

                GridServiceDeploymentFuture<String> fut = new GridServiceDeploymentFuture<>(cfg, cfg.getName());

                fut.onDone(err);

                failedFuts.add(fut);
            }
        }

        return new PreparedConfigurations<>(cfgsCp, failedFuts);
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
    private IgniteInternalFuture<?> deployAll(Collection<ServiceConfiguration> cfgs,
        @Nullable IgnitePredicate<ClusterNode> dfltNodeFilter) {
        assert cfgs != null;

        PreparedConfigurations<String> srvCfg = prepareServiceConfigurations(cfgs, dfltNodeFilter);

        List<ServiceConfiguration> cfgsCp = srvCfg.cfgs;

        List<GridServiceDeploymentFuture<String>> failedFuts = srvCfg.failedFuts;

        Collections.sort(cfgsCp, new Comparator<ServiceConfiguration>() {
            @Override public int compare(ServiceConfiguration cfg1, ServiceConfiguration cfg2) {
                return cfg1.getName().compareTo(cfg2.getName());
            }
        });

        GridServiceDeploymentCompoundFuture<String> res;

        while (true) {
            res = new GridServiceDeploymentCompoundFuture<>();

            if (ctx.deploy().enabled())
                ctx.cache().context().deploy().ignoreOwnership(true);

            try {
                if (cfgsCp.size() == 1)
                    writeServiceToCache(res, cfgsCp.get(0));
                else if (cfgsCp.size() > 1) {
                    try (Transaction tx = serviceCache().txStart(PESSIMISTIC, READ_COMMITTED)) {
                        for (ServiceConfiguration cfg : cfgsCp) {
                            try {
                                writeServiceToCache(res, cfg);
                            }
                            catch (IgniteCheckedException e) {
                                if (X.hasCause(e, ClusterTopologyCheckedException.class))
                                    throw e; // Retry.
                                else
                                    U.error(log, e.getMessage());
                            }
                        }

                        tx.commit();
                    }
                }

                break;
            }
            catch (IgniteException | IgniteCheckedException e) {
                for (String name : res.servicesToRollback()) {
                    GridServiceDeploymentFuture<String> fut;

                    if ((fut = depFuts.remove(name)) != null)
                        fut.onDone(e);
                }

                if (X.hasCause(e, ClusterTopologyCheckedException.class)) {
                    if (log.isDebugEnabled())
                        log.debug("Topology changed while deploying services (will retry): " + e.getMessage());
                }
                else {
                    res.onDone(new IgniteCheckedException(
                        new ServiceDeploymentException("Failed to deploy provided services.", e, cfgs)));

                    return res;
                }
            }
            finally {
                if (ctx.deploy().enabled())
                    ctx.cache().context().deploy().ignoreOwnership(false);
            }
        }

        if (ctx.clientDisconnected()) {
            IgniteClientDisconnectedCheckedException err =
                new IgniteClientDisconnectedCheckedException(ctx.cluster().clientReconnectFuture(),
                    "Failed to deploy services, client node disconnected: " + cfgs);

            for (String name : res.servicesToRollback()) {
                GridServiceDeploymentFuture<String> fut = depFuts.remove(name);

                if (fut != null)
                    fut.onDone(err);
            }

            return new GridFinishedFuture<>(err);
        }

        if (failedFuts != null) {
            for (GridServiceDeploymentFuture<String> fut : failedFuts)
                res.add(fut, false);
        }

        res.markInitialized();

        return res;
    }

    /**
     * @param res Resulting compound future.
     * @param cfg Service configuration.
     * @throws IgniteCheckedException If operation failed.
     */
    private void writeServiceToCache(GridServiceDeploymentCompoundFuture<String> res, ServiceConfiguration cfg)
        throws IgniteCheckedException {
        String name = cfg.getName();

        GridServiceDeploymentFuture<String> fut = new GridServiceDeploymentFuture<>(cfg, name);

        GridServiceDeploymentFuture<String> old = depFuts.putIfAbsent(name, fut);

        try {
            if (old != null) {
                if (!old.configuration().equalsIgnoreNodeFilter(cfg))
                    throw new IgniteCheckedException("Failed to deploy service (service already exists with different " +
                        "configuration) [deployed=" + old.configuration() + ", new=" + cfg + ']');
                else {
                    res.add(old, false);

                    return;
                }
            }

            GridServiceDeploymentKey key = new GridServiceDeploymentKey(name);

            GridServiceDeployment dep = (GridServiceDeployment)serviceCache().getAndPutIfAbsent(key,
                new GridServiceDeployment(ctx.localNodeId(), cfg));

            if (dep != null) {
                if (!dep.configuration().equalsIgnoreNodeFilter(cfg)) {
                    throw new IgniteCheckedException("Failed to deploy service (service already exists with " +
                        "different configuration) [deployed=" + dep.configuration() + ", new=" + cfg + ']');
                }
                else {
                    res.add(fut, false);

                    Iterator<Cache.Entry<Object, Object>> it = serviceEntries(ServiceAssignmentsPredicate.INSTANCE);

                    while (it.hasNext()) {
                        Cache.Entry<Object, Object> e = it.next();

                        GridServiceAssignments assigns = (GridServiceAssignments)e.getValue();

                        if (assigns.name().equals(name)) {
                            fut.onDone();

                            depFuts.remove(name, fut);

                            break;
                        }
                    }
                }
            }
            else
                res.add(fut, true);
        }
        catch (IgniteCheckedException e) {
            fut.onDone(e);

            res.add(fut, false);

            depFuts.remove(name, fut);

            throw e;
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> cancel(String name) {
        while (true) {
            try {
                return removeServiceFromCache(name).fut;
            }
            catch (IgniteException | IgniteCheckedException e) {
                if (X.hasCause(e, ClusterTopologyCheckedException.class)) {
                    if (log.isDebugEnabled())
                        log.debug("Topology changed while cancelling service (will retry): " + e.getMessage());
                }
                else {
                    U.error(log, "Failed to undeploy service: " + name, e);

                    return new GridFinishedFuture<>(e);
                }
            }
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> cancelAll() {
        Iterator<Cache.Entry<Object, Object>> it = serviceEntries(ServiceDeploymentPredicate.INSTANCE);

        List<String> svcNames = new ArrayList<>();

        while (it.hasNext()) {
            GridServiceDeployment dep = (GridServiceDeployment)it.next().getValue();

            svcNames.add(dep.configuration().getName());
        }

        return cancelAll(svcNames);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public IgniteInternalFuture<?> cancelAll(Collection<String> servicesNames) {
        List<String> svcNamesCp = new ArrayList<>(servicesNames);

        Collections.sort(svcNamesCp);

        GridCompoundFuture res;

        while (true) {
            res = null;

            List<String> toRollback = new ArrayList<>();

            try (Transaction tx = serviceCache().txStart(PESSIMISTIC, READ_COMMITTED)) {
                for (String name : servicesNames) {
                    if (res == null)
                        res = new GridCompoundFuture<>();

                    try {
                        CancelResult cr = removeServiceFromCache(name);

                        if (cr.rollback)
                            toRollback.add(name);

                        res.add(cr.fut);
                    }
                    catch (IgniteException | IgniteCheckedException e) {
                        if (X.hasCause(e, ClusterTopologyCheckedException.class))
                            throw e; // Retry.
                        else {
                            U.error(log, "Failed to undeploy service: " + name, e);

                            res.add(new GridFinishedFuture<>(e));
                        }
                    }
                }

                tx.commit();

                break;
            }
            catch (IgniteException | IgniteCheckedException e) {
                for (String name : toRollback)
                    undepFuts.remove(name).onDone(e);

                if (X.hasCause(e, ClusterTopologyCheckedException.class)) {
                    if (log.isDebugEnabled())
                        log.debug("Topology changed while cancelling service (will retry): " + e.getMessage());
                }
                else
                    return new GridFinishedFuture<>(e);
            }
        }

        if (res != null) {
            res.markInitialized();

            return res;
        }
        else
            return new GridFinishedFuture<>();
    }

    /**
     * @param name Name of service to remove from internal cache.
     * @return Cancellation future and a flag whether it should be completed and removed on error.
     * @throws IgniteCheckedException If operation failed.
     */
    private CancelResult removeServiceFromCache(String name) throws IgniteCheckedException {
        try {
            ctx.security().authorize(name, SecurityPermission.SERVICE_CANCEL);
        }
        catch (SecurityException e) {
            return new CancelResult(new GridFinishedFuture<>(e), false);
        }

        GridFutureAdapter<?> fut = new GridFutureAdapter<>();

        GridFutureAdapter<?> old = undepFuts.putIfAbsent(name, fut);

        if (old != null)
            return new CancelResult(old, false);
        else {
            GridServiceDeploymentKey key = new GridServiceDeploymentKey(name);

            try {
                if (serviceCache().getAndRemove(key) == null) {
                    // Remove future from local map if service was not deployed.
                    undepFuts.remove(name, fut);

                    fut.onDone();

                    return new CancelResult(fut, false);
                }
                else
                    return new CancelResult(fut, true);
            }
            catch (IgniteCheckedException e) {
                undepFuts.remove(name, fut);

                fut.onDone(e);

                throw e;
            }
        }
    }

    /** {@inheritDoc} */
    @Override public Map<UUID, Integer> serviceTopology(String name, long timeout) throws IgniteCheckedException {
        IgniteInternalCache<Object, Object> cache = serviceCache();

        ClusterNode node = cache.affinity().mapKeyToNode(name);

        final ServiceTopologyCallable call = new ServiceTopologyCallable(name);

        return ctx.closure().callAsyncNoFailover(
            GridClosureCallMode.BROADCAST,
            call,
            Collections.singletonList(node),
            false,
            timeout,
            true).get();
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

    /** {@inheritDoc} */
    @Override public Collection<ServiceDescriptor> serviceDescriptors() {
        Collection<ServiceDescriptor> descs = new ArrayList<>();

        Iterator<Cache.Entry<Object, Object>> it = serviceEntries(ServiceDeploymentPredicate.INSTANCE);

        while (it.hasNext()) {
            Cache.Entry<Object, Object> e = it.next();

            GridServiceDeployment dep = (GridServiceDeployment)e.getValue();

            ServiceDescriptorImpl desc = new ServiceDescriptorImpl(dep);

            try {
                GridServiceAssignments assigns = (GridServiceAssignments)serviceCache().getForcePrimary(
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

    /** {@inheritDoc} */
    @Override public <T> T service(String name) {
        ctx.security().authorize(name, SecurityPermission.SERVICE_INVOKE);

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

    /** {@inheritDoc} */
    @Override public ServiceContextImpl serviceContext(String name) {
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

    /** {@inheritDoc} */
    @Override public <T> T serviceProxy(ClusterGroup prj, String name, Class<? super T> srvcCls, boolean sticky,
        long timeout)
        throws IgniteException {
        ctx.security().authorize(name, SecurityPermission.SERVICE_INVOKE);

        if (hasLocalNode(prj)) {
            ServiceContextImpl ctx = serviceContext(name);

            if (ctx != null) {
                Service svc = ctx.service();

                if (svc != null) {
                    if (srvcCls.isAssignableFrom(svc.getClass()))
                        return (T)svc;
                    else if (!PlatformService.class.isAssignableFrom(svc.getClass())) {
                        throw new IgniteException("Service does not implement specified interface [svcItf=" +
                                srvcCls.getName() + ", svcCls=" + svc.getClass().getName() + ']');
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
        ctx.security().authorize(name, SecurityPermission.SERVICE_INVOKE);

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
        IgniteInternalCache<Object, Object> cache = serviceCache();

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

            try (GridNearTxLocal tx = cache.txStartEx(PESSIMISTIC, REPEATABLE_READ)) {
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
        if (assigns.topologyVersion() < ctx.discovery().topologyVersion()) {
            if (log.isDebugEnabled())
                log.debug("Skip outdated assignment [assigns=" + assigns +
                    ", topVer=" + ctx.discovery().topologyVersion() + ']');

            return;
        }

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
        for (Iterator<ServiceContextImpl> it = ctxs.iterator(); it.hasNext(); ) {
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
            IgniteInternalCache<Object, Object> cache = serviceCache();

            GridCacheQueryManager qryMgr = cache.context().queries();

            CacheQuery<Map.Entry<Object, Object>> qry = qryMgr.createScanQuery(p, null, false, null);

            DiscoveryDataClusterState clusterState = ctx.state().clusterState();

            if ((clusterState.hasBaselineTopology()
                && !CU.baselineNode(ctx.cluster().get().localNode(), clusterState))
                || !cache.context().affinityNode()) {
                ClusterNode oldestSrvNode =
                    ctx.discovery().oldestAliveServerNode(AffinityTopologyVersion.NONE);

                if (oldestSrvNode == null)
                    return new GridEmptyIterator<>();

                qry.projection(ctx.cluster().get().forNode(oldestSrvNode));
            }
            else
                qry.projection(ctx.cluster().get().forLocal());

            GridCloseableIterator<Map.Entry<Object, Object>> iter = qry.executeScanQuery();

            return cache.context().itHolder().iterator(iter,
                new CacheIteratorConverter<Cache.Entry<Object, Object>, Map.Entry<Object, Object>>() {
                    @Override protected Cache.Entry<Object, Object> convert(Map.Entry<Object, Object> e) {
                        // Actually Scan Query returns Iterator<CacheQueryEntry> by default,
                        // CacheQueryEntry implements both Map.Entry and Cache.Entry interfaces.
                        return (Cache.Entry<Object, Object>)e;
                    }

                    @Override protected void remove(Cache.Entry<Object, Object> item) {
                        throw new UnsupportedOperationException();
                    }
                });
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /**
     * Called right after utility cache is started and ready for the usage.
     */
    public void onUtilityCacheStarted() {
        synchronized (pendingJobCtxs) {
            if (pendingJobCtxs.isEmpty())
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
    private class ServiceEntriesListener implements CacheEntryUpdatedListener<Object, Object> {
        /** {@inheritDoc} */
        @Override public void onUpdated(final Iterable<CacheEntryEvent<?, ?>> deps) {
            GridSpinBusyLock busyLock = GridServiceProcessor.this.busyLock;

            if (busyLock == null || !busyLock.enterBusy())
                return;

            try {
                depExe.execute(new DepRunnable() {
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
        for (CacheEntryEvent<?, ?> e : evts) {
            if (e.getKey() instanceof GridServiceDeploymentKey)
                processDeployment((CacheEntryEvent)e);
            else if (e.getKey() instanceof GridServiceAssignmentsKey)
                processAssignment((CacheEntryEvent)e);
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

        if (e.getEventType() != REMOVED) {
            svcName.set(dep.configuration().getName());

            // Ignore other utility cache events.
            AffinityTopologyVersion topVer = ctx.discovery().topologyVersionEx();

            ClusterNode oldest = U.oldest(ctx.discovery().nodes(topVer), null);

            // Process deployment on coordinator only.
            if (oldest.isLocal())
                onDeployment(dep, topVer);
        }
        // Handle undeployment.
        else {
            String name = e.getKey().name();

            undeploy(name);

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
            IgniteInternalCache<Object, Object> cache = serviceCache();

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
                    depExe.execute(new DepRunnable() {
                        @Override public void run0() {
                            onDeployment(dep, topVer);
                        }
                    });
                }
            });
        }
    }

    /**
     * Topology listener.
     */
    private class TopologyListener implements DiscoveryEventListener {
        /** */
        private volatile AffinityTopologyVersion currTopVer = null;

        /**
         * Check that listening-in topology version is the latest and wait until exchange is finished.
         *
         * @param initTopVer listening-in topology version.
         * @return {@code True} if current event is not last and should be skipped.
         */
        private boolean skipExchange(final AffinityTopologyVersion initTopVer) {
            AffinityTopologyVersion pendingTopVer = null;
            AffinityTopologyVersion newTopVer;

            if (!initTopVer.equals(newTopVer = currTopVer))
                pendingTopVer = newTopVer;
            else {
                IgniteInternalFuture<?> affReadyFut = ctx.cache().context().exchange().affinityReadyFuture(initTopVer);

                if (affReadyFut != null) {
                    try {
                        affReadyFut.get();
                    }
                    catch (IgniteCheckedException e) {
                        U.warn(log, "Failed to wait for affinity ready future " +
                            "(the assignment will be recalculated anyway):" + e.toString());
                    }
                }

                // If exchange already moved forward - skip current version.
                if (!initTopVer.equals(newTopVer = currTopVer))
                    pendingTopVer = newTopVer;
            }

            boolean skipExchange = pendingTopVer != null;

            if (skipExchange && log.isInfoEnabled()) {
                log.info("Service processor detected a topology change during " +
                    "assignments calculation (will abort current iteration and " +
                    "re-calculate on the newer version): " +
                    "[topVer=" + initTopVer + ", newTopVer=" + pendingTopVer + ']');
            }

            return skipExchange;
        }

        /** {@inheritDoc} */
        @Override public void onEvent(final DiscoveryEvent evt, final DiscoCache discoCache) {
            GridSpinBusyLock busyLock = GridServiceProcessor.this.busyLock;

            if (busyLock == null || !busyLock.enterBusy())
                return;

            try {
                final AffinityTopologyVersion topVer;

                if (evt instanceof DiscoveryCustomEvent) {
                    DiscoveryCustomMessage msg = ((DiscoveryCustomEvent)evt).customMessage();

                    if (msg instanceof CacheAffinityChangeMessage) {
                        if (!((CacheAffinityChangeMessage)msg).exchangeNeeded())
                            return;
                    }
                    else if (msg instanceof DynamicCacheChangeBatch) {
                        if (!((DynamicCacheChangeBatch)msg).exchangeNeeded())
                            return;
                    }
                    else
                        return;

                    if (msg instanceof MetadataUpdateProposedMessage || msg instanceof MetadataUpdateAcceptedMessage)
                        return;

                    topVer = ((DiscoveryCustomEvent)evt).affinityTopologyVersion();
                }
                else
                    topVer = new AffinityTopologyVersion((evt).topologyVersion(), 0);

                currTopVer = topVer;

                depExe.execute(new DepRunnable() {
                    @Override public void run0() {
                        // In case the cache instance isn't tracked by DiscoveryManager anymore.
                        discoCache.updateAlives(ctx.discovery());

                        ClusterNode oldest = discoCache.oldestAliveServerNode();

                        if (oldest != null && oldest.isLocal()) {
                            final Collection<GridServiceDeployment> retries = new ConcurrentLinkedQueue<>();

                            if (ctx.deploy().enabled())
                                ctx.cache().context().deploy().ignoreOwnership(true);

                            try {
                                Iterator<Cache.Entry<Object, Object>> it = serviceEntries(
                                    ServiceDeploymentPredicate.INSTANCE);

                                while (it.hasNext()) {
                                    // If topology changed again, let next event handle it.
                                    if (skipExchange(topVer))
                                        return;

                                    Cache.Entry<Object, Object> e = it.next();

                                    GridServiceDeployment dep = (GridServiceDeployment)e.getValue();

                                    try {
                                        svcName.set(dep.configuration().getName());

                                        reassign(dep, topVer);
                                    }
                                    catch (IgniteCheckedException ex) {
                                        if (!(ex instanceof ClusterTopologyCheckedException))
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

                        Iterator<Cache.Entry<Object, Object>> it = serviceEntries(ServiceAssignmentsPredicate.INSTANCE);

                        // Clean up zombie assignments.
                        IgniteInternalCache<Object, Object> cache = serviceCache();

                        while (it.hasNext()) {
                            // If topology changed again, let next event handle it.
                            if (skipExchange(topVer))
                                return;

                            Cache.Entry<Object, Object> e = it.next();

                            if (cache.context().affinity().primaryByKey(ctx.grid().localNode(), e.getKey(), topVer)) {
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
            GridSpinBusyLock busyLock = GridServiceProcessor.this.busyLock;

            if (busyLock == null || !busyLock.enterBusy())
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
                            depExe.execute(new Runnable() {
                                @Override public void run() {
                                    onReassignmentFailed(topVer, retries);
                                }
                            });
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

        if (e.getEventType() != REMOVED) {
            svcName.set(assigns.name());

            Throwable t = null;

            try {
                redeploy(assigns);
            }
            catch (Error | RuntimeException th) {
                t = th;
            }

            GridServiceDeploymentFuture<String> fut = depFuts.get(assigns.name());

            if (fut != null && fut.configuration().equalsIgnoreNodeFilter(assigns.configuration())) {
                depFuts.remove(assigns.name(), fut);

                // Complete deployment futures once the assignments have been stored in cache.
                fut.onDone(null, t);
            }
        }
        // Handle undeployment.
        else
            undeploy(e.getKey().name());
    }

    /**
     * @param name Name.
     */
    private void undeploy(String name) {
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

    /**
     *
     */
    private static class CancelResult {
        /** */
        IgniteInternalFuture<?> fut;

        /** */
        boolean rollback;

        /**
         * @param fut Future.
         * @param rollback {@code True} if service was cancelled during current call.
         */
        CancelResult(IgniteInternalFuture<?> fut, boolean rollback) {
            this.fut = fut;
            this.rollback = rollback;
        }
    }

    /**
     *
     */
    private abstract class DepRunnable implements Runnable {
        /** {@inheritDoc} */
        @Override public void run() {
            GridSpinBusyLock busyLock = GridServiceProcessor.this.busyLock;

            if (busyLock == null || !busyLock.enterBusy())
                return;

            // Won't block ServiceProcessor stopping process.
            busyLock.leaveBusy();

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
    private static class ServiceTopologyCallable implements IgniteCallable<Map<UUID, Integer>> {
        /** */
        private static final long serialVersionUID = 0L;

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
                List<ComputeJobContext> pendingCtxs = ((GridServiceProcessor)ignite.context().service()).pendingJobCtxs;

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
    }
}
