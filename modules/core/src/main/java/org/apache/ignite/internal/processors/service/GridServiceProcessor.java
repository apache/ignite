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

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.events.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.cluster.*;
import org.apache.ignite.internal.managers.eventstorage.*;
import org.apache.ignite.internal.processors.*;
import org.apache.ignite.internal.processors.affinity.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.processors.cache.query.*;
import org.apache.ignite.internal.processors.cache.transactions.*;
import org.apache.ignite.internal.processors.timeout.*;
import org.apache.ignite.internal.util.*;
import org.apache.ignite.internal.util.future.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.marshaller.*;
import org.apache.ignite.services.*;
import org.apache.ignite.thread.*;
import org.jetbrains.annotations.*;
import org.jsr166.*;

import javax.cache.*;
import javax.cache.event.*;
import java.util.*;
import java.util.concurrent.*;

import static org.apache.ignite.configuration.DeploymentMode.*;
import static org.apache.ignite.events.EventType.*;
import static org.apache.ignite.internal.processors.cache.GridCacheUtils.*;
import static org.apache.ignite.transactions.TransactionConcurrency.*;
import static org.apache.ignite.transactions.TransactionIsolation.*;

/**
 * Grid service processor.
 */
@SuppressWarnings({"SynchronizationOnLocalVariableOrMethodParameter", "ConstantConditions"})
public class GridServiceProcessor extends GridProcessorAdapter {
    /** Time to wait before reassignment retries. */
    private static final long RETRY_TIMEOUT = 1000;

    /** Local service instances. */
    private final Map<String, Collection<ServiceContextImpl>> locSvcs = new HashMap<>();

    /** Deployment futures. */
    private final ConcurrentMap<String, GridServiceDeploymentFuture> depFuts = new ConcurrentHashMap8<>();

    /** Deployment futures. */
    private final ConcurrentMap<String, GridFutureAdapter<?>> undepFuts = new ConcurrentHashMap8<>();

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

    /** Deployment listener ID. */
    private UUID cfgQryId;

    /** Assignment listener ID. */
    private UUID assignQryId;

    /**
     * @param ctx Kernal context.
     */
    public GridServiceProcessor(GridKernalContext ctx) {
        super(ctx);

        depExe = Executors.newSingleThreadExecutor(new IgniteThreadFactory(ctx.gridName(), "srvc-deploy"));
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
    @SuppressWarnings("unchecked")
    @Override public void onKernalStart() throws IgniteCheckedException {
        if (ctx.isDaemon())
            return;

        cache = ctx.cache().utilityCache();

        if (!ctx.clientNode())
            ctx.event().addLocalEventListener(topLsnr, EVTS_DISCOVERY);

        try {
            if (ctx.deploy().enabled())
                ctx.cache().context().deploy().ignoreOwnership(true);

            cfgQryId = cache.context().continuousQueries().executeInternalQuery(
                new DeploymentListener(), null, cache.context().affinityNode(), true);

            assignQryId = cache.context().continuousQueries().executeInternalQuery(
                new AssignmentListener(), null, cache.context().affinityNode(), true);
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

        if (cfgQryId != null)
            cache.context().continuousQueries().cancelInternalQuery(cfgQryId);

        if (assignQryId != null)
            cache.context().continuousQueries().cancelInternalQuery(assignQryId);

        Collection<ServiceContextImpl> ctxs = new ArrayList<>();

        synchronized (locSvcs) {
            for (Collection<ServiceContextImpl> ctxs0 : locSvcs.values())
                ctxs.addAll(ctxs0);
        }

        for (ServiceContextImpl ctx : ctxs) {
            ctx.setCancelled(true);
            ctx.service().cancel(ctx);

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

        if (log.isDebugEnabled())
            log.debug("Stopped service processor.");
    }

    /** {@inheritDoc} */
    @Override public void onDisconnected(IgniteFuture<?> reconnectFut) throws IgniteCheckedException {
        for (Map.Entry<String, GridServiceDeploymentFuture> e : depFuts.entrySet()) {
            GridServiceDeploymentFuture fut = e.getValue();

            fut.onDone(new IgniteClientDisconnectedCheckedException(ctx.cluster().clientReconnectFuture(),
                "Failed to deploy service, client node disconnected."));

            depFuts.remove(e.getKey(), fut);
        }

        for (Map.Entry<String, GridFutureAdapter<?>> e : undepFuts.entrySet()) {
            GridFutureAdapter fut = e.getValue();

            fut.onDone(new IgniteClientDisconnectedCheckedException(ctx.cluster().clientReconnectFuture(),
                "Failed to undeploy service, client node disconnected."));

            undepFuts.remove(e.getKey(), fut);
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

        validate(cfg);

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
        Collection<IgniteInternalFuture<?>> futs = new ArrayList<>();

        Iterator<Cache.Entry<Object, Object>> it = serviceEntries(ServiceDeploymentPredicate.INSTANCE);

        while (it.hasNext()) {
            Cache.Entry<Object, Object> e = it.next();

            if (!(e.getKey() instanceof GridServiceDeploymentKey))
                continue;

            GridServiceDeployment dep = (GridServiceDeployment)e.getValue();

            // Cancel each service separately.
            futs.add(cancel(dep.configuration().getName()));
        }

        return futs.isEmpty() ? new GridFinishedFuture<>() : new GridCompoundFuture(null, futs);
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

            return (T)ctxs.iterator().next().service();
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

            return ctxs.iterator().next();
        }
    }

    /**
     * @param prj Grid projection.
     * @param name Service name.
     * @param svcItf Service class.
     * @param sticky Whether multi-node request should be done.
     * @param <T> Service interface type.
     * @return The proxy of a service by its name and class.
     * @throws IgniteException If failed to create proxy.
     */
    @SuppressWarnings("unchecked")
    public <T> T serviceProxy(ClusterGroup prj, String name, Class<? super T> svcItf, boolean sticky)
        throws IgniteException {
        if (hasLocalNode(prj)) {
            ServiceContextImpl ctx = serviceContext(name);

            if (ctx != null) {
                if (!svcItf.isAssignableFrom(ctx.service().getClass()))
                    throw new IgniteException("Service does not implement specified interface [svcItf=" +
                        svcItf.getSimpleName() + ", svcCls=" + ctx.service().getClass() + ']');

                return (T)ctx.service();
            }
        }

        return new GridServiceProxy<>(prj, name, svcItf, sticky, ctx).proxy();
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

            for (ServiceContextImpl ctx : ctxs)
                res.add((T)ctx.service());

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
    private void reassign(GridServiceDeployment dep, long topVer) throws IgniteCheckedException {
        ServiceConfiguration cfg = dep.configuration();

        int totalCnt = cfg.getTotalCount();
        int maxPerNodeCnt = cfg.getMaxPerNodeCount();
        String cacheName = cfg.getCacheName();
        Object affKey = cfg.getAffinityKey();

        while (true) {
            try (IgniteInternalTx tx = cache.txStartEx(PESSIMISTIC, REPEATABLE_READ)) {
                GridServiceAssignmentsKey key = new GridServiceAssignmentsKey(cfg.getName());

                GridServiceAssignments oldAssigns = (GridServiceAssignments)cache.get(key);

                GridServiceAssignments assigns = new GridServiceAssignments(cfg, dep.nodeId(), topVer);

                Map<UUID, Integer> cnts = new HashMap<>();

                if (affKey != null) {
                    ClusterNode n = ctx.affinity().mapKeyToNode(cacheName, affKey, new AffinityTopologyVersion(topVer));

                    if (n != null) {
                        int cnt = maxPerNodeCnt == 0 ? totalCnt == 0 ? 1 : totalCnt : maxPerNodeCnt;

                        cnts.put(n.id(), cnt);
                    }
                }
                else {
                    Collection<ClusterNode> nodes = assigns.nodeFilter() == null ?
                        ctx.discovery().nodes(topVer) :
                        F.view(ctx.discovery().nodes(topVer), assigns.nodeFilter());

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
                                            if (e.getValue() < maxPerNodeCnt) {
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

                cache.getAndPut(key, assigns);

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

        Service svc = assigns.service();

        Collection<ServiceContextImpl> ctxs;

        synchronized (locSvcs) {
            ctxs = locSvcs.get(svcName);

            if (ctxs == null)
                locSvcs.put(svcName, ctxs = new ArrayList<>());
        }

        synchronized (ctxs) {
            if (ctxs.size() > assignCnt) {
                int cancelCnt = ctxs.size() - assignCnt;

                cancel(ctxs, cancelCnt);
            }
            else if (ctxs.size() < assignCnt) {
                int createCnt = assignCnt - ctxs.size();

                for (int i = 0; i < createCnt; i++) {
                    final Service cp = copyAndInject(svc);

                    final ExecutorService exe = Executors.newSingleThreadExecutor(threadFactory);

                    final ServiceContextImpl svcCtx = new ServiceContextImpl(assigns.name(),
                        UUID.randomUUID(), assigns.cacheName(), assigns.affinityKey(), cp, exe);

                    ctxs.add(svcCtx);

                    try {
                        // Initialize service.
                        cp.init(svcCtx);
                    }
                    catch (Throwable e) {
                        log.error("Failed to initialize service (service will not be deployed): " + assigns.name(), e);

                        ctxs.remove(svcCtx);

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
                    exe.submit(new Runnable() {
                        @Override public void run() {
                            try {
                                cp.execute(svcCtx);
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
        }
    }

    /**
     * @param svc Service.
     * @return Copy of service.
     */
    private Service copyAndInject(Service svc) {
        Marshaller m = ctx.config().getMarshaller();

        try {
            byte[] bytes = m.marshal(svc);

            Service cp = m.unmarshal(bytes, svc.getClass().getClassLoader());

            ctx.resource().inject(cp);

            return cp;
        }
        catch (IgniteCheckedException e) {
            log.error("Failed to copy service (will reuse same instance): " + svc.getClass(), e);

            return svc;
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
            try {
                svcCtx.service().cancel(svcCtx);
            }
            catch (Throwable e) {
                log.error("Failed to cancel service (ignoring) [name=" + svcCtx.name() +
                    ", execId=" + svcCtx.executionId() + ']', e);

                if (e instanceof Error)
                    throw e;
            }
            finally {
                try {
                    ctx.resource().cleanup(svcCtx.service());
                }
                catch (IgniteCheckedException e) {
                    log.error("Failed to clean up service (will ignore): " + svcCtx.name(), e);
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
        if (!cache.context().affinityNode()) {
            ClusterNode oldestSrvNode =
                CU.oldestAliveCacheServerNode(cache.context().shared(), AffinityTopologyVersion.NONE);

            if (oldestSrvNode == null)
                return F.emptyIterator();

            GridCacheQueryManager qryMgr = cache.context().queries();

            CacheQuery<Map.Entry<Object, Object>> qry = qryMgr.createScanQuery(p, null, false);

            qry.keepAll(false);

            qry.projection(ctx.cluster().get().forNode(oldestSrvNode));

            return cache.context().itHolder().iterator(qry.execute(),
                new CacheIteratorConverter<Object, Map.Entry<Object,Object>>() {
                    @Override protected Object convert(Map.Entry<Object, Object> e) {
                        return new CacheEntryImpl<>(e.getKey(), e.getValue());
                    }

                    @Override protected void remove(Object item) {
                        throw new UnsupportedOperationException();
                    }
                }
            );
        }
        else
            return cache.entrySetx().iterator();
    }

    /**
     * Service deployment listener.
     */
    private class DeploymentListener implements CacheEntryUpdatedListener<Object, Object> {
        /** {@inheritDoc} */
        @Override public void onUpdated(final Iterable<CacheEntryEvent<?, ?>> deps) {
            depExe.submit(new BusyRunnable() {
                @Override public void run0() {
                    for (CacheEntryEvent<?, ?> e : deps) {
                        if (!(e.getKey() instanceof GridServiceDeploymentKey))
                            continue;

                        GridServiceDeployment dep = (GridServiceDeployment)e.getValue();

                        if (dep != null) {
                            svcName.set(dep.configuration().getName());

                            // Ignore other utility cache events.
                            long topVer = ctx.discovery().topologyVersion();

                            ClusterNode oldest = U.oldest(ctx.discovery().nodes(topVer), null);

                            if (oldest.isLocal())
                                onDeployment(dep, topVer);
                        }
                        // Handle undeployment.
                        else {
                            String name = ((GridServiceDeploymentKey)e.getKey()).name();

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
                }
            });
        }

        /**
         * Deployment callback.
         *
         * @param dep Service deployment.
         * @param topVer Topology version.
         */
        private void onDeployment(final GridServiceDeployment dep, final long topVer) {
            // Retry forever.
            try {
                long newTopVer = ctx.discovery().topologyVersion();

                // If topology version changed, reassignment will happen from topology event.
                if (newTopVer == topVer)
                    reassign(dep, topVer);
            }
            catch (IgniteCheckedException e) {
                if (!(e instanceof ClusterTopologyCheckedException))
                    log.error("Failed to do service reassignment (will retry): " + dep.configuration().getName(), e);

                long newTopVer = ctx.discovery().topologyVersion();

                if (newTopVer != topVer) {
                    assert newTopVer > topVer;

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
    }

    /**
     * Topology listener.
     */
    private class TopologyListener implements GridLocalEventListener {
        /** {@inheritDoc} */
        @Override public void onEvent(final Event evt) {
            if (!busyLock.enterBusy())
                return;

            try {
                depExe.submit(new BusyRunnable() {
                    @Override public void run0() {
                        AffinityTopologyVersion topVer =
                            new AffinityTopologyVersion(((DiscoveryEvent)evt).topologyVersion());

                        ClusterNode oldest = CU.oldestAliveCacheServerNode(cache.context().shared(), topVer);

                        if (oldest != null && oldest.isLocal()) {
                            final Collection<GridServiceDeployment> retries = new ConcurrentLinkedQueue<>();

                            if (ctx.deploy().enabled())
                                ctx.cache().context().deploy().ignoreOwnership(true);

                            try {
                                Iterator<Cache.Entry<Object, Object>> it = serviceEntries(
                                    ServiceDeploymentPredicate.INSTANCE);

                                while (it.hasNext()) {
                                    Cache.Entry<Object, Object> e = it.next();

                                    if (!(e.getKey() instanceof GridServiceDeploymentKey))
                                        continue;

                                    GridServiceDeployment dep = (GridServiceDeployment)e.getValue();

                                    try {
                                        svcName.set(dep.configuration().getName());

                                        ctx.cache().internalCache(UTILITY_CACHE_NAME).context().affinity().
                                            affinityReadyFuture(topVer).get();

                                        reassign(dep, topVer.topologyVersion());
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
                                onReassignmentFailed(topVer.topologyVersion(), retries);
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
        private void onReassignmentFailed(final long topVer, final Collection<GridServiceDeployment> retries) {
            if (!busyLock.enterBusy())
                return;

            try {
                // If topology changed again, let next event handle it.
                if (ctx.discovery().topologyVersion() != topVer)
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
     * Assignment listener.
     */
    private class AssignmentListener implements CacheEntryUpdatedListener<Object, Object> {
        /** {@inheritDoc} */
        @Override public void onUpdated(final Iterable<CacheEntryEvent<?, ?>> assignCol) throws CacheEntryListenerException {
            depExe.submit(new BusyRunnable() {
                @Override public void run0() {
                    for (CacheEntryEvent<?, ?> e : assignCol) {
                        if (!(e.getKey() instanceof GridServiceAssignmentsKey))
                            continue;

                        GridServiceAssignments assigns = (GridServiceAssignments)e.getValue();

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
                            String name = ((GridServiceAssignmentsKey)e.getKey()).name();

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
                }
            });
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
}
