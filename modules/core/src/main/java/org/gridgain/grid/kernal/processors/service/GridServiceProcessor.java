// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.service;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.events.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.managers.eventstorage.*;
import org.gridgain.grid.kernal.processors.*;
import org.gridgain.grid.kernal.processors.timeout.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.marshaller.*;
import org.gridgain.grid.service.*;
import org.gridgain.grid.thread.*;
import org.gridgain.grid.util.*;
import org.gridgain.grid.util.future.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jdk8.backport.*;
import org.jetbrains.annotations.*;

import java.util.*;
import java.util.concurrent.*;

import static org.gridgain.grid.GridDeploymentMode.*;
import static org.gridgain.grid.cache.GridCacheTxConcurrency.*;
import static org.gridgain.grid.cache.GridCacheTxIsolation.*;
import static org.gridgain.grid.events.GridEventType.*;

/**
 * Grid service processor.
 *
 * @author @java.author
 * @version @java.version
 */
@SuppressWarnings({"SynchronizationOnLocalVariableOrMethodParameter", "ConstantConditions"})
public class GridServiceProcessor extends GridProcessorAdapter {
    /** Thread factory. */
    private GridThreadFactory threadFactory = new GridThreadFactory(ctx.gridName());

    /** Time to wait before reassignment retries. */
    private static final long RETRY_TIMEOUT = 1000;

    /** Service configuration cache. */
    private GridCacheProjection<GridServiceConfigurationKey, GridServiceConfiguration> cfgCache;

    /** Service assignments cache. */
    private GridCacheProjection<GridServiceAssignmentsKey, GridServiceAssignments> assignCache;

    /** Local service instances. */
    private final Map<String, Collection<GridServiceContextImpl>> locSvcs = new HashMap<>();

    /** Topology listener. */
    private TopologyListener topLsnr = new TopologyListener();

    /** Deployment listener. */
    private DeploymentListener depLsnr = new DeploymentListener();

    /** Assignment listener. */
    private AssignmentListener assignLsnr = new AssignmentListener();

    /** Deployment futures. */
    private final ConcurrentMap<String, GridFutureAdapter<?>> futs = new ConcurrentHashMap8<>();

    /** Busy lock. */
    private final GridSpinBusyLock busyLock = new GridSpinBusyLock();

    /**
     * @param ctx Kernal context.
     */
    protected GridServiceProcessor(GridKernalContext ctx) {
        super(ctx);
    }

    /** {@inheritDoc} */
    @Override public void start() throws GridException {
        GridConfiguration cfg = ctx.config();

        GridDeploymentMode depMode = cfg.getDeploymentMode();

        if (cfg.isPeerClassLoadingEnabled() && (depMode == PRIVATE || depMode == ISOLATED) &&
            !F.isEmpty(cfg.getServiceConfiguration()))
            throw new GridException("Cannot deploy services in PRIVATE or ISOLATED deployment mode: " + depMode);
    }

    /** {@inheritDoc} */
    @Override public void onKernalStart() throws GridException {
        cfgCache = ctx.cache().utilityCache(GridServiceConfigurationKey.class, GridServiceConfiguration.class);
        assignCache = ctx.cache().utilityCache(GridServiceAssignmentsKey.class, GridServiceAssignments.class);

        ctx.event().addLocalEventListener(topLsnr, EVTS_DISCOVERY);
        ctx.event().addLocalEventListener(depLsnr, EVT_CACHE_OBJECT_PUT);
        ctx.event().addLocalEventListener(assignLsnr, EVT_CACHE_OBJECT_PUT);

        Collection<GridFuture<?>> futs = new ArrayList<>();

        for (GridServiceConfiguration c : ctx.config().getServiceConfiguration())
            futs.add(deploy(c));

        // Await for services to deploy.
        for (GridFuture<?> f : futs)
            f.get();

        // Just in case if we missed an event, reprocess assignments.
        for (GridServiceAssignments assigns : assignCache.values())
            redeploy(assigns);

        if (log.isDebugEnabled())
            log.debug("Started service processor.");
    }

    /** {@inheritDoc} */
    @Override public void onKernalStop(boolean cancel) {
        busyLock.block();

        ctx.event().removeLocalEventListener(topLsnr);
        ctx.event().removeLocalEventListener(depLsnr);
        ctx.event().removeLocalEventListener(assignLsnr);

        Map<String, Collection<GridServiceContextImpl>> locSvcs;

        synchronized (this.locSvcs) {
            locSvcs = new HashMap<>(this.locSvcs);
        }

        for (Collection<GridServiceContextImpl> ctxs : locSvcs.values()) {
            synchronized (ctxs) {
                cancel(ctxs, ctxs.size());
            }
        }

        if (log.isDebugEnabled())
            log.debug("Stopped service processor.");
    }

    /**
     * Validates service configuration.
     *
     * @param c Service configuration.
     * @throws GridRuntimeException If validation failed.
     */
    private void validate(GridServiceConfiguration c) throws GridRuntimeException {
        GridConfiguration cfg = ctx.config();

        GridDeploymentMode depMode = cfg.getDeploymentMode();

        if (cfg.isPeerClassLoadingEnabled() && (depMode == PRIVATE || depMode == ISOLATED))
            throw new GridRuntimeException("Cannot deploy services in PRIVATE or ISOLATED deployment mode: " + depMode);

        ensure(c.getTotalCount() > 0, "getTotalCount() > 0", c.getTotalCount());
        ensure(c.getMaxPerNodeCount() > 0, "getMaxPerNodeCount() > 0", c.getMaxPerNodeCount());
        ensure(c.getService() != null, "getService() != null", c.getService());
    }

    /**
     * @param cond Condition.
     * @param desc Description.
     * @param v Value.
     */
    private void ensure(boolean cond, String desc, Object v) {
        if (!cond)
            throw new GridRuntimeException("Service configuration check failed (" + desc + "): " + v);
    }

    /**
     * @param name Service name.
     * @param svc Service.
     * @return Future.
     */
    public GridFuture<?> deployOnEachNode(GridProjection prj, String name, GridService svc) {
        return deployMultiple(prj, name, svc, 0, 1);
    }

    /**
     * @param name Service name.
     * @param svc Service.
     * @return Future.
     */
    public GridFuture<?> deploySingleton(GridProjection prj, String name, GridService svc) {
        return deployMultiple(prj, name, svc, 1, 1);
    }

    /**
     * @param name Service name.
     * @param svc Service.
     * @param totalCnt Total count.
     * @param maxPerNodeCnt Max per-node count.
     * @return Future.
     */
    public GridFuture<?> deployMultiple(GridProjection prj, String name, GridService svc, int totalCnt,
        int maxPerNodeCnt) {
        GridServiceConfiguration cfg = new GridServiceConfiguration();

        cfg.setName(name);
        cfg.setService(svc);
        cfg.setTotalCount(totalCnt);
        cfg.setMaxPerNodeCount(maxPerNodeCnt);
        cfg.setNodeFilter(F.<GridNode>alwaysTrue() == prj.predicate() ? null : prj.predicate());

        return deploy(cfg);
    }

    /**
     * @param name Service name.
     * @param svc Service.
     * @param cacheName Cache name.
     * @param  affKey Affinity key.
     * @return Future.
     */
    public GridFuture<?> deployForAffinityKey(String name, GridService svc, String cacheName, Object affKey) {
        GridServiceConfiguration cfg = new GridServiceConfiguration();

        cfg.setName(name);
        cfg.setService(svc);
        cfg.setCacheName(cacheName);
        cfg.setAffinityKey(affKey);

        return deploy(cfg);
    }

    /**
     * @param cfg Service configuration.
     * @return Future.
     */
    public GridFuture<?> deploy(GridServiceConfiguration cfg) {
        validate(cfg);

        try {
            GridFutureAdapter<?> fut = new GridFutureAdapter<>(ctx);

            GridFutureAdapter<?> old;

            if ((old = futs.putIfAbsent(cfg.getName(), fut)) != null) {
                fut = old;

                return fut;
            }

            if (!cfgCache.putxIfAbsent(new GridServiceConfigurationKey(cfg.getName()), cfg))
                fut.onDone();

            return fut;
        }
        catch (GridException e) {
            log.error("Failed to deploy service: " + cfg.getName(), e);

            return new GridFinishedFuture<>(ctx, e);
        }
    }

    /**
     * @param name Service name.
     * @return Future.
     */
    public GridFuture<?> cancel(GridProjection prj, String name) {
        return ctx.closure().broadcast(new GridClosure<String, Object>() {
            @Override public Object apply(String name) {
                Collection<GridServiceContextImpl> ctxs = localContexts(name);

                if (ctxs != null) {
                    synchronized (ctxs) {
                        cancel(ctxs, ctxs.size());
                    }
                }

                return null;
            }
        }, name, prj.nodes());
    }

    /**
     * @return Collection of service descriptors.
     */
    public Collection<? extends GridServiceDescriptor> deployedServices() {
        Collection<GridServiceDescriptor> descs = new ArrayList<>();

        for (GridServiceConfiguration cfg : cfgCache.values()) {
            GridServiceDescriptorImpl desc = new GridServiceDescriptorImpl(cfg);

            try {
                GridServiceAssignments assigns = assignCache.get(new GridServiceAssignmentsKey(cfg.getName()));

                if (assigns != null) {
                    desc.topologySnapshot(assigns.assigns());

                    descs.add(desc);
                }
            }
            catch (GridException e) {
                log.error("Failed to get assignments from replicated cache for service: " + cfg.getName(), e);
            }
        }

        return descs;
    }

    /**
     * @param name Service name.
     * @return Local contexts.
     */
    @Nullable private Collection<GridServiceContextImpl> localContexts(String name) {
        synchronized (locSvcs) {
            return locSvcs.get(name);
        }
    }

    /**
     * Reassigns service to nodes.
     *
     * @param cfg Configuration.
     * @param topVer Topology version.
     * @throws GridException If failed.
     */
    private void reassign(GridServiceConfiguration cfg, long topVer) throws GridException {
        int totalCnt = cfg.getTotalCount();
        int maxPerNodeCnt = cfg.getMaxPerNodeCount();
        String cacheName = cfg.getCacheName();
        Object affKey = cfg.getAffinityKey();

        try (GridCacheTx tx = assignCache.txStart(PESSIMISTIC, REPEATABLE_READ)) {
            GridServiceAssignmentsKey key = new GridServiceAssignmentsKey(cfg.getName());

            GridServiceAssignments oldAssigns = assignCache.get(key);

            GridServiceAssignments assigns = new GridServiceAssignments(cfg.getName(), cfg.getService(),
                cfg.getCacheName(), cfg.getAffinityKey(), topVer, cfg.getNodeFilter());

            Map<UUID, Integer> cnts = new HashMap<>();

            if (affKey != null) {
                GridNode n = ctx.affinity().mapKeyToNode(cacheName, affKey, topVer);

                assert n != null;

                int cnt = maxPerNodeCnt == 0 ? totalCnt == 0 ? 1 : totalCnt : maxPerNodeCnt;

                cnts.put(n.id(), cnt);
            }
            else {
                Collection<GridNode> nodes =
                    assigns.nodeFilter() == null ?
                        ctx.discovery().nodes(topVer) :
                        F.view(ctx.discovery().nodes(topVer), assigns.nodeFilter());

                int size = nodes.size();

                int perNodeCnt = totalCnt != 0 ? totalCnt / size : maxPerNodeCnt;
                int remainder = totalCnt != 0 ? totalCnt % size : 0;

                if (perNodeCnt > maxPerNodeCnt) {
                    perNodeCnt = maxPerNodeCnt;
                    remainder = 0;
                }

                for (GridNode n : nodes)
                    cnts.put(n.id(), perNodeCnt);

                assert perNodeCnt >= 0;
                assert remainder >= 0;

                if (remainder > 0) {
                    int cnt = perNodeCnt + 1;

                    if (oldAssigns != null) {
                        Set<UUID> used = new HashSet<>();

                        // Avoid redundant moving of services.
                        for (Map.Entry<UUID, Integer> e : oldAssigns.assigns().entrySet()) {
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
                                    e.setValue(e.getValue() + 1);

                                    if (--remainder == 0)
                                        break;
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

            assigns.assigns(cnts);

            assignCache.put(key, assigns);

            tx.commit();
         }
    }

    /**
     * Redeploys local services based on assignments.
     *
     * @param assigns Assignments.
     */
    private void redeploy(GridServiceAssignments assigns) {
        String svcName = assigns.name();

        int assignCnt = assigns.assigns().get(ctx.localNodeId());

        GridService svc = assigns.service();

        Collection<GridServiceContextImpl> ctxs;

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
                    final GridService copy = copy(svc);

                    final ExecutorService exe = Executors.newSingleThreadExecutor(threadFactory);

                    final GridServiceContextImpl ctx = new GridServiceContextImpl(assigns.name(),
                        UUID.randomUUID(), assigns.cacheName(), assigns.affinityKey(), copy, exe);

                    ctxs.add(ctx);

                    if (log.isInfoEnabled())
                        log.info("Starting service instance [name=" + ctx.name() + ", execId=" +
                            ctx.executionId() + ']');

                    // Start service in its own thread.
                    exe.submit(new Runnable() {
                        @Override public void run() {
                            try {
                                copy.execute(ctx);
                            }
                            catch (Throwable e) {
                                log.error("Service execution stopped with error [name=" + ctx.name() +
                                    ", execId=" + ctx.executionId() + ']', e);
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
    private GridService copy(GridService svc) {
        GridMarshaller m = ctx.config().getMarshaller();

        try {
            byte[] bytes = m.marshal(svc);

            return m.unmarshal(bytes, svc.getClass().getClassLoader());
        }
        catch (GridException e) {
            log.error("Failed to copy service (will reuse same instance): " + svc.getClass(), e);

            return svc;
        }
    }

    /**
     * @param ctxs Contexts to cancel.
     * @param cancelCnt Number of contexts to cancel.
     */
    private void cancel(Collection<GridServiceContextImpl> ctxs, int cancelCnt) {
        for (Iterator<GridServiceContextImpl> it = ctxs.iterator(); it.hasNext();) {
            GridServiceContextImpl ctx = it.next();

            // Flip cancelled flag.
            ctx.setCancelled(true);

            // Notify service about cancellation.
            try {
                ctx.service().cancel();
            }
            catch (Throwable e) {
                log.error("Failed to cancel service (ignoring) [name=" + ctx.name() +
                    ", execId=" + ctx.executionId() + ']', e);
            }

            it.remove();

            // Close out executor thread for the service.
            ctx.executor().shutdownNow();

            if (log.isInfoEnabled())
                log.info("Cancelled service instance [name=" + ctx.name() + ", execId=" +
                    ctx.executionId() + ']');

            if (--cancelCnt == 0)
                break;
        }
    }

    /**
     * Service deployment listener.
     */
    private class DeploymentListener implements GridLocalEventListener {
        /** {@inheritDoc} */
        @Override public void onEvent(GridEvent evt) {
            if (!busyLock.enterBusy())
                return;

            try {
                if (evt.type() == EVT_CACHE_OBJECT_PUT) {
                    Object val = ((GridCacheEvent)evt).newValue();

                    // Ignore other utility cache events.
                    if (val instanceof GridServiceConfiguration) {
                        GridServiceConfiguration cfg = (GridServiceConfiguration)val;

                        long topVer = ctx.discovery().topologyVersion();

                        GridNode oldest = U.oldest(ctx.discovery().nodes(topVer), null);

                        if (oldest.isLocal())
                            onDeployment(cfg, topVer);
                    }
                }
                // Handle undeployment.
                else if (evt.type() == EVT_CACHE_OBJECT_REMOVED) {
                    Object val = ((GridCacheEvent)evt).oldValue();

                    // Ignore other utility cache events.
                    if (val instanceof GridServiceConfiguration) {
                        GridServiceConfiguration cfg = (GridServiceConfiguration)val;

                        String svcName = cfg.getName();

                        Collection<GridServiceContextImpl> ctxs;

                        synchronized (locSvcs) {
                            ctxs = locSvcs.remove(svcName);
                        }

                        if (ctx != null) {
                            synchronized (ctxs) {
                                cancel(ctxs, ctxs.size());
                            }
                        }

                        GridFutureAdapter<?> fut = futs.get(cfg.getName());

                        // Complete deployment future if undeployment happened.
                        if (fut != null)
                            fut.onDone();
                    }
                }
            }
            finally {
                busyLock.leaveBusy();
            }
        }

        /**
         * Deployment callback.
         *
         * @param cfg Service configuration.
         * @param topVer Topology version.
         */
        private void onDeployment(final GridServiceConfiguration cfg, final long topVer) {
            if (!busyLock.enterBusy())
                return;

            // Retry forever.
            try {
                long newTopVer = ctx.discovery().topologyVersion();

                // If topology version changed, reassignment will happen from topology event.
                if (newTopVer == topVer)
                    reassign(cfg, topVer);

                GridFutureAdapter<?> fut = futs.get(cfg.getName());

                // Complete deployment futures once the assignments have been stored in cache.
                if (fut != null)
                    fut.onDone();
            }
            catch (GridException e) {
                if (!(e instanceof GridTopologyException))
                    log.error("Failed to do service reassignment (will retry): " + cfg.getName(), e);

                long newTopVer = ctx.discovery().topologyVersion();

                if (newTopVer != topVer) {
                    assert newTopVer > topVer;

                    // Reassignment will happen from topology event.
                    return;
                }

                ctx.timeout().addTimeoutObject(new GridTimeoutObject() {
                    private GridUuid id = GridUuid.randomUuid();

                    private long start = System.currentTimeMillis();

                    @Override public GridUuid timeoutId() {
                        return id;
                    }

                    @Override public long endTime() {
                        return start + RETRY_TIMEOUT;
                    }

                    @Override public void onTimeout() {
                        // Try again.
                        onDeployment(cfg, topVer);
                    }
                });
            }
            finally {
                busyLock.leaveBusy();
            }
        }
    }

    /**
     * Topology listener.
     */
    private class TopologyListener implements GridLocalEventListener {
        /** {@inheritDoc} */
        @Override public void onEvent(GridEvent evt) {
            if (!busyLock.enterBusy())
                return;

            try {
                long topVer = ((GridDiscoveryEvent)evt).topologyVersion();

                GridNode oldest = U.oldest(ctx.discovery().nodes(topVer), null);

                if (oldest.isLocal()) {
                    final Collection<GridServiceConfiguration> retries = new ConcurrentLinkedQueue<>();

                    for (GridServiceConfiguration cfg : cfgCache.values()) {
                        try {
                            reassign(cfg, topVer);
                        }
                        catch (GridException e) {
                            if (!(e instanceof GridTopologyException))
                                LT.error(log, e, "Failed to do service reassignment (will retry): " + cfg.getName());

                            retries.add(cfg);
                        }
                    }

                    if (!retries.isEmpty())
                        onReassignmentFailed(topVer, retries);
                }
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
        private void onReassignmentFailed(final long topVer, final Collection<GridServiceConfiguration> retries) {
            if (!busyLock.enterBusy())
                return;

            try {
                // If topology changed again, let next event handle it.
                if (ctx.discovery().topologyVersion() != topVer)
                    return;

                for (Iterator<GridServiceConfiguration> it = retries.iterator(); it.hasNext(); ) {
                    GridServiceConfiguration cfg = it.next();

                    try {
                        reassign(cfg, topVer);

                        it.remove();
                    }
                    catch (GridException e) {
                        if (!(e instanceof GridTopologyException))
                            LT.error(log, e, "Failed to do service reassignment (will retry): " + cfg.getName());
                    }
                }

                if (!retries.isEmpty()) {
                    ctx.timeout().addTimeoutObject(new GridTimeoutObject() {
                        private GridUuid id = GridUuid.randomUuid();

                        private long start = System.currentTimeMillis();

                        @Override public GridUuid timeoutId() {
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
    private class AssignmentListener implements GridLocalEventListener {
        /** {@inheritDoc} */
        @Override public void onEvent(GridEvent evt) {
            if (!busyLock.enterBusy())
                return;

            try {
                if (evt.type() == EVT_CACHE_OBJECT_PUT) {
                    Object val = ((GridCacheEvent)evt).newValue();

                    // Ignore other utility cache events.
                    if (val instanceof GridServiceAssignments)
                        redeploy((GridServiceAssignments)val);
                }
                // Handle undeployment.
                else if (evt.type() == EVT_CACHE_OBJECT_REMOVED) {
                    Object val = ((GridCacheEvent)evt).oldValue();

                    // Ignore other utility cache events.
                    if (val instanceof GridServiceAssignments) {
                        GridServiceAssignments assigns = (GridServiceAssignments)val;

                        String svcName = assigns.name();

                        Collection<GridServiceContextImpl> ctxs;

                        synchronized (locSvcs) {
                            ctxs = locSvcs.remove(svcName);
                        }

                        if (ctx != null) {
                            synchronized (ctxs) {
                                cancel(ctxs, ctxs.size());
                            }
                        }

                        GridFutureAdapter<?> fut = futs.get(assigns.name());

                        // Complete deployment future if undeployment happened.
                        if (fut != null)
                            fut.onDone();
                    }
                }
            }
            finally {
                busyLock.leaveBusy();
            }
        }
    }
}
