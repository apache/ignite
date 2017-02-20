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

package org.apache.ignite.internal.processors.cache;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.binary.BinaryInvalidTypeException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.DeploymentMode;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.managers.deployment.GridDeployment;
import org.apache.ignite.internal.managers.deployment.GridDeploymentInfo;
import org.apache.ignite.internal.managers.deployment.GridDeploymentInfoBean;
import org.apache.ignite.internal.managers.eventstorage.GridLocalEventListener;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearCacheAdapter;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryManager;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.lang.GridPeerDeployAware;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.CA;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;
import org.jsr166.ConcurrentHashMap8;
import org.jsr166.ConcurrentLinkedHashMap;

import static org.apache.ignite.configuration.DeploymentMode.CONTINUOUS;
import static org.apache.ignite.configuration.DeploymentMode.ISOLATED;
import static org.apache.ignite.configuration.DeploymentMode.PRIVATE;
import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;

/**
 * Deployment manager for cache.
 */
public class GridCacheDeploymentManager<K, V> extends GridCacheSharedManagerAdapter<K, V> {
    /** Cache class loader */
    private volatile ClassLoader globalLdr;

    /** Undeploys. */
    private final Map<String, List<CA>> undeploys = new HashMap<>();

    /** Per-thread deployment context. */
    private ConcurrentMap<IgniteUuid, CachedDeploymentInfo<K, V>> deps = new ConcurrentHashMap8<>();

    /** Collection of all known participants (Node ID -> Loader ID). */
    private Map<UUID, IgniteUuid> allParticipants = new ConcurrentHashMap8<>();

    /** Discovery listener. */
    private GridLocalEventListener discoLsnr;

    /** Local deployment. */
    private final AtomicReference<GridDeployment> locDep = new AtomicReference<>();

    /** Local deployment ownership flag. */
    private volatile boolean locDepOwner;

    /** */
    private final ThreadLocal<Boolean> ignoreOwnership = new ThreadLocal<Boolean>() {
        @Override protected Boolean initialValue() {
            return false;
        }
    };

    /** */
    private boolean depEnabled;

    /** {@inheritDoc} */
    @Override public void start0() throws IgniteCheckedException {
        globalLdr = new CacheClassLoader(cctx.gridConfig().getClassLoader());

        depEnabled = cctx.gridDeploy().enabled();

        if (depEnabled) {
            discoLsnr = new GridLocalEventListener() {
                @Override public void onEvent(Event evt) {
                    assert evt.type() == EVT_NODE_FAILED || evt.type() == EVT_NODE_LEFT : "Unexpected event: " + evt;

                    UUID id = ((DiscoveryEvent)evt).eventNode().id();

                    if (log.isDebugEnabled())
                        log.debug("Processing node departure: " + id);

                    for (Map.Entry<IgniteUuid, CachedDeploymentInfo<K, V>> entry : deps.entrySet()) {
                        CachedDeploymentInfo<K, V> d = entry.getValue();

                        if (log.isDebugEnabled())
                            log.debug("Examining cached info: " + d);

                        if (d.senderId().equals(id) || d.removeParticipant(id)) {
                            deps.remove(entry.getKey(), d);

                            if (log.isDebugEnabled())
                                log.debug("Removed cached info [d=" + d + ", deps=" + deps + ']');
                        }
                    }

                    allParticipants.remove(id);
                }
            };

            cctx.gridEvents().addLocalEventListener(discoLsnr, EVT_NODE_LEFT, EVT_NODE_FAILED);
        }
    }

    /** {@inheritDoc} */
    @Override protected void stop0(boolean cancel) {
        if (discoLsnr != null)
            cctx.gridEvents().removeLocalEventListener(discoLsnr);
    }

    /**
     * @return Local-only class loader.
     */
    public ClassLoader localLoader() {
        GridDeployment dep = locDep.get();

        return dep == null ? U.gridClassLoader() : dep.classLoader();
    }

    /**
     * Gets distributed class loader. Note that
     * {@link #p2pContext(UUID, IgniteUuid, String, DeploymentMode, Map, boolean)} must be
     * called from the same thread prior to using this class loader, or the
     * loading may happen for the wrong node or context.
     *
     * @return Cache class loader.
     */
    public ClassLoader globalLoader() {
        return globalLdr;
    }

    /**
     * Callback on method enter.
     */
    public void onEnter() {
        if (depEnabled && !locDepOwner && !ignoreOwnership.get()
            && !cctx.kernalContext().job().internal()) {
            ClassLoader ldr = Thread.currentThread().getContextClassLoader();

            // We mark node as deployment owner if accessing cache not from p2p deployed job
            // and not from internal job.
            if (!U.p2pLoader(ldr))
                // If not deployment class loader, classes can be loaded from this node.
                locDepOwner = true;
        }
    }

    /**
     * @param ignore {@code True} to ignore.
     */
    public boolean ignoreOwnership(boolean ignore) {
        boolean old = ignoreOwnership.get();

        ignoreOwnership.set(ignore);

        return old;
    }

    /**
     * Undeploy all queued up closures.
     *
     * @param ctx Cache context.
     */
    public void unwind(GridCacheContext ctx) {
        List<CA> q;

        synchronized (undeploys) {
            q = undeploys.remove(ctx.name());
        }

        if (q == null)
            return;

        int cnt = 0;

        for (CA c : q) {
            c.apply();

            cnt++;
        }

        if (log.isDebugEnabled())
            log.debug("Unwound undeploys count: " + cnt);
    }

    /**
     * Undeploys given class loader.
     *
     * @param ldr Class loader to undeploy.
     * @param ctx Grid cache context.
     */
    public void onUndeploy(final ClassLoader ldr, final GridCacheContext<K, V> ctx) {
        assert ldr != null;

        if (log.isDebugEnabled())
            log.debug("Received onUndeploy() request [ldr=" + ldr + ", cctx=" + cctx + ']');

        synchronized (undeploys) {
            List<CA> queue = undeploys.get(ctx.name());

            if (queue == null)
                undeploys.put(ctx.name(), queue = new ArrayList<>());

            queue.add(new CA() {
                @Override public void apply() {
                    onUndeploy0(ldr, ctx);
                }
            });
        }

        // Unwind immediately for local and replicate caches.
        // We go through preloader for proper synchronization.
        if (ctx.isLocal())
            ctx.preloader().unwindUndeploys();
    }

    /**
     * @param ldr Loader.
     * @param cacheCtx Cache context.
     */
    private void onUndeploy0(final ClassLoader ldr, final GridCacheContext<K, V> cacheCtx) {
        GridCacheAdapter<K, V> cache = cacheCtx.cache();

        Collection<KeyCacheObject> keys = new ArrayList<>();

        addEntries(ldr, keys, cache);

        if (cache.isNear())
            addEntries(ldr, keys, (((GridNearCacheAdapter)cache).dht()));

        if (log.isDebugEnabled())
            log.debug("Finished searching keys for undeploy [keysCnt=" + keys.size() + ']');

        cache.clearLocally(keys, true);

        if (cacheCtx.isNear())
            cacheCtx.near().dht().clearLocally(keys, true);

        GridCacheQueryManager<K, V> qryMgr = cacheCtx.queries();

        if (qryMgr != null)
            qryMgr.onUndeploy(ldr);

        // Examine swap for entries to undeploy.
        int swapUndeployCnt = cacheCtx.isNear() ?
            cacheCtx.near().dht().context().swap().onUndeploy(ldr) :
            cacheCtx.swap().onUndeploy(ldr);

        if (cacheCtx.userCache() && (!keys.isEmpty() || swapUndeployCnt != 0)) {
            U.quietAndWarn(log, "");
            U.quietAndWarn(
                log,
                "Cleared all cache entries for undeployed class loader [cacheName=" + cacheCtx.namexx() +
                    ", undeployCnt=" + keys.size() + ", swapUndeployCnt=" + swapUndeployCnt +
                    ", clsLdr=" + ldr.getClass().getName() + ']');
            U.quietAndWarn(
                log,
                "  ^-- Cache auto-undeployment happens in SHARED deployment mode " +
                    "(to turn off, switch to CONTINUOUS mode)");
            U.quietAndWarn(log, "");
        }

        // Avoid class caching issues inside classloader.
        globalLdr = new CacheClassLoader();
    }

    /**
     * @param ldr Class loader.
     * @param keys Keys.
     * @param cache Cache.
     */
    private void addEntries(ClassLoader ldr, Collection<KeyCacheObject> keys, GridCacheAdapter cache) {
        GridCacheContext cacheCtx = cache.context();

        for (GridCacheEntryEx e : (Iterable<GridCacheEntryEx>)cache.entries()) {
            boolean undeploy = cacheCtx.isNear() ?
                undeploy(ldr, e, cacheCtx.near()) || undeploy(ldr, e, cacheCtx.near().dht()) :
                undeploy(ldr, e, cacheCtx.cache());

            if (undeploy)
                keys.add(e.key());
        }
    }

    /**
     * @param ldr Class loader.
     * @param e Entry.
     * @param cache Cache.
     * @return {@code True} if need to undeploy.
     */
    private boolean undeploy(ClassLoader ldr, GridCacheEntryEx e, GridCacheAdapter cache) {
        KeyCacheObject key = e.key();

        GridCacheEntryEx entry = cache.peekEx(key);

        if (entry == null)
            return false;

        Object key0;
        Object val0;

        try {
            CacheObject v = entry.peek(true, false, false, null);

            key0 = key.value(cache.context().cacheObjectContext(), false);

            assert key0 != null : "Key cannot be null for cache entry: " + e;

            val0 = CU.value(v, cache.context(), false);
        }
        catch (GridCacheEntryRemovedException ignore) {
            return false;
        }
        catch (BinaryInvalidTypeException ex) {
            log.error("An attempt to undeploy cache with binary objects.", ex);

            return false;
        }
        catch (IgniteCheckedException | IgniteException ignore) {
            // Peek can throw runtime exception if unmarshalling failed.
            return true;
        }

        ClassLoader keyLdr = U.detectObjectClassLoader(key0);
        ClassLoader valLdr = U.detectObjectClassLoader(val0);

        boolean res = F.eq(ldr, keyLdr) || F.eq(ldr, valLdr);

        if (log.isDebugEnabled())
            log.debug("Finished examining entry [entryCls=" + e.getClass() +
                ", key=" + key0 + ", keyCls=" + key0.getClass() +
                ", valCls=" + (val0 != null ? val0.getClass() : "null") +
                ", keyLdr=" + keyLdr + ", valLdr=" + valLdr + ", res=" + res + ']');

        return res;
    }

    /**
     * @param sndId Sender node ID.
     * @param ldrId Loader ID.
     * @param userVer User version.
     * @param mode Deployment mode.
     * @param participants Node participants.
     * @param locDepOwner {@code True} if local deployment owner.
     */
    public void p2pContext(UUID sndId, IgniteUuid ldrId, String userVer, DeploymentMode mode,
        Map<UUID, IgniteUuid> participants, boolean locDepOwner) {
        assert depEnabled;

        if (mode == PRIVATE || mode == ISOLATED) {
            ClusterNode node = cctx.discovery().node(sndId);

            if (node == null) {
                if (log.isDebugEnabled())
                    log.debug("Ignoring p2p context (sender has left) [sndId=" + sndId + ", ldrId=" +  ldrId +
                        ", userVer=" + userVer + ", mode=" + mode + ", participants=" + participants + ']');

                return;
            }

            boolean daemon = node.isDaemon();

            // Always output in debug.
            if (log.isDebugEnabled())
                log.debug("Ignoring deployment in PRIVATE or ISOLATED mode [sndId=" + sndId + ", ldrId=" + ldrId +
                    ", userVer=" + userVer + ", mode=" + mode + ", participants=" + participants +
                    ", daemon=" + daemon + ']');

            if (!daemon) {
                LT.warn(log, "Ignoring deployment in PRIVATE or ISOLATED mode " +
                    "[sndId=" + sndId + ", ldrId=" + ldrId + ", userVer=" + userVer + ", mode=" + mode +
                    ", participants=" + participants + ", daemon=" + daemon + ']');
            }

            return;
        }

        if (mode != cctx.gridConfig().getDeploymentMode()) {
            LT.warn(log, "Local and remote deployment mode mismatch (please fix configuration and restart) " +
                "[locDepMode=" + cctx.gridConfig().getDeploymentMode() + ", rmtDepMode=" + mode + ", rmtNodeId=" +
                sndId + ']');

            return;
        }

        if (log.isDebugEnabled())
            log.debug("Setting p2p context [sndId=" + sndId + ", ldrId=" +  ldrId + ", userVer=" + userVer +
                ", seqNum=" + ldrId.localId() + ", mode=" + mode + ", participants=" + participants +
                ", locDepOwner=" + locDepOwner + ']');

        CachedDeploymentInfo<K, V> depInfo;

        while (true) {
            depInfo = deps.get(ldrId);

            if (depInfo == null) {
                depInfo = new CachedDeploymentInfo<>(sndId, ldrId, userVer, mode, participants);

                CachedDeploymentInfo<K, V> old = deps.putIfAbsent(ldrId, depInfo);

                if (old != null)
                    depInfo = old;
                else
                    break;
            }

            if (participants != null) {
                if (!depInfo.addParticipants(participants, cctx)) {
                    deps.remove(ldrId, depInfo);

                    continue;
                }
            }

            break;
        }

        Map<UUID, IgniteUuid> added = null;

        if (locDepOwner)
            added = addGlobalParticipants(sndId, ldrId, participants, locDepOwner);

        if (cctx.discovery().node(sndId) == null) {
            // Sender has left.
            deps.remove(ldrId, depInfo);

            if (added != null)
                added.remove(sndId);

            allParticipants.remove(sndId);
        }

        if (participants != null) {
            for (UUID id : participants.keySet()) {
                if (cctx.discovery().node(id) == null) {
                    if (depInfo.removeParticipant(id))
                        deps.remove(ldrId, depInfo);

                    if (added != null)
                        added.remove(id);

                    allParticipants.remove(id);
                }
            }
        }

        if (added != null && !added.isEmpty())
            cctx.gridDeploy().addCacheParticipants(allParticipants, added);
    }

    /**
     * Adds deployment info to deployment contexts queue.
     *
     * @param info Info to add.
     */
    public void addDeploymentContext(GridDeploymentInfo info) {
        IgniteUuid ldrId = info.classLoaderId();

        while (true) {
            CachedDeploymentInfo<K, V> depInfo = deps.get(ldrId);

            if (depInfo == null) {
                depInfo = new CachedDeploymentInfo<>(ldrId.globalId(), ldrId, info.userVersion(), info.deployMode(),
                    info.participants());

                CachedDeploymentInfo<K, V> old = deps.putIfAbsent(ldrId, depInfo);

                if (old != null)
                    depInfo = old;
                else
                    break;
            }

            Map<UUID, IgniteUuid> participants = info.participants();

            if (participants != null) {
                if (!depInfo.addParticipants(participants, cctx)) {
                    deps.remove(ldrId, depInfo);

                    continue;
                }
            }

            break;
        }
    }

    /**
     * @param sndNodeId Sender node ID.
     * @param sndLdrId Sender loader ID.
     * @param participants Participants.
     * @param locDepOwner {@code True} if local deployment owner.
     * @return Added participants.
     */
    @Nullable private Map<UUID, IgniteUuid> addGlobalParticipants(UUID sndNodeId, IgniteUuid sndLdrId,
        Map<UUID, IgniteUuid> participants, boolean locDepOwner) {
        Map<UUID, IgniteUuid> added = null;

        if (participants != null) {
            for (Map.Entry<UUID, IgniteUuid> entry : participants.entrySet()) {
                UUID nodeId = entry.getKey();
                IgniteUuid ldrVer = entry.getValue();

                if (!ldrVer.equals(allParticipants.get(nodeId))) {
                    allParticipants.put(nodeId, ldrVer);

                    if (added == null)
                        added = IgniteUtils.newHashMap(participants.size());

                    added.put(nodeId, ldrVer);
                }
            }
        }

        if (locDepOwner) {
            assert sndNodeId != null;
            assert sndLdrId != null;

            if (!sndLdrId.equals(allParticipants.get(sndNodeId))) {
                allParticipants.put(sndNodeId, sndLdrId);

                if (added == null)
                    added = U.newHashMap(1);

                added.put(sndNodeId, sndLdrId);
            }
        }

        return added;
    }

    /**
     * Register local classes.
     *
     * @param objs Objects to register.
     * @throws IgniteCheckedException If registration failed.
     */
    public void registerClasses(Object... objs) throws IgniteCheckedException {
        registerClasses(F.asList(objs));
    }

    /**
     * Register local classes.
     *
     * @param objs Objects to register.
     * @throws IgniteCheckedException If registration failed.
     */
    public void registerClasses(Iterable<?> objs) throws IgniteCheckedException {
        if (objs != null)
            for (Object o : objs)
                registerClass(o);
    }

    /**
     * @param obj Object whose class to register.
     * @throws IgniteCheckedException If failed.
     */
    public void registerClass(Object obj) throws IgniteCheckedException {
        if (obj == null)
            return;

        if (obj instanceof GridPeerDeployAware) {
            GridPeerDeployAware p = (GridPeerDeployAware)obj;

            registerClass(p.deployClass(), p.classLoader());
        }
        else
            registerClass(obj instanceof Class ? (Class)obj : obj.getClass());
    }

    /**
     * @param cls Class to register.
     * @throws IgniteCheckedException If failed.
     */
    public void registerClass(Class<?> cls) throws IgniteCheckedException {
        if (cls == null)
            return;

        registerClass(cls, U.detectClassLoader(cls));
    }

    /**
     * @param cls Class to register.
     * @param ldr Class loader.
     * @throws IgniteCheckedException If registration failed.
     */
    public void registerClass(Class<?> cls, ClassLoader ldr) throws IgniteCheckedException {
        assert cctx.deploymentEnabled();

        if (cls == null || GridCacheInternal.class.isAssignableFrom(cls))
            return;

        if (ldr == null)
            ldr = U.detectClassLoader(cls);

        // Don't register remote class loaders.
        if (U.p2pLoader(ldr))
            return;

        GridDeployment dep = locDep.get();

        if (dep == null || (!ldr.equals(dep.classLoader()) && !U.hasParent(ldr, dep.classLoader()))) {
            while (true) {
                dep = locDep.get();

                // Don't register remote class loaders.
                if (dep != null && !dep.local())
                    return;

                if (dep != null) {
                    ClassLoader curLdr = dep.classLoader();

                    if (curLdr.equals(ldr))
                        break;

                    // If current deployment is either system loader or GG loader,
                    // then we don't check it, as new loader is most likely wider.
                    if (!curLdr.equals(U.gridClassLoader()) && dep.deployedClass(cls.getName()) != null)
                        // Local deployment can load this class already, so no reason
                        // to look for another class loader.
                        break;
                }

                GridDeployment newDep = cctx.gridDeploy().deploy(cls, ldr);

                if (newDep != null) {
                    if (dep != null) {
                        // Check new deployment.
                        if (newDep.deployedClass(dep.sampleClassName()) != null) {
                            if (locDep.compareAndSet(dep, newDep))
                                break; // While loop.
                        }
                        else
                            throw new IgniteCheckedException("Encountered incompatible class loaders for cache " +
                                "[class1=" + cls.getName() + ", class2=" + dep.sampleClassName() + ']');
                    }
                    else if (locDep.compareAndSet(null, newDep))
                        break; // While loop.
                }
                else
                    throw new IgniteCheckedException("Failed to deploy class for local deployment [clsName=" + cls.getName() +
                        ", ldr=" + ldr + ']');
            }
        }
    }

    /**
     * Prepares deployable object.
     *
     * @param deployable Deployable object.
     */
    public void prepare(GridCacheDeployable deployable) {
        assert depEnabled;

        // Only set deployment info if it was not set automatically.
        if (deployable.deployInfo() == null) {
            GridDeploymentInfoBean dep = globalDeploymentInfo();

            if (dep == null) {
                GridDeployment locDep0 = locDep.get();

                if (locDep0 != null) {
                    // Will copy sequence number to bean.
                    dep = new GridDeploymentInfoBean(locDep0);

                    dep.localDeploymentOwner(locDepOwner);
                }
            }

            if (dep != null)
                deployable.prepare(dep);

            if (log.isDebugEnabled())
                log.debug("Prepared grid cache deployable [dep=" + dep + ", deployable=" + deployable + ']');
        }
    }

    /**
     * @return First global deployment.
     */
    @Nullable public GridDeploymentInfoBean globalDeploymentInfo() {
        assert depEnabled;

        if (locDepOwner)
            return null;

        // Do not return info if mode is CONTINUOUS.
        // In this case deployment info will be set by GridCacheMessage.prepareObject().
        if (cctx.gridConfig().getDeploymentMode() == CONTINUOUS)
            return null;

        for (CachedDeploymentInfo<K, V> d : deps.values()) {
            if (cctx.discovery().node(d.senderId()) == null)
                // Sender has left.
                continue;

            // Participants map.
            Map<UUID, IgniteUuid> participants = d.participants();

            if (participants != null) {
                for (UUID id : participants.keySet()) {
                    if (cctx.discovery().node(id) != null) {
                        // At least 1 participant is still in the grid.
                        return new GridDeploymentInfoBean(d.loaderId(), d.userVersion(), d.mode(),
                            participants, locDepOwner);
                    }
                }
            }
        }

        return null;
    }

    /** {@inheritDoc} */
    @Override public void printMemoryStats() {
        X.println(">>> ");
        X.println(">>> Cache deployment manager memory stats [grid=" + cctx.gridName() + ']');
        X.println(">>>   Undeploys: " + undeploys.size());
        X.println(">>>   Cached deployments: " + deps.size());
        X.println(">>>   All participants: " + allParticipants.size());
    }

    /**
     * @param ldr Class loader to get ID for.
     * @return ID for given class loader or {@code null} if given loader is not
     *      grid deployment class loader.
     */
    @Nullable public IgniteUuid getClassLoaderId(@Nullable ClassLoader ldr) {
        if (ldr == null)
            return null;

        return cctx.gridDeploy().getClassLoaderId(ldr);
    }

    /**
     * @param ldrId Class loader ID.
     * @return Class loader ID or {@code null} if loader not found.
     */
    @Nullable public ClassLoader getClassLoader(IgniteUuid ldrId) {
        assert ldrId != null;

        GridDeployment dep = cctx.gridDeploy().getDeployment(ldrId);

        return dep != null ? dep.classLoader() : null;
    }

    /**
     * @return {@code True} if context class loader is global.
     */
    public boolean isGlobalLoader() {
        return cctx.gridDeploy().isGlobalLoader(Thread.currentThread().getContextClassLoader());
    }

    /**
     * Cache class loader.
     */
    private class CacheClassLoader extends ClassLoader {
        /** */
        private final String[] p2pExclude;

        /**
         * Sets context class loader as parent.
         */
        private CacheClassLoader() {
            this(U.detectClassLoader(GridCacheDeploymentManager.class));
        }

        /**
         * Sets context class loader.
         * If user's class loader is null then will be used default class loader.
         *
         * @param classLdr User's class loader.
         */
        private CacheClassLoader(ClassLoader classLdr) {
            super(classLdr != null ? classLdr : U.detectClassLoader(GridCacheDeploymentManager.class));

            p2pExclude = cctx.gridConfig().getPeerClassLoadingLocalClassPathExclude();
        }

        /** {@inheritDoc} */
        @Override public Class<?> loadClass(String name) throws ClassNotFoundException {
            // Always delegate to deployment manager.
            return findClass(name);
        }

        /** {@inheritDoc} */
        @SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
        @Override protected Class<?> findClass(String name) throws ClassNotFoundException {
            // Try local deployment first.
            if (!isLocallyExcluded(name)) {
                GridDeployment d = cctx.gridDeploy().getLocalDeployment(name);

                if (d != null) {
                    Class cls = d.deployedClass(name);

                    if (cls != null)
                        return cls;
                }
            }

            for (CachedDeploymentInfo<K, V> t : deps.values()) {
                UUID sndId = t.senderId();
                IgniteUuid ldrId = t.loaderId();
                String userVer = t.userVersion();
                DeploymentMode mode = t.mode();
                Map<UUID, IgniteUuid> participants = t.participants();

                GridDeployment d = cctx.gridDeploy().getGlobalDeployment(
                    mode,
                    name,
                    name,
                    userVer,
                    sndId,
                    ldrId,
                    participants,
                    F.<ClusterNode>alwaysTrue());

                if (d != null) {
                    Class cls = d.deployedClass(name);

                    if (cls != null)
                        return cls;
                }
            }

            Class cls = getParent().loadClass(name);

            if (cls != null)
                return cls;

            throw new ClassNotFoundException("Failed to load class [name=" + name+ ", ctx=" + deps + ']');
        }

        /**
         * @param name Name of the class.
         * @return {@code True} if locally excluded.
         */
        private boolean isLocallyExcluded(String name) {
            if (p2pExclude != null) {
                for (String path : p2pExclude) {
                    // Remove star (*) at the end.
                    if (path.endsWith("*"))
                        path = path.substring(0, path.length() - 1);

                    if (name.startsWith(path))
                        return true;
                }
            }

            return false;
        }
    }

    /**
     *
     */
    private static class CachedDeploymentInfo<K, V> {
        /** */
        private final UUID sndId;

        /** */
        private final IgniteUuid ldrId;

        /** */
        private final String userVer;

        /** */
        private final DeploymentMode depMode;

        /** */
        @GridToStringInclude
        private Map<UUID, IgniteUuid> participants;

        /** Read write lock for adding and removing participants. */
        private final ReadWriteLock participantsLock = new ReentrantReadWriteLock();

        /**
         * @param sndId Sender.
         * @param ldrId Loader ID.
         * @param userVer User version.
         * @param depMode Deployment mode.
         * @param participants Participants.
         */
        private CachedDeploymentInfo(UUID sndId, IgniteUuid ldrId, String userVer, DeploymentMode depMode,
            Map<UUID, IgniteUuid> participants) {
            assert sndId.equals(ldrId.globalId()) || participants != null;

            this.sndId = sndId;
            this.ldrId = ldrId;
            this.userVer = userVer;
            this.depMode = depMode;
            this.participants = participants == null || participants.isEmpty() ? null :
                new ConcurrentLinkedHashMap<>(participants);
        }

        /**
         * @param newParticipants Participants to add.
         * @param cctx Cache context.
         * @return {@code True} if cached info is valid.
         */
        boolean addParticipants(Map<UUID, IgniteUuid> newParticipants, GridCacheSharedContext<K, V> cctx) {
            participantsLock.readLock().lock();

            try {
                if (participants != null && participants.isEmpty())
                    return false;

                for (Map.Entry<UUID, IgniteUuid> e : newParticipants.entrySet()) {
                    assert e.getKey().equals(e.getValue().globalId());

                    if (cctx.discovery().node(e.getKey()) != null)
                        // Participant has left.
                        continue;

                    if (participants == null)
                        participants = new ConcurrentLinkedHashMap<>();

                    if (!participants.containsKey(e.getKey()))
                        participants.put(e.getKey(), e.getValue());
                }

                return true;
            }
            finally {
                participantsLock.readLock().unlock();
            }
        }

        /**
         * @param leftNodeId Left node ID.
         * @return {@code True} if participant has been removed and there are no participants left.
         */
        boolean removeParticipant(UUID leftNodeId) {
            assert leftNodeId != null;

            participantsLock.writeLock().lock();

            try {
                return participants != null && participants.remove(leftNodeId) != null && participants.isEmpty();
            }
            finally {
                participantsLock.writeLock().unlock();
            }
        }

        /**
         * @return Participants.
         */
        Map<UUID, IgniteUuid> participants() {
            return participants;
        }

        /**
         * @return Sender ID.
         */
        UUID senderId() {
            return sndId;
        }

        /**
         * @return Class loader ID.
         */
        IgniteUuid loaderId() {
            return ldrId;
        }

        /**
         * @return User version.
         */
        String userVersion() {
            return userVer;
        }

        /**
         * @return Deployment mode.
         */
        public DeploymentMode mode() {
            return depMode;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(CachedDeploymentInfo.class, this);
        }
    }
}
