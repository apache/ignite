/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.events.*;
import org.gridgain.grid.kernal.managers.deployment.*;
import org.gridgain.grid.kernal.managers.eventstorage.*;
import org.gridgain.grid.kernal.processors.cache.query.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.util.*;
import org.gridgain.grid.util.lang.*;
import org.gridgain.grid.util.tostring.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jdk8.backport.*;
import org.jetbrains.annotations.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.*;

import static org.gridgain.grid.GridDeploymentMode.*;
import static org.gridgain.grid.events.GridEventType.*;

/**
 * Deployment manager for cache.
 */
public class GridCacheDeploymentManager<K, V> extends GridCacheManagerAdapter<K, V> {
    /** Node filter. */
    private GridPredicate<GridNode> nodeFilter;

    /** Cache class loader */
    private volatile ClassLoader globalLdr;

    /** Undeploys. */
    private final ConcurrentLinkedQueue<CA> undeploys = new ConcurrentLinkedQueue<>();

    /** Per-thread deployment context. */
    private ConcurrentMap<GridUuid, CachedDeploymentInfo<K, V>> deps =
        new ConcurrentHashMap8<>();

    /** Collection of all known participants (Node ID -> Loader ID). */
    private Map<UUID, GridUuid> allParticipants = new ConcurrentHashMap8<>();

    /** Discovery listener. */
    private GridLocalEventListener discoLsnr;

    /** Local deployment. */
    private final AtomicReference<GridDeployment> locDep = new AtomicReference<>();

    /** Local deployment ownership flag. */
    private volatile boolean locDepOwner;

    /** */
    private final GridThreadLocal<Boolean> ignoreOwnership = new GridThreadLocal<Boolean>() {
        @Override protected Boolean initialValue() {
            return false;
        }
    };

    /** */
    private boolean depEnabled;

    /** {@inheritDoc} */
    @Override public void start0() throws GridException {
        globalLdr = new CacheClassLoader();

        nodeFilter = new P1<GridNode>() {
            @Override public boolean apply(GridNode node) {
                return U.hasCache(node, cctx.namex());
            }
        };

        depEnabled = cctx.gridDeploy().enabled();

        if (depEnabled) {
            discoLsnr = new GridLocalEventListener() {
                @Override public void onEvent(GridEvent evt) {
                    assert evt.type() == EVT_NODE_FAILED || evt.type() == EVT_NODE_LEFT : "Unexpected event: " + evt;

                    UUID id = ((GridDiscoveryEvent)evt).eventNode().id();

                    if (log.isDebugEnabled())
                        log.debug("Processing node departure: " + id);

                    for (Map.Entry<GridUuid, CachedDeploymentInfo<K, V>> entry : deps.entrySet()) {
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

            cctx.events().addListener(discoLsnr, EVT_NODE_LEFT, EVT_NODE_FAILED);
        }
    }

    /** {@inheritDoc} */
    @Override protected void stop0(boolean cancel) {
        if (discoLsnr != null)
            cctx.events().removeListener(discoLsnr);
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
     * {@link #p2pContext(UUID, GridUuid, String, GridDeploymentMode, Map, boolean)} must be
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
        if (!locDepOwner && depEnabled && !ignoreOwnership.get()
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
    public void ignoreOwnership(boolean ignore) {
        ignoreOwnership.set(ignore);
    }

    /**
     * Undeploy all queued up closures.
     */
    public void unwind() {
        int cnt = 0;

        for (CA c = undeploys.poll(); c != null; c = undeploys.poll()) {
            c.apply();

            cnt++;
        }

        if (log.isDebugEnabled())
            log.debug("Unwound undeploys count: " + cnt);
    }

    /**
     * Undeploys given class loader.
     *
     * @param leftNodeId Left node ID.
     * @param ldr Class loader to undeploy.
     */
    public void onUndeploy(@Nullable final UUID leftNodeId, final ClassLoader ldr) {
        assert ldr != null;

        GridCacheAdapter<K, V> cache = cctx.cache();

        if (log.isDebugEnabled())
            log.debug("Received onUndeploy() request [ldr=" + ldr + ", cctx=" + cctx +
                ", cacheCls=" + cctx.cache().getClass().getSimpleName() + ", cacheSize=" + cache.size() + ']');

        undeploys.add(new CA() {
            @Override public void apply() {
                onUndeploy0(leftNodeId, ldr);
            }
        });

        // Unwind immediately for local and replicate caches.
        // We go through preloader for proper synchronization.
        if (cctx.isLocal() || cctx.isReplicated())
            cctx.preloader().unwindUndeploys();
    }

    /**
     * @param leftNodeId Left node ID.
     * @param ldr Loader.
     */
    private void onUndeploy0(@Nullable final UUID leftNodeId, final ClassLoader ldr) {
        GridCacheAdapter<K, V> cache = cctx.cache();

        Set<K> keySet = cache.keySet(cctx.vararg(
            new P1<GridCacheEntry<K, V>>() {
                @Override public boolean apply(GridCacheEntry<K, V> e) {
                    return cctx.isNear() ? undeploy(e, cctx.near()) || undeploy(e, cctx.near().dht()) :
                        undeploy(e, cctx.cache());
                }

                /**
                 * @param e Entry.
                 * @param cache Cache.
                 * @return {@code True} if entry should be undeployed.
                 */
                private boolean undeploy(GridCacheEntry<K, V> e, GridCacheAdapter<K, V> cache) {
                    K k = e.getKey();

                    GridCacheEntryEx<K, V> entry = cache.peekEx(e.getKey());

                    if (entry == null)
                        return false;

                    V v;

                    try {
                        v = entry.peek(GridCachePeekMode.GLOBAL, CU.<K, V>empty());
                    }
                    catch (GridCacheEntryRemovedException ignore) {
                        return false;
                    }
                    catch (GridRuntimeException ignore) {
                        // Peek can throw runtime exception if unmarshalling failed.
                        return true;
                    }

                    assert k != null : "Key cannot be null for cache entry: " + e;

                    ClassLoader keyLdr = U.detectObjectClassLoader(k);
                    ClassLoader valLdr = U.detectObjectClassLoader(v);

                    boolean res = F.eq(ldr, keyLdr) || F.eq(ldr, valLdr);

                    if (log.isDebugEnabled())
                        log.debug("Finished examining entry [entryCls=" + e.getClass() +
                            ", key=" + k + ", keyCls=" + k.getClass() +
                            ", valCls=" + (v != null ? v.getClass() : "null") +
                            ", keyLdr=" + keyLdr + ", valLdr=" + valLdr + ", res=" + res + ']');

                    return res;
                }
            }));

        Collection<K> keys = new LinkedList<>();

        for (K k : keySet)
            keys.add(k);

        if (log.isDebugEnabled())
            log.debug("Finished searching keys for undeploy [keysCnt=" + keys.size() + ']');

        cache.clearAll(keys, true);

        if (cctx.isNear())
            cctx.near().dht().clearAll(keys, true);

        GridCacheQueryManager<K, V> qryMgr = cctx.queries();

        if (qryMgr != null)
            qryMgr.onUndeploy(ldr);

        // Examine swap for entries to undeploy.
        int swapUndeployCnt = cctx.isNear() ?
            cctx.near().dht().context().swap().onUndeploy(leftNodeId, ldr) :
            cctx.swap().onUndeploy(leftNodeId, ldr);

        U.quietAndWarn(log, "");
        U.quietAndWarn(
            log,
            "Cleared all cache entries for undeployed class loader [[cacheName=" + cctx.namexx() +
                ", undeployCnt=" + keys.size() + ", swapUndeployCnt=" + swapUndeployCnt +
                ", clsLdr=" + ldr.getClass().getName() + ']',
            "Cleared all cache entries for undeployed class loader for cache: " + cctx.namexx());
        U.quietAndWarn(
            log,
            "  ^-- Cache auto-undeployment happens in SHARED deployment mode (to turn off, switch to CONTINUOUS mode)");
        U.quietAndWarn(log, "");

        // Avoid class caching issues inside classloader.
        globalLdr = new CacheClassLoader();
    }

    /**
     * @param sndId Sender node ID.
     * @param ldrId Loader ID.
     * @param userVer User version.
     * @param mode Deployment mode.
     * @param participants Node participants.
     * @param locDepOwner {@code True} if local deployment owner.
     */
    public void p2pContext(UUID sndId, GridUuid ldrId, String userVer, GridDeploymentMode mode,
        Map<UUID, GridUuid> participants, boolean locDepOwner) {
        assert depEnabled;

        if (mode == PRIVATE || mode == ISOLATED) {
            GridNode node = cctx.discovery().node(sndId);

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
                LT.warn(log, null, "Ignoring deployment in PRIVATE or ISOLATED mode " +
                    "[sndId=" + sndId + ", ldrId=" + ldrId + ", userVer=" + userVer + ", mode=" + mode +
                    ", participants=" + participants + ", daemon=" + daemon + ']');
            }

            return;
        }

        if (mode != cctx.gridConfig().getDeploymentMode()) {
            LT.warn(log, null, "Local and remote deployment mode mismatch (please fix configuration and restart) " +
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

        Map<UUID, GridUuid> added = null;

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
        GridUuid ldrId = info.classLoaderId();

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

            Map<UUID, GridUuid> participants = info.participants();

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
    @Nullable private Map<UUID, GridUuid> addGlobalParticipants(UUID sndNodeId, GridUuid sndLdrId,
        Map<UUID, GridUuid> participants, boolean locDepOwner) {
        Map<UUID, GridUuid> added = null;

        if (participants != null) {
            for (Map.Entry<UUID, GridUuid> entry : participants.entrySet()) {
                UUID nodeId = entry.getKey();
                GridUuid ldrVer = entry.getValue();

                if (!ldrVer.equals(allParticipants.get(nodeId))) {
                    allParticipants.put(nodeId, ldrVer);

                    if (added == null)
                        added = GridUtils.newHashMap(participants.size());

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
     * @throws GridException If registration failed.
     */
    public void registerClasses(Object... objs) throws GridException {
        registerClasses(F.asList(objs));
    }

    /**
     * Register local classes.
     *
     * @param objs Objects to register.
     * @throws GridException If registration failed.
     */
    public void registerClasses(Iterable<?> objs) throws GridException {
        if (objs != null)
            for (Object o : objs)
                registerClass(o);
    }

    /**
     * @param obj Object whose class to register.
     * @throws GridException If failed.
     */
    public void registerClass(Object obj) throws GridException {
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
     * @throws GridException If failed.
     */
    public void registerClass(Class<?> cls) throws GridException {
        if (cls == null)
            return;

        registerClass(cls, U.detectClassLoader(cls));
    }

    /**
     * @param cls Class to register.
     * @param ldr Class loader.
     * @throws GridException If registration failed.
     */
    public void registerClass(Class<?> cls, ClassLoader ldr) throws GridException {
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
                            throw new GridException("Encountered incompatible class loaders for cache " +
                                "[class1=" + cls.getName() + ", class2=" + dep.sampleClassName() + ']');
                    }
                    else if (locDep.compareAndSet(null, newDep))
                        break; // While loop.
                }
                else
                    throw new GridException("Failed to deploy class for local deployment [clsName=" + cls.getName() +
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
            Map<UUID, GridUuid> participants = d.participants();

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
        X.println(">>> Cache deployment manager memory stats [grid=" + cctx.gridName() +
            ", cache=" + cctx.name() + ']');
        X.println(">>>   Undeploys: " + undeploys.size());
        X.println(">>>   Cached deployments: " + deps.size());
        X.println(">>>   All participants: " + allParticipants.size());
    }

    /**
     * @param ldr Class loader to get ID for.
     * @return ID for given class loader or {@code null} if given loader is not
     *      grid deployment class loader.
     */
    @Nullable public GridUuid getClassLoaderId(@Nullable ClassLoader ldr) {
        if (ldr == null)
            return null;

        return cctx.gridDeploy().getClassLoaderId(ldr);
    }

    /**
     * @param ldrId Class loader ID.
     * @return Class loader ID or {@code null} if loader not found.
     */
    @Nullable public ClassLoader getClassLoader(GridUuid ldrId) {
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
            super(U.detectClassLoader(GridCacheDeploymentManager.class));

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
                GridUuid ldrId = t.loaderId();
                String userVer = t.userVersion();
                GridDeploymentMode mode = t.mode();
                Map<UUID, GridUuid> participants = t.participants();

                GridDeployment d = cctx.gridDeploy().getGlobalDeployment(
                    mode,
                    name,
                    name,
                    userVer,
                    sndId,
                    ldrId,
                    participants,
                    nodeFilter);

                if (d != null) {
                    Class cls = d.deployedClass(name);

                    if (cls != null)
                        return cls;
                }
            }

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
        private final GridUuid ldrId;

        /** */
        private final String userVer;

        /** */
        private final GridDeploymentMode depMode;

        /** */
        @GridToStringInclude
        private Map<UUID, GridUuid> participants;

        /** Read write lock for adding and removing participants. */
        private final ReadWriteLock participantsLock = new ReentrantReadWriteLock();

        /**
         * @param sndId Sender.
         * @param ldrId Loader ID.
         * @param userVer User version.
         * @param depMode Deployment mode.
         * @param participants Participants.
         */
        private CachedDeploymentInfo(UUID sndId, GridUuid ldrId, String userVer, GridDeploymentMode depMode,
            Map<UUID, GridUuid> participants) {
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
        boolean addParticipants(Map<UUID, GridUuid> newParticipants, GridCacheContext<K, V> cctx) {
            participantsLock.readLock().lock();

            try {
                if (participants != null && participants.isEmpty())
                    return false;

                for (Map.Entry<UUID, GridUuid> e : newParticipants.entrySet()) {
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
        Map<UUID, GridUuid> participants() {
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
        GridUuid loaderId() {
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
        public GridDeploymentMode mode() {
            return depMode;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(CachedDeploymentInfo.class, this);
        }
    }
}
