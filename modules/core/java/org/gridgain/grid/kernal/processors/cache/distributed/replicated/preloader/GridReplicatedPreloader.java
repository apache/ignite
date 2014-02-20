// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.distributed.replicated.preloader;

import org.gridgain.grid.*;
import org.gridgain.grid.events.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.util.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.grid.util.future.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.*;

import static org.gridgain.grid.events.GridEventType.*;
import static org.gridgain.grid.cache.GridCachePreloadMode.*;
import static org.gridgain.grid.util.GridConcurrentFactory.*;

/**
 * Class that takes care about entries preloading in replicated cache.
 *
 * @author @java.author
 * @version @java.version
 */
public class GridReplicatedPreloader<K, V> extends GridCachePreloaderAdapter<K, V> {
    /** Busy lock to control activeness of threads (loader, sender). */
    private final ReadWriteLock busyLock = new ReentrantReadWriteLock();

    /** Future to wait for the end of preloading on. */
    private final GridFutureAdapter<?> syncPreloadFut = new GridFutureAdapter(cctx.kernalContext());

    /** Lock to prevent preloading for the time of eviction. */
    private final ReentrantReadWriteLock evictLock = new ReentrantReadWriteLock();

    /** Supply pool. */
    private GridReplicatedPreloadSupplyPool<K, V> supplyPool;

    /** Demand pool. */
    private GridReplicatedPreloadDemandPool<K, V> demandPool;

    /** Eviction history. */
    private volatile Map<K, GridCacheVersion> evictHist = new HashMap<>();

    /** Preloading flag. */
    private AtomicBoolean preloadStarted = new AtomicBoolean();

    /** Force key futures. */
    private final ConcurrentMap<GridUuid, GridReplicatedForceKeysFuture<K, V>> forceKeyFuts = newMap();

    /** Discovery listener. */
    private final GridLocalEventListener discoLsnr = new GridLocalEventListener() {
        @Override public void onEvent(GridEvent evt) {
            GridDiscoveryEvent e = (GridDiscoveryEvent)evt;

            assert e.type() == EVT_NODE_LEFT || e.type() == EVT_NODE_FAILED;

            for (GridReplicatedForceKeysFuture<K, V> f : forceKeyFuts.values())
                f.onNodeLeft(e.eventNodeId());
        }
    };

    /**
     * @param cctx Cache context.
     */
    public GridReplicatedPreloader(GridCacheContext<K, V> cctx) {
        super(cctx);
    }

    /** {@inheritDoc} */
    @Override public void start() throws GridException {
        demandPool = new GridReplicatedPreloadDemandPool<>(cctx, busyLock, evictLock,
            new P1<GridCacheEntryInfo<K, V>>() {
                @Override public boolean apply(GridCacheEntryInfo<K, V> info) {
                    return preloadingPermitted(info.key(), info.version());
                }
            });

        supplyPool = new GridReplicatedPreloadSupplyPool<>(cctx,
            new PA() {
                @Override public boolean apply() {
                    return syncPreloadFut.isDone();
                }
            }, busyLock);

        cctx.io().addHandler(GridReplicatedForceKeysRequest.class,
            new CI2<UUID, GridReplicatedForceKeysRequest<K, V>>() {
                @Override public void apply(UUID nodeId, GridReplicatedForceKeysRequest<K, V> req) {
                    processForceKeyRequest(nodeId, req);
                }
            });

        cctx.io().addHandler(GridReplicatedForceKeysResponse.class,
            new CI2<UUID, GridReplicatedForceKeysResponse<K, V>>() {
                @Override public void apply(UUID nodeId, GridReplicatedForceKeysResponse<K, V> res) {
                    processForceKeyResponse(nodeId, res);
                }
            });

        cctx.events().addListener(discoLsnr, EVT_NODE_LEFT, EVT_NODE_FAILED);
    }

    /**
     * @throws GridException In case of error.
     */
    @Override public void onKernalStart() throws GridException {
        if (cctx.preloadEnabled()) {
            if (log.isDebugEnabled())
                log.debug("Creating initial assignments.");

            createAssignments(EVT_NODE_JOINED, syncPreloadFut);
        }

        supplyPool.start();
        demandPool.start();

        // Clear eviction history on preload finish.
        syncPreloadFut.listenAsync(
            new CIX1<GridFuture<?>>() {
                @Override public void applyx(GridFuture<?> gridFut) {
                    // Wait until all eviction activities finish.
                    evictLock.writeLock().lock();

                    try {
                        evictHist = null;
                    }
                    finally {
                        evictLock.writeLock().unlock();
                    }
                }
            }
        );

        if (cctx.config().getPreloadMode() == SYNC) {
            U.log(log, "Starting preloading in SYNC mode: " + cctx.name());

            final long start = U.currentTimeMillis();

            syncPreloadFut.listenAsync(new CI1<GridFuture<?>>() {
                @Override public void apply(GridFuture<?> t) {
                    U.log(log, "Completed preloading in SYNC mode [name=" + cctx.name() + ", time=" +
                        (U.currentTimeMillis() - start) + " ms]");
                }
            });
        }

        if (!cctx.preloadEnabled())
            syncPreloadFut.onDone();
    }

    /** {@inheritDoc} */
    @Override public void preloadPredicate(GridPredicate<GridCacheEntryInfo<K, V>> preloadPred) {
        super.preloadPredicate(preloadPred);

        assert demandPool != null && supplyPool != null : "preloadPredicate may be called only after start()";

        demandPool.preloadPredicate(preloadPred);
        supplyPool.preloadPredicate(preloadPred);
    }

    /**
     * @param discoEvtType Corresponding discovery event.
     * @param finishFut Finish future for assignments.
     */
    void createAssignments(final int discoEvtType, GridFutureAdapter<?> finishFut) {
        assert cctx.preloadEnabled();
        assert finishFut != null;

        if (!preloadStarted.compareAndSet(false, true)) {
            if (log.isDebugEnabled())
                log.debug("Ignoring preload request since it already started or in progress.");

            return;
        }

        long maxOrder = cctx.localNode().order() - 1; // Preload only from elder nodes.

        Collection<GridReplicatedPreloadAssignment> assigns = new LinkedList<>();

        Collection<GridNode> rmts = CU.allNodes(cctx, maxOrder);

        if (!rmts.isEmpty()) {
            for (int part : partitions(cctx.localNode())) {
                Collection<GridNode> partNodes = cctx.affinity().nodes(part, maxOrder);

                int cnt = partNodes.size();

                assert cnt > 0;

                for (int mod = 0; mod < cnt; mod++) {
                    GridReplicatedPreloadAssignment assign =
                        new GridReplicatedPreloadAssignment(part, mod, cnt);

                    assigns.add(assign);

                    if (log.isDebugEnabled())
                        log.debug("Created assignment: " + assign);
                }
            }
        }

        if (cctx.events().isRecordable(EVT_CACHE_PRELOAD_STARTED))
            cctx.events().addPreloadEvent(-1, EVT_CACHE_PRELOAD_STARTED, cctx.discovery().shadow(cctx.localNode()),
                discoEvtType, cctx.localNode().metrics().getNodeStartTime());

        if (!assigns.isEmpty())
            demandPool.assign(assigns, finishFut, maxOrder);
        else
            finishFut.onDone();

        // Preloading stopped event notification.
        if (cctx.events().isRecordable(EVT_CACHE_PRELOAD_STOPPED)) {
            finishFut.listenAsync(
                new CIX1<GridFuture<?>>() {
                    @Override public void applyx(GridFuture<?> gridFut) {
                        cctx.events().addPreloadEvent(-1, EVT_CACHE_PRELOAD_STOPPED,
                            cctx.discovery().shadow(cctx.localNode()),
                            discoEvtType, cctx.localNode().metrics().getNodeStartTime());
                    }
                }
            );
        }
    }

    /**
     * @param node Node.
     * @return Collection of partition numbers for the node.
     */
    Set<Integer> partitions(GridNode node) {
        assert node != null;

        Set<Integer> parts = new HashSet<>();

        long topVer = cctx.discovery().topologyVersion();
        int partCnt = cctx.config().getAffinity().partitions();

        for (int i = 0; i < partCnt; i++)
            if (cctx.affinity().nodes(i, topVer).contains(node))
                parts.add(i);

        return parts;
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"LockAcquiredButNotSafelyReleased"})
    @Override public void onKernalStop() {
        if (log.isDebugEnabled())
            log.debug("Replicated preloader onKernalStop callback.");

        cctx.events().removeListener(discoLsnr);

        // Acquire write lock.
        busyLock.writeLock().lock();

        if (supplyPool != null)
            supplyPool.stop();

        if (demandPool != null)
            demandPool.stop();

        if (log.isDebugEnabled())
            log.debug("Replicated preloader has been stopped.");
    }

    /** {@inheritDoc} */
    @Override public GridFuture<?> startFuture() {
        return cctx.config().getPreloadMode() != SYNC ? new GridFinishedFuture(cctx.kernalContext()) : syncPreloadFut;
    }

    /** {@inheritDoc} */
    @Override public GridFuture<?> syncFuture() {
        return syncPreloadFut;
    }

    /**
     * Acquires lock for evictions to proceed (this makes preloading impossible).
     *
     * @return {@code True} if lock was acuired.
     */
    @SuppressWarnings({"LockAcquiredButNotSafelyReleased"})
    public boolean lock() {
        if (!syncPreloadFut.isDone()) {
            evictLock.writeLock().lock();

            return true;
        }

        return false;
    }

    /**
     * Makes preloading possible.
     */
    public void unlock() {
        evictLock.writeLock().unlock();
    }

    /**
     * @param key Key.
     * @param ver Version.
     */
    public void onEntryEvicted(K key, GridCacheVersion ver) {
        assert key != null;
        assert ver != null;
        assert evictLock.isWriteLockedByCurrentThread(); // Only one thread can enter this method at a time.

        if (syncPreloadFut.isDone())
            // Ignore since preloading finished.
            return;

        Map<K, GridCacheVersion> evictHist0 = evictHist;

        assert evictHist0 != null;

        GridCacheVersion ver0 = evictHist0.get(key);

        if (ver0 == null || ver0.isLess(ver)) {
            GridCacheVersion ver1 = evictHist0.put(key, ver);

            assert ver1 == ver0;
        }
    }

    /**
     * @param key Key.
     * @param ver Version.
     * @return {@code True} if preloading is permitted.
     */
    public boolean preloadingPermitted(K key, GridCacheVersion ver) {
        assert key != null;
        assert ver != null;
        assert evictLock.getReadHoldCount() == 1;
        assert !syncPreloadFut.isDone();

        Map<K, GridCacheVersion> evictHist0 = evictHist;

        assert evictHist0 != null;

        GridCacheVersion ver0 = evictHist0.get(key);

        // Permit preloading if version in history
        // is missing or less than passed in.
        return ver0 == null || ver0.isLess(ver);
    }

    /** {@inheritDoc} */
    @Override public void unwindUndeploys() {
        demandPool.unwindUndeploys();
    }

    /**
     * Adds force keys future to future map.
     *
     * @param fut Future to add.
     */
    void addFuture(GridReplicatedForceKeysFuture<K, V> fut) {
        forceKeyFuts.put(fut.futureId(), fut);
    }

    /**
     * Removes force keys future from future map.
     *
     * @param fut Future to remove.
     */
    void removeFuture(GridReplicatedForceKeysFuture<K, V> fut) {
        forceKeyFuts.remove(fut.futureId(), fut);
    }

    /**
     * @return {@code true} if entered to busy state.
     */
    private boolean enterBusy() {
        if (busyLock.readLock().tryLock())
            return true;

        if (log.isDebugEnabled())
            log.debug("Failed to enter busy state on node: " + cctx.nodeId());

        return false;
    }

    /**
     */
    private void leaveBusy() {
        busyLock.readLock().unlock();
    }

    /**
     * @param nodeId Node ID.
     * @param req Request.
     */
    private void processForceKeyRequest(UUID nodeId, GridReplicatedForceKeysRequest<K, V> req) {
        if (!enterBusy())
            return;

        try {
            GridReplicatedForceKeysResponse<K, V> res = new GridReplicatedForceKeysResponse<>(req.futureId());

            for (K key : req.keys()) {
                GridCacheEntryEx<K, V> entry = cctx.cache().peekEx(key);

                if (entry != null) {
                    GridCacheEntryInfo<K, V> info = entry.info();

                    if (info != null && !info.isNew())
                        res.addInfo(info);
                }
                else if (log.isDebugEnabled())
                    log.debug("Key is not present in replicated cache: " + key);
            }

            if (log.isDebugEnabled())
                log.debug("Sending force key response [node=" + nodeId + ", res=" + res + ']');

            try {
                cctx.io().send(nodeId, res);
            }
            catch (GridTopologyException ignore) {
                if (log.isDebugEnabled())
                    log.debug("Received force key request form failed node (will ignore) [nodeId=" + nodeId +
                        ", req=" + req + ']');
            }
            catch (GridException e) {
                U.error(log, "Failed to reply to force key request [nodeId=" + nodeId + ", req=" + req + ']', e);
            }
        }
        finally {
            leaveBusy();
        }
    }

    /**
     * @param nodeId Node ID.
     * @param res Response.
     */
    private void processForceKeyResponse(UUID nodeId, GridReplicatedForceKeysResponse<K, V> res) {
        if (!enterBusy())
            return;

        try {
            GridReplicatedForceKeysFuture<K, V> f = forceKeyFuts.get(res.futureId());

            if (f == null) {
                if (log.isDebugEnabled())
                    log.debug("Receive force key response for unknown future (is it duplicate?) [nodeId=" + nodeId +
                        ", res=" + res + ']');

                return;
            }

            f.onResult(res);
        }
        finally {
            leaveBusy();
        }
    }

    /** {@inheritDoc} */
    @Override public GridFuture<Object> request(Collection<? extends K> keys, long topVer) {
        if (syncFuture().isDone())
            return new GridFinishedFuture<>(cctx.kernalContext());

        GridReplicatedForceKeysFuture<K, V> fut = new GridReplicatedForceKeysFuture<>(cctx, this, topVer);

        fut.init(keys);

        return fut;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridReplicatedPreloader.class, this);
    }
}
