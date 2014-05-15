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
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.resources.*;
import org.gridgain.grid.thread.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.grid.util.worker.*;
import org.jetbrains.annotations.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.gridgain.grid.cache.GridCacheAtomicityMode.*;
import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.gridgain.grid.kernal.GridClosureCallMode.*;
import static org.gridgain.grid.cache.GridCacheConfiguration.*;

/**
 * Distributed Garbage Collector for cache.
 */
public class GridCacheDgcManager<K, V> extends GridCacheManagerAdapter<K, V> {
    /** Flag to log trace enabled/disabled message. */
    private static final AtomicBoolean traceLogged = new AtomicBoolean();

    /** DGC thread. */
    private GridThread gcThread;

    /** Request worker thread. */
    private GridThread reqThread;

    /** Request worker. */
    private RequestWorker reqWorker;

    /** Response worker thread. */
    private GridThread resThread;

    /** Response worker. */
    private ResponseWorker resWorker;

    /** DGC frequency. */
    private long dgcFreq;

    /** DGC suspect lock timeout. */
    private long dgcSuspectLockTimeout;

    /** Trace log. */
    private GridLogger traceLog;

    /** */
    private CI2<UUID, GridCacheDgcRequest<K, V>> reqHnd = new CI2<UUID, GridCacheDgcRequest<K, V>>() {
        @Override public void apply(UUID nodeId, GridCacheDgcRequest<K, V> req) {
            if (log.isDebugEnabled())
                log.debug("Received DGC request [rmtNodeId=" + nodeId + ", req=" + req + ']');

            reqWorker.addDgcRequest(F.t(nodeId, req));
        }
    };

    /** */
    private CI2<UUID, GridCacheDgcResponse<K, V>> resHnd = new CI2<UUID, GridCacheDgcResponse<K, V>>() {
        @Override public void apply(UUID nodeId, GridCacheDgcResponse<K, V> res) {
            if (log.isDebugEnabled())
                log.debug("Received DGC response [rmtNodeId=" + nodeId + ", res=" + res + ']');

            resWorker.addDgcResponse(F.t(nodeId, res));
        }
    };

    /** {@inheritDoc} */
    @Override public void start0() throws GridException {
        if (cctx.config().getCacheMode() == LOCAL || cctx.config().getAtomicityMode() == ATOMIC)
            // No-op for local and atomic caches.
            return;

        traceLog = log.getLogger(DGC_TRACE_LOGGER_NAME);

        if (traceLogged.compareAndSet(false, true)) {
            if (traceLog.isDebugEnabled())
                traceLog.debug("DGC trace log enabled.");
            else
                U.log(log , "DGC trace log disabled.");
        }

        dgcFreq = cctx.config().getDgcFrequency();

        A.ensure(dgcFreq >= 0, "dgcFreq cannot be negative");

        dgcSuspectLockTimeout = cctx.config().getDgcSuspectLockTimeout();

        A.ensure(dgcSuspectLockTimeout >= 0, "dgcSuspiciousLockTimeout cannot be negative");

        if (dgcFreq > 0 && log.isDebugEnabled()) {
            log.debug(
                "Locks older than " + dgcSuspectLockTimeout + " ms. " +
                "will be implicitly removed in case they are not present on lock owner nodes. " +
                "To change this behavior please configure 'dgcFrequency' and 'dgcSuspectLockTimeout' " +
                "cache configuration properties."
            );
        }

        reqThread = new GridThread(reqWorker = new RequestWorker());

        reqThread.start();

        resThread = new GridThread(resWorker = new ResponseWorker());

        resThread.start();

        cctx.io().addHandler(GridCacheDgcRequest.class, reqHnd);
        cctx.io().addHandler(GridCacheDgcResponse.class, resHnd);

        if (log.isDebugEnabled())
            log.debug("Started DGC manager " +
                "[dgcFreq=" + dgcFreq + ", suspectLockTimeout=" + dgcSuspectLockTimeout + ']');
    }

    /** {@inheritDoc} */
    @Override protected void onKernalStart0() throws GridException {
        if (dgcFreq > 0) {
            // Start thread here since discovery may not start within DGC frequency and
            // thread cannot be started in start0() method.
            gcThread = new GridThread(new DgcWorker());

            gcThread.start();
        }
    }

    /** {@inheritDoc} */
    @Override public void onKernalStop0(boolean cancel) {
        if (cctx.config().getCacheMode() == LOCAL || cctx.config().getAtomicityMode() == ATOMIC)
            // No-op for local and atomic caches.
            return;

        cctx.io().removeHandler(GridCacheDgcRequest.class, reqHnd);
        cctx.io().removeHandler(GridCacheDgcResponse.class, resHnd);

        if (reqThread != null) {
            U.interrupt(reqThread);

            U.join(reqThread, log);
        }

        if (resThread != null) {
            U.interrupt(resThread);

            U.join(resThread, log);
        }

        if (gcThread != null) {
            U.interrupt(gcThread);

            U.join(gcThread, log);
        }
    }

    /**
     * Runs DGC procedure on demand using
     * {@link GridCacheConfiguration#getDgcSuspectLockTimeout()} to identify suspect locks.
     * <p>
     * Method blocks current thread until locks are examined and all DGC requests are sent
     * to remote nodes.
     * <p>
     * DGC does not remove locks if {@link GridCacheConfiguration#isDgcRemoveLocks()}
     * is set to {@code false}.
     */
    public void dgc() {
        dgc(dgcSuspectLockTimeout, true, cctx.config().isDgcRemoveLocks());
    }

    /**
     * Runs DGC procedure on demand using provided parameter to identify suspect locks.
     * <p>
     * Method blocks current thread until locks are examined and all DGC requests are sent
     * to remote nodes and (if {@code global} is {@code true}) all nodes having this cache
     * get signal to start DGC procedure.
     *
     * @param suspectLockTimeout Custom suspect lock timeout (should be greater than or equal to 0).
     * @param global If {@code true} then DGC procedure will start on all nodes having this cache.
     * @param rmvLocks If {@code false} then DGC does not remove locks, just report them to log.
     */
    public void dgc(long suspectLockTimeout, boolean global, boolean rmvLocks) {
        A.ensure(suspectLockTimeout >= 0, "suspectLockTimeout cannot be negative");

        if (log.isDebugEnabled())
            log.debug("Starting DGC iteration.");

        Map<UUID, GridCacheDgcRequest<K, V>> map = new HashMap<>();

        long threshold = U.currentTimeMillis() - suspectLockTimeout;

        P1<GridCacheMvccCandidate<K>> suspectLockPred = suspectLockPredicate(threshold);

        // DHT remote locks.
        Collection<GridCacheMvccCandidate<K>> suspectLocks = F.view(
            cctx.mvcc().remoteCandidates(), suspectLockPred);

        // Empty list to avoid IDE warning below.
        Collection<GridCacheMvccCandidate<K>> nearSuspectLocks = Collections.emptyList();

        GridCacheContext<K, V> nearCtx = cctx.isDht() ? cctx.dht().near().context() : null;

        if (cctx.isDht() || cctx.isColocated()) {
            // Add DHT local locks.
            suspectLocks = F.concat(false, suspectLocks, F.view(
                cctx.mvcc().localCandidates(), suspectLockPred));

            if (cctx.isDht()) {
                assert nearCtx != null;

                nearSuspectLocks = F.view(nearCtx.mvcc().remoteCandidates(), nearSuspectLockPredicate(threshold));
            }
        }

        if (traceLog.isDebugEnabled() && !suspectLocks.isEmpty()) {
            traceLog.debug("Beginning to check on suspect locks [" + U.nl() +
                "\t DHT suspect locks: " + suspectLocks + "," + U.nl() +
                "\t DHT active transactions: " + cctx.tm().txs() + U.nl() +
                "\t DHT active local locks: " + cctx.mvcc().localCandidates() + U.nl() +
                "\t DHT active remote locks: " + cctx.mvcc().remoteCandidates() + U.nl() +
                (nearCtx == null || nearSuspectLocks.isEmpty() ? "" :
                    "\t near suspect locks: " + nearSuspectLocks + U.nl() +
                    "\t near active transactions: " + nearCtx.tm().txs() + U.nl() +
                    "\t near active local locks: " + nearCtx.mvcc().localCandidates() + U.nl() +
                    "\t near active remote locks: " + nearCtx.mvcc().remoteCandidates() + U.nl()) +
                "]");
        }

        if (!nearSuspectLocks.isEmpty())
            suspectLocks = F.concat(false, suspectLocks, nearSuspectLocks);

        for (GridCacheMvccCandidate<K> lock : suspectLocks) {
            if (lock.dhtLocal()) {
                if (cctx.nodeId().equals(lock.otherNodeId())) {
                    if (badLock(lock.key(), new GridCacheDgcLockCandidate(lock.otherNodeId(), lock.otherVersion(),
                        lock.version()), cctx.localNodeId())) {
                        if (traceLog.isDebugEnabled())
                            traceLog.debug("Failed to find near-local lock for DHT-local lock: " + lock);
                    }
                }
                else {
                    GridCacheDgcRequest<K, V> req = F.addIfAbsent(map, lock.otherNodeId(),
                        new GridCacheDgcRequest<K, V>());

                    assert req != null;

                    req.removeLocks(rmvLocks);

                    req.addCandidate(lock.key(), new GridCacheDgcLockCandidate(lock.otherNodeId(), lock.otherVersion(),
                        lock.version()));
                }
            }
            // DHT or near remote.
            else {
                // Grab node ID for replicated transactions and otherNodeId for DHT transactions.
                UUID nodeId = lock.otherNodeId() == null ? lock.nodeId() : lock.otherNodeId();

                GridCacheDgcRequest<K, V> req = F.addIfAbsent(map, nodeId, new GridCacheDgcRequest<K, V>());

                assert req != null;

                req.removeLocks(rmvLocks);

                req.addCandidate(lock.key(), new GridCacheDgcLockCandidate(nodeId, null, lock.version()));
            }
        }

        if (log.isDebugEnabled())
            log.debug("Finished examining locks.");

        for (Map.Entry<UUID, GridCacheDgcRequest<K, V>> entry : map.entrySet()) {
            UUID nodeId = entry.getKey();
            GridCacheDgcRequest<K, V> req = entry.getValue();

            if (cctx.discovery().node(nodeId) == null)
                // Node has left the topology, safely remove all locks.
                resWorker.addDgcResponse(F.t(nodeId, fakeResponse(req)));
            else
                sendMessage(nodeId, req);
        }

        if (log.isDebugEnabled())
            log.debug("Finished sending DGC requests.");

        Collection<GridNode> nodes = CU.remoteNodes(cctx);

        if (global && !nodes.isEmpty())
            cctx.closures().callAsync(
                BROADCAST,
                new DgcCallable(cctx.name(), suspectLockTimeout, cctx.config().isDgcRemoveLocks()),
                nodes
            );

        if (log.isDebugEnabled())
            log.debug("Finished DGC iteration.");
    }

    /**
     * @param threshold Threshold.
     * @return Predicate.
     */
    private P1<GridCacheMvccCandidate<K>> nearSuspectLockPredicate(final long threshold) {
        return new P1<GridCacheMvccCandidate<K>>() {
            @Override public boolean apply(GridCacheMvccCandidate<K> lock) {
                return !lock.nearLocal() && !lock.used() && lock.timestamp() < threshold;
            }
        };
    }

    /**
     * @param threshold Threshold.
     * @return Predicate.
     */
    private P1<GridCacheMvccCandidate<K>> suspectLockPredicate(final long threshold) {
        return new P1<GridCacheMvccCandidate<K>>() {
            @Override public boolean apply(GridCacheMvccCandidate<K> lock) {
                return !lock.used() && lock.timestamp() < threshold;
            }
        };
    }

    /**
     * Checks if there is a corresponding local lock for given remove candidate.
     *
     * @param key Key to check.
     * @param cand Candidate.
     * @param sndId Sender id.
     * @return {@code True} if lock is considered to be bad (i.e. there is no local candidate matching
     *      remote one.
     */
    private boolean badLock(K key, GridCacheDgcLockCandidate cand, UUID sndId) {
        if (cand.near() && cctx.isColocated()) {
            GridCacheTxManager<K, V> tm = cctx.tm();

            if (tm.tx(cand.nearVersion()) == null && cctx.mvcc().explicitLock(key, cand.nearVersion()) == null) {
                if (traceLog.isDebugEnabled()) {
                    traceLog.debug("Failed to find explicit lock or active transaction for remote " +
                        "candidate [cand=" + cand + ", rmtNodeId=" + sndId + ']');
                }

                return true;
            }
        }
        else {
            GridCacheVersion ver = cand.near() ? cand.nearVersion() : cand.version();

            while (true) {
                GridCacheEntryEx<K, V> cached = cand.near() ?
                    cctx.dht().near().peekEx(key) : cctx.cache().peekEx(key);

                try {
                    if (cached == null || !cached.hasLockCandidate(ver)) {
                        if (traceLog.isDebugEnabled()) {
                            traceLog.debug("Failed to find main lock for remote candidate [cand=" + cand +
                                ", entry=" + cached + ", rmtNodeId=" + sndId + ']');
                        }

                        return true;
                    }

                    break;
                }
                catch (GridCacheEntryRemovedException ignore) {
                    if (log.isDebugEnabled())
                        log.debug("Found remove entry during DGC check (will retry): " + cached);
                }
            }
        }

        return false;
    }

    /**
     * @param req Request to create fake response for.
     * @return Fake response.
     */
    private GridCacheDgcResponse<K, V> fakeResponse(GridCacheDgcRequest<K, V> req) {
        assert req != null;

        GridCacheDgcResponse<K, V> res = new GridCacheDgcResponse<>();

        res.removeLocks(req.removeLocks());

        for (Map.Entry<K, Collection<GridCacheDgcLockCandidate>> entry : req.candidatesMap().entrySet()) {
            K key = entry.getKey();

            for (GridCacheDgcLockCandidate cand : entry.getValue())
                res.addCandidate(key, new GridCacheDgcBadLock(cand.nearVersion(), cand.version(), false));
        }

        return res;
    }

    /**
     * Send message to node.
     *
     * @param nodeId Node id.
     * @param msg  Message to send.
     */
    private void sendMessage(UUID nodeId, GridCacheMessage<K, V> msg) {
        try {
            cctx.io().send(nodeId, msg);

            if (log.isDebugEnabled())
                log.debug("Sent DGC message [rmtNodeId=" + nodeId + ", msg=" + msg + ']');
        }
        catch (GridTopologyException ignored) {
            if (log.isDebugEnabled())
                log.debug("Failed to send message to node (node left grid): " + nodeId);
        }
        catch (GridException e) {
            U.error(log, "Failed to send message to node: " + nodeId, e);
        }
    }

    /**
     * @return String with data for tracing.
     */
    private String traceData() {
        assert traceLog.isDebugEnabled();

        String nearInfo = "";

        if (cctx.isDht()) {
            GridCacheContext<K, V> nearCtx = cctx.dht().near().context();

            nearInfo =
                "\t near active transactions: " + nearCtx.tm().txs() +
                U.nl() +
                "\t near active local locks: " + nearCtx.mvcc().localCandidates() +
                U.nl() +
                "\t near active remote locks: " +
                    nearCtx.mvcc().remoteCandidates() +
                U.nl();
        }

        return U.nl() +
            "\t DHT active transactions: " + cctx.tm().txs() +
            U.nl() +
            "\t DHT active local locks: " + cctx.mvcc().localCandidates() +
            U.nl() +
            "\t DHT active remote locks: " + cctx.mvcc().remoteCandidates() +
            U.nl() +
            nearInfo;
    }

    /**
     * Worker that scans current locks and initiates DGC requests if needed.
     */
    private class DgcWorker extends GridWorker {
        /**
         * Constructor.
         */
        private DgcWorker() {
            super(cctx.gridName(), "cache-dgc-worker", log);
        }

        /** {@inheritDoc} */
        @SuppressWarnings({"BusyWait"})
        @Override public void body() throws InterruptedException {
            assert dgcFreq > 0;

            while (!isCancelled()) {
                Thread.sleep(dgcFreq);

                dgc(dgcSuspectLockTimeout, false, cctx.config().isDgcRemoveLocks());
            }
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheDgcManager.class, this);
    }

    /**
     * Worker that processes DGC requests and sends responses back.
     */
    private class RequestWorker extends GridWorker {
        /** */
        private BlockingQueue<GridBiTuple<UUID, GridCacheDgcRequest<K, V>>> queue =
            new LinkedBlockingQueue<>();

        /**
         * Default constructor.
         */
        RequestWorker() {
            super(cctx.gridName(), "cache-dgc-req-worker", log);
        }

        /**
         * @param t Request tuple.
         */
        void addDgcRequest(GridBiTuple<UUID, GridCacheDgcRequest<K, V>> t) {
            assert t != null;

            queue.add(t);
        }

        /** {@inheritDoc} */
        @Override public void body() throws InterruptedException {
            while (!isCancelled()) {
                GridBiTuple<UUID, GridCacheDgcRequest<K, V>> tup = queue.take();

                UUID sndId = tup.get1();
                GridCacheDgcRequest<K, V> req = tup.get2();

                GridCacheDgcResponse<K, V> res = new GridCacheDgcResponse<>();

                res.removeLocks(req.removeLocks());

                boolean found = false;

                for (Map.Entry<K, Collection<GridCacheDgcLockCandidate>> entry : req.candidatesMap().entrySet()) {
                    K key = entry.getKey();
                    Collection<GridCacheDgcLockCandidate> cands = entry.getValue();

                    for (GridCacheDgcLockCandidate cand : cands) {
                        GridCacheTxManager<K, V> tm = cand.near() && !cctx.isColocated() ?
                            cctx.dht().near().context().tm() : cctx.tm();

                        if (badLock(key, cand, sndId)) {
                            GridCacheVersion ver = cand.near() ? cand.nearVersion() : cand.version();

                            res.addCandidate(key, new GridCacheDgcBadLock(
                                cand.nearVersion(),
                                cand.version(),
                                tm.rolledbackVersions(ver).contains(ver)));

                            found = true;
                        }
                    }
                }

                if (found) {
                    if (traceLog.isDebugEnabled())
                        traceLog.debug("DGC trace data: " + U.nl() + traceData());

                    assert !res.candidatesMap().isEmpty();

                    sendMessage(sndId, res);
                }
            }
        }
    }

    /**
     * Worker that processes DGC responses.
     */
    private class ResponseWorker extends GridWorker {
        /** */
        private BlockingQueue<GridBiTuple<UUID, GridCacheDgcResponse<K, V>>> queue =
            new LinkedBlockingQueue<>();

        /**
         * Default constructor.
         */
        ResponseWorker() {
            super(cctx.gridName(), "cache-dgc-res-worker", log);
        }

        /**
         * @param t Response tuple.
         */
        void addDgcResponse(GridBiTuple<UUID, GridCacheDgcResponse<K, V>> t) {
            assert t != null;

            queue.add(t);
        }

        /** {@inheritDoc} */
        @SuppressWarnings({"MismatchedQueryAndUpdateOfCollection", "TooBroadScope"})
        @Override public void body() throws InterruptedException {
            GridCacheContext<K, V> nearCtx = cctx.isDht() ? cctx.dht().near().context() : null;

            while (!isCancelled()) {
                GridBiTuple<UUID, GridCacheDgcResponse<K, V>> tup = queue.take();

                GridCacheDgcResponse<K, V> res = tup.get2();

                int salvagedTxCnt = 0;
                int rolledbackTxCnt = 0;
                int rmvLockCnt = 0;

                Map<K, Collection<GridCacheDgcBadLock>> nonTx = new HashMap<>();

                Collection<GridCacheVersion> xids = new HashSet<>();

                for (Map.Entry<K, Collection<GridCacheDgcBadLock>> e : res.candidatesMap().entrySet()) {
                    for (GridCacheDgcBadLock badLock : e.getValue()) {
                        GridCacheContext<K, V> cacheCtx = cctx;

                        GridCacheTxEx<K, V> tx = cacheCtx.tm().txx(badLock.version());

                        if (tx == null && nearCtx != null) {
                            tx = nearCtx.tm().txx(badLock.version());

                            if (tx != null)
                                cacheCtx = nearCtx;
                        }

                        if (tx != null) {
                            if (!xids.add(tx.xidVersion()))
                                // Transaction bad lock belongs to has already been processed.
                                continue;

                            if (badLock.rollback()) {
                                if (res.removeLocks()) {
                                    try {
                                        tx.rollback();

                                        if (traceLog.isDebugEnabled())
                                            traceLog.debug("DGC has rolled back transaction: " + tx);

                                        rolledbackTxCnt++;
                                    }
                                    catch (GridException ex) {
                                        U.error(log, "DGC failed to rollback transaction: " + tx, ex);
                                    }
                                }
                                else if (traceLog.isDebugEnabled())
                                    traceLog.debug("DGC has not rolled back transaction due to user configuration: " +
                                        tx);
                            }
                            else {
                                if (res.removeLocks()) {
                                    if (!cacheCtx.tm().salvageTx(tx))
                                        continue;

                                    if (traceLog.isDebugEnabled())
                                        traceLog.debug("DGC has salvaged transaction: " + tx);

                                    salvagedTxCnt++;
                                }
                                else if (traceLog.isDebugEnabled())
                                    traceLog.debug("DGC has not salvaged DHT transaction due to user configuration: " +
                                        tx);
                            }
                        }
                        else {
                            Collection<GridCacheDgcBadLock> col =
                                F.addIfAbsent(nonTx, e.getKey(), new LinkedHashSet<GridCacheDgcBadLock>());

                            assert col != null;

                            col.add(badLock);
                        }
                    }
                }

                if (!nonTx.isEmpty()) {
                    GridCacheVersion newVer = cctx.versions().next();

                    for (Map.Entry<K, Collection<GridCacheDgcBadLock>> e : nonTx.entrySet()) {
                        while (true) {
                            GridCacheEntryEx<K, V> cached = cctx.cache().peekEx(e.getKey());

                            if (cached == null && nearCtx != null)
                                cached = nearCtx.near().peekEx(e.getKey());

                            if (cached != null) {
                                if (!res.removeLocks()) {
                                    if (traceLog.isDebugEnabled())
                                        traceLog.debug("DGC has not removed locks on entry due to user " +
                                            "configuration [entry=" + cached + ", badLocks=" + e.getValue() + ']');

                                    break; // While loop.
                                }

                                try {
                                    Collection<GridCacheVersion> locks = new LinkedList<>();

                                    for (GridCacheDgcBadLock badLock : e.getValue()) {
                                        if (cached.hasLockCandidate(badLock.version()))
                                            locks.add(badLock.version());
                                    }

                                    if (locks.isEmpty())
                                        break; // While loop.

                                    // Invalidate before removing lock.
                                    try {
                                        cached.invalidate(null, newVer);
                                    }
                                    catch (GridException ex) {
                                        U.error(log, "Failed to invalidate entry: " + cached, ex);
                                    }

                                    for (GridCacheVersion ver : locks) {
                                        if (cached.removeLock(ver))
                                            rmvLockCnt++;
                                    }

                                    if (traceLog.isDebugEnabled())
                                        traceLog.debug("DGC has removed locks on entry " +
                                            "[entry=" + cached + ", badLocks=" + locks + ']');

                                    break; // While loop.
                                }
                                catch (GridCacheEntryRemovedException ignored) {
                                    if (log.isDebugEnabled())
                                        log.debug("Attempted to remove lock on obsolete entry (will retry): " + cached);
                                }
                            }
                            else
                                break;
                        }
                    }
                }

                if (salvagedTxCnt != 0 || rolledbackTxCnt != 0 || rmvLockCnt != 0) {
                    U.warn(log, "DGCed suspicious transactions and locks " +
                        "(consider increasing 'dgcSuspectLockTimeout' configuration property) " +
                        "[rmtNodeId=" + tup.get1() + ", salvagedTxCnt=" + salvagedTxCnt +
                        ", rolledbackTxCnt=" + rolledbackTxCnt +
                        ", rmvLockCnt=" + rmvLockCnt + ']');
                }
            }
        }
    }

    /**
     *
     */
    private static class DgcCallable implements GridCallable<Object> {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private final String cacheName;

        /** */
        private final long suspectLockTimeout;

        /** */
        private final boolean rmvLocks;

        /** */
        @GridInstanceResource
        private Grid grid;

        /**
         * @param cacheName Cache name.
         * @param suspectLockTimeout Suspect lock timeout.
         * @param rmvLocks Remove locks flag.
         */
        private DgcCallable(String cacheName, long suspectLockTimeout, boolean rmvLocks) {
            this.cacheName = cacheName;
            this.suspectLockTimeout = suspectLockTimeout;
            this.rmvLocks = rmvLocks;
        }

        /** {@inheritDoc} */
        @Nullable @Override public Object call() throws Exception {
            ((GridEx)grid).cachex(cacheName).dgc(suspectLockTimeout, false, rmvLocks);

            return null;
        }
    }

}
