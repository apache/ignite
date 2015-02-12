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

package org.apache.ignite.internal.processors.cache.distributed.dht;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.cluster.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.processors.cache.distributed.*;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.*;
import org.apache.ignite.internal.processors.cache.distributed.near.*;
import org.apache.ignite.internal.processors.cache.transactions.*;
import org.apache.ignite.internal.processors.cache.version.*;
import org.apache.ignite.internal.transactions.*;
import org.apache.ignite.internal.util.*;
import org.apache.ignite.internal.util.future.*;
import org.apache.ignite.internal.util.lang.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.transactions.*;
import org.jetbrains.annotations.*;

import javax.cache.*;
import java.io.*;
import java.util.*;

import static org.apache.ignite.internal.managers.communication.GridIoPolicy.*;
import static org.apache.ignite.internal.processors.cache.GridCacheOperation.*;
import static org.apache.ignite.internal.processors.cache.GridCacheUtils.*;
import static org.apache.ignite.transactions.IgniteTxConcurrency.*;
import static org.apache.ignite.transactions.IgniteTxState.*;

/**
 * Base class for transactional DHT caches.
 */
public abstract class GridDhtTransactionalCacheAdapter<K, V> extends GridDhtCacheAdapter<K, V> {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    protected GridDhtTransactionalCacheAdapter() {
        // No-op.
    }

    /**
     * @param ctx Context.
     */
    protected GridDhtTransactionalCacheAdapter(GridCacheContext<K, V> ctx) {
        super(ctx);
    }

    /**
     * Constructor used for near-only cache.
     *
     * @param ctx Cache context.
     * @param map Cache map.
     */
    protected GridDhtTransactionalCacheAdapter(GridCacheContext<K, V> ctx, GridCacheConcurrentMap<K, V> map) {
        super(ctx, map);
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteCheckedException {
        super.start();

        preldr = new GridDhtPreloader<>(ctx);

        preldr.start();

        ctx.io().addHandler(ctx.cacheId(), GridNearGetRequest.class, new CI2<UUID, GridNearGetRequest<K, V>>() {
            @Override public void apply(UUID nodeId, GridNearGetRequest<K, V> req) {
                processNearGetRequest(nodeId, req);
            }
        });

        ctx.io().addHandler(ctx.cacheId(), GridNearLockRequest.class, new CI2<UUID, GridNearLockRequest<K, V>>() {
            @Override public void apply(UUID nodeId, GridNearLockRequest<K, V> req) {
                processNearLockRequest(nodeId, req);
            }
        });

        ctx.io().addHandler(ctx.cacheId(), GridDhtLockRequest.class, new CI2<UUID, GridDhtLockRequest<K, V>>() {
            @Override public void apply(UUID nodeId, GridDhtLockRequest<K, V> req) {
                processDhtLockRequest(nodeId, req);
            }
        });

        ctx.io().addHandler(ctx.cacheId(), GridDhtLockResponse.class, new CI2<UUID, GridDhtLockResponse<K, V>>() {
            @Override public void apply(UUID nodeId, GridDhtLockResponse<K, V> req) {
                processDhtLockResponse(nodeId, req);
            }
        });

        ctx.io().addHandler(ctx.cacheId(), GridNearUnlockRequest.class, new CI2<UUID, GridNearUnlockRequest<K, V>>() {
            @Override public void apply(UUID nodeId, GridNearUnlockRequest<K, V> req) {
                processNearUnlockRequest(nodeId, req);
            }
        });

        ctx.io().addHandler(ctx.cacheId(), GridDhtUnlockRequest.class, new CI2<UUID, GridDhtUnlockRequest<K, V>>() {
            @Override public void apply(UUID nodeId, GridDhtUnlockRequest<K, V> req) {
                processDhtUnlockRequest(nodeId, req);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public abstract GridNearTransactionalCache<K, V> near();

    /**
     * @param nodeId Primary node ID.
     * @param req Request.
     * @param res Response.
     * @return Remote transaction.
     * @throws IgniteCheckedException If failed.
     * @throws GridDistributedLockCancelledException If lock has been cancelled.
     */
    @SuppressWarnings({"RedundantTypeArguments"})
    @Nullable GridDhtTxRemote<K, V> startRemoteTx(UUID nodeId,
        GridDhtLockRequest<K, V> req,
        GridDhtLockResponse<K, V> res)
        throws IgniteCheckedException, GridDistributedLockCancelledException {
        List<K> keys = req.keys();
        List<IgniteTxEntry<K, V>> writes = req.writeEntries();

        GridDhtTxRemote<K, V> tx = null;

        int size = F.size(keys);

        for (int i = 0; i < size; i++) {
            K key = keys.get(i);

            if (key == null)
                continue;

            IgniteTxKey<K> txKey = ctx.txKey(key);

            IgniteTxEntry<K, V> writeEntry = writes == null ? null : writes.get(i);

            assert F.isEmpty(req.candidatesByIndex(i));

            GridCacheVersion drVer = req.drVersionByIndex(i);

            if (log.isDebugEnabled())
                log.debug("Unmarshalled key: " + key);

            GridDistributedCacheEntry<K, V> entry = null;

            while (true) {
                try {
                    int part = ctx.affinity().partition(key);

                    GridDhtLocalPartition<K, V> locPart = ctx.topology().localPartition(part, req.topologyVersion(),
                        false);

                    if (locPart == null || !locPart.reserve()) {
                        if (log.isDebugEnabled())
                            log.debug("Local partition for given key is already evicted (will add to invalid " +
                                "partition list) [key=" + key + ", part=" + part + ", locPart=" + locPart + ']');

                        res.addInvalidPartition(part);

                        // Invalidate key in near cache, if any.
                        if (isNearEnabled(cacheCfg))
                            obsoleteNearEntry(key, req.version());

                        break;
                    }

                    try {
                        // Handle implicit locks for pessimistic transactions.
                        if (req.inTx()) {
                            if (tx == null)
                                tx = ctx.tm().tx(req.version());

                            if (tx == null) {
                                tx = new GridDhtTxRemote<>(
                                    ctx.shared(),
                                    req.nodeId(),
                                    req.futureId(),
                                    nodeId,
                                    req.nearXidVersion(),
                                    req.threadId(),
                                    req.topologyVersion(),
                                    req.version(),
                                    /*commitVer*/null,
                                    ctx.system(),
                                    PESSIMISTIC,
                                    req.isolation(),
                                    req.isInvalidate(),
                                    req.timeout(),
                                    req.txSize(),
                                    req.groupLockKey(),
                                    req.subjectId(),
                                    req.taskNameHash());

                                tx = ctx.tm().onCreated(tx);

                                if (tx == null || !ctx.tm().onStarted(tx))
                                    throw new IgniteTxRollbackCheckedException("Failed to acquire lock (transaction " +
                                        "has been completed) [ver=" + req.version() + ", tx=" + tx + ']');
                            }

                            tx.addWrite(
                                ctx,
                                writeEntry == null ? NOOP : writeEntry.op(),
                                txKey,
                                req.keyBytes() != null ? req.keyBytes().get(i) : null,
                                writeEntry == null ? null : writeEntry.value(),
                                writeEntry == null ? null : writeEntry.valueBytes(),
                                writeEntry == null ? null : writeEntry.entryProcessors(),
                                drVer,
                                req.accessTtl());

                            if (req.groupLock())
                                tx.groupLockKey(txKey);
                        }

                        entry = entryExx(key, req.topologyVersion());

                        // Add remote candidate before reordering.
                        entry.addRemote(
                            req.nodeId(),
                            nodeId,
                            req.threadId(),
                            req.version(),
                            req.timeout(),
                            tx != null,
                            tx != null && tx.implicitSingle(),
                            null
                        );

                        // Invalidate key in near cache, if any.
                        if (isNearEnabled(cacheCfg) && req.invalidateNearEntry(i))
                            invalidateNearEntry(key, req.version());

                        // Get entry info after candidate is added.
                        if (req.needPreloadKey(i)) {
                            entry.unswap();

                            GridCacheEntryInfo<K, V> info = entry.info();

                            if (info != null && !info.isNew() && !info.isDeleted())
                                res.addPreloadEntry(info);
                        }

                        // Double-check in case if sender node left the grid.
                        if (ctx.discovery().node(req.nodeId()) == null) {
                            if (log.isDebugEnabled())
                                log.debug("Node requesting lock left grid (lock request will be ignored): " + req);

                            entry.removeLock(req.version());

                            if (tx != null) {
                                tx.clearEntry(txKey);

                                // If there is a concurrent salvage, there could be a case when tx is moved to
                                // COMMITTING state, but this lock is never acquired.
                                if (tx.state() == COMMITTING)
                                    tx.forceCommit();
                                else
                                    tx.rollback();
                            }

                            return null;
                        }

                        // Entry is legit.
                        break;
                    }
                    finally {
                        locPart.release();
                    }
                }
                catch (GridDhtInvalidPartitionException e) {
                    if (log.isDebugEnabled())
                        log.debug("Received invalid partition exception [e=" + e + ", req=" + req + ']');

                    res.addInvalidPartition(e.partition());

                    // Invalidate key in near cache, if any.
                    if (isNearEnabled(cacheCfg))
                        obsoleteNearEntry(key, req.version());

                    if (tx != null) {
                        tx.clearEntry(txKey);

                        if (log.isDebugEnabled())
                            log.debug("Cleared invalid entry from remote transaction (will skip) [entry=" +
                                entry + ", tx=" + tx + ']');
                    }

                    break;
                }
                catch (GridCacheEntryRemovedException ignored) {
                    assert entry.obsoleteVersion() != null : "Obsolete flag not set on removed entry: " +
                        entry;

                    if (log.isDebugEnabled())
                        log.debug("Received entry removed exception (will retry on renewed entry): " + entry);

                    if (tx != null) {
                        tx.clearEntry(txKey);

                        if (log.isDebugEnabled())
                            log.debug("Cleared removed entry from remote transaction (will retry) [entry=" +
                                entry + ", tx=" + tx + ']');
                    }
                }
            }
        }

        if (tx != null && tx.empty()) {
            if (log.isDebugEnabled())
                log.debug("Rolling back remote DHT transaction because it is empty [req=" + req + ", res=" + res + ']');

            tx.rollback();

            tx = null;
        }

        return tx;
    }

    /**
     * @param nodeId Node ID.
     * @param req Request.
     */
    protected final void processDhtLockRequest(final UUID nodeId, final GridDhtLockRequest<K, V> req) {
        IgniteInternalFuture<Object> keyFut = F.isEmpty(req.keys()) ? null :
            ctx.dht().dhtPreloader().request(req.keys(), req.topologyVersion());

        if (keyFut == null || keyFut.isDone())
            processDhtLockRequest0(nodeId, req);
        else {
            keyFut.listenAsync(new CI1<IgniteInternalFuture<Object>>() {
                @Override public void apply(IgniteInternalFuture<Object> t) {
                    processDhtLockRequest0(nodeId, req);
                }
            });
        }
    }

    /**
     * @param nodeId Node ID.
     * @param req Request.
     */
    protected final void processDhtLockRequest0(UUID nodeId, GridDhtLockRequest<K, V> req) {
        assert nodeId != null;
        assert req != null;
        assert !nodeId.equals(locNodeId);

        if (log.isDebugEnabled())
            log.debug("Processing dht lock request [locNodeId=" + locNodeId + ", nodeId=" + nodeId + ", req=" + req +
                ']');

        int cnt = F.size(req.keys());

        GridDhtLockResponse<K, V> res;

        GridDhtTxRemote<K, V> dhtTx = null;
        GridNearTxRemote<K, V> nearTx = null;

        boolean fail = false;
        boolean cancelled = false;

        try {
            res = new GridDhtLockResponse<>(ctx.cacheId(), req.version(), req.futureId(), req.miniId(), cnt);

            dhtTx = startRemoteTx(nodeId, req, res);
            nearTx = isNearEnabled(cacheCfg) ? near().startRemoteTx(nodeId, req) : null;

            if (nearTx != null && !nearTx.empty())
                res.nearEvicted(nearTx.evicted());
            else {
                if (!F.isEmpty(req.nearKeys())) {
                    Collection<IgniteTxKey<K>> nearEvicted = new ArrayList<>(req.nearKeys().size());

                    nearEvicted.addAll(F.viewReadOnly(req.nearKeys(), new C1<K, IgniteTxKey<K>>() {
                        @Override public IgniteTxKey<K> apply(K k) {
                            return ctx.txKey(k);
                        }
                    }));

                    res.nearEvicted(nearEvicted);
                }
            }
        }
        catch (IgniteTxRollbackCheckedException e) {
            String err = "Failed processing DHT lock request (transaction has been completed): " + req;

            U.error(log, err, e);

            res = new GridDhtLockResponse<>(ctx.cacheId(), req.version(), req.futureId(), req.miniId(),
                new IgniteTxRollbackCheckedException(err, e));

            fail = true;
        }
        catch (IgniteCheckedException e) {
            String err = "Failed processing DHT lock request: " + req;

            U.error(log, err, e);

            res = new GridDhtLockResponse<>(ctx.cacheId(), req.version(), req.futureId(), req.miniId(), new IgniteCheckedException(err, e));

            fail = true;
        }
        catch (GridDistributedLockCancelledException ignored) {
            // Received lock request for cancelled lock.
            if (log.isDebugEnabled())
                log.debug("Received lock request for canceled lock (will ignore): " + req);

            res = null;

            fail = true;
            cancelled = true;
        }

        boolean releaseAll = false;

        if (res != null) {
            try {
                // Reply back to sender.
                ctx.io().send(nodeId, res, ctx.ioPolicy());
            }
            catch (ClusterTopologyCheckedException ignored) {
                U.warn(log, "Failed to send lock reply to remote node because it left grid: " + nodeId);

                fail = true;
                releaseAll = true;
            }
            catch (IgniteCheckedException e) {
                U.error(log, "Failed to send lock reply to node (lock will not be acquired): " + nodeId, e);

                fail = true;
            }
        }

        if (fail) {
            if (dhtTx != null)
                dhtTx.rollback();

            if (nearTx != null) // Even though this should never happen, we leave this check for consistency.
                nearTx.rollback();

            List<K> keys = req.keys();

            if (keys != null) {
                for (K key : keys) {
                    while (true) {
                        GridDistributedCacheEntry<K, V> entry = peekExx(key);

                        try {
                            if (entry != null) {
                                // Release all locks because sender node left grid.
                                if (releaseAll)
                                    entry.removeExplicitNodeLocks(req.nodeId());
                                else
                                    entry.removeLock(req.version());
                            }

                            break;
                        }
                        catch (GridCacheEntryRemovedException ignore) {
                            if (log.isDebugEnabled())
                                log.debug("Attempted to remove lock on removed entity during during failure " +
                                    "handling for dht lock request (will retry): " + entry);
                        }
                    }
                }
            }

            if (releaseAll && !cancelled)
                U.warn(log, "Sender node left grid in the midst of lock acquisition (locks have been released).");
        }
    }

    /** {@inheritDoc} */
    protected void processDhtUnlockRequest(UUID nodeId, GridDhtUnlockRequest<K, V> req) {
        clearLocks(nodeId, req);

        if (isNearEnabled(cacheCfg))
            near().clearLocks(nodeId, req);
    }

    /**
     * @param nodeId Node ID.
     * @param req Request.
     */
    private void processNearLockRequest(UUID nodeId, GridNearLockRequest<K, V> req) {
        assert isAffinityNode(cacheCfg);
        assert nodeId != null;
        assert req != null;

        if (log.isDebugEnabled())
            log.debug("Processing near lock request [locNodeId=" + locNodeId + ", nodeId=" + nodeId + ", req=" + req +
                ']');

        ClusterNode nearNode = ctx.discovery().node(nodeId);

        if (nearNode == null) {
            U.warn(log, "Received lock request from unknown node (will ignore): " + nodeId);

            return;
        }

        // Group lock can be only started from local node, so we never start group lock transaction on remote node.
        IgniteInternalFuture<?> f = lockAllAsync(ctx, nearNode, req, null);

        // Register listener just so we print out errors.
        // Exclude lock timeout exception since it's not a fatal exception.
        f.listenAsync(CU.errorLogger(log, GridCacheLockTimeoutException.class,
            GridDistributedLockCancelledException.class));
    }

    /**
     * @param nodeId Node ID.
     * @param res Response.
     */
    private void processDhtLockResponse(UUID nodeId, GridDhtLockResponse<K, V> res) {
        assert nodeId != null;
        assert res != null;
        GridDhtLockFuture<K, V> fut = (GridDhtLockFuture<K, V>)ctx.mvcc().<Boolean>future(res.version(),
            res.futureId());

        if (fut == null) {
            if (log.isDebugEnabled())
                log.debug("Received response for unknown future (will ignore): " + res);

            return;
        }

        fut.onResult(nodeId, res);
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Boolean> lockAllAsync(
        @Nullable Collection<? extends K> keys,
        long timeout,
        IgniteTxLocalEx<K, V> txx,
        boolean isInvalidate,
        boolean isRead,
        boolean retval,
        IgniteTxIsolation isolation,
        long accessTtl,
        IgnitePredicate<Cache.Entry<K, V>>[] filter) {
        return lockAllAsyncInternal(
            keys,
            timeout,
            txx,
            isInvalidate,
            isRead,
            retval,
            isolation,
            accessTtl,
            filter);
    }

    /**
     * Acquires locks in partitioned cache.
     *
     * @param keys Keys to lock.
     * @param timeout Lock timeout.
     * @param txx Transaction.
     * @param isInvalidate Invalidate flag.
     * @param isRead Read flag.
     * @param retval Return value flag.
     * @param isolation Transaction isolation.
     * @param accessTtl TTL for read operation.
     * @param filter Optional filter.
     * @return Lock future.
     */
    public GridDhtFuture<Boolean> lockAllAsyncInternal(@Nullable Collection<? extends K> keys,
        long timeout,
        IgniteTxLocalEx<K, V> txx,
        boolean isInvalidate,
        boolean isRead,
        boolean retval,
        IgniteTxIsolation isolation,
        long accessTtl,
        IgnitePredicate<Cache.Entry<K, V>>[] filter) {
        if (keys == null || keys.isEmpty())
            return new GridDhtFinishedFuture<>(ctx.kernalContext(), true);

        GridDhtTxLocalAdapter<K, V> tx = (GridDhtTxLocalAdapter<K, V>)txx;

        assert tx != null;

        GridDhtLockFuture<K, V> fut = new GridDhtLockFuture<>(
            ctx,
            tx.nearNodeId(),
            tx.nearXidVersion(),
            tx.topologyVersion(),
            keys.size(),
            isRead,
            timeout,
            tx,
            tx.threadId(),
            accessTtl,
            filter);

        for (K key : keys) {
            if (key == null)
                continue;

            try {
                while (true) {
                    GridDhtCacheEntry<K, V> entry = entryExx(key, tx.topologyVersion());

                    try {
                        fut.addEntry(entry);

                        // Possible in case of cancellation or time out.
                        if (fut.isDone())
                            return fut;

                        break;
                    }
                    catch (GridCacheEntryRemovedException ignore) {
                        if (log.isDebugEnabled())
                            log.debug("Got removed entry when adding lock (will retry): " + entry);
                    }
                    catch (GridDistributedLockCancelledException e) {
                        if (log.isDebugEnabled())
                            log.debug("Got lock request for cancelled lock (will fail): " + entry);

                        return new GridDhtFinishedFuture<>(ctx.kernalContext(), e);
                    }
                }
            }
            catch (GridDhtInvalidPartitionException e) {
                fut.addInvalidPartition(ctx, e.partition());

                if (log.isDebugEnabled())
                    log.debug("Added invalid partition to DHT lock future [part=" + e.partition() + ", fut=" +
                        fut + ']');
            }
        }

        ctx.mvcc().addFuture(fut);

        fut.map();

        return fut;
    }

    /**
     * @param cacheCtx Cache context.
     * @param nearNode Near node.
     * @param req Request.
     * @param filter0 Filter.
     * @return Future.
     */
    public IgniteInternalFuture<GridNearLockResponse<K, V>> lockAllAsync(
        final GridCacheContext<K, V> cacheCtx,
        final ClusterNode nearNode,
        final GridNearLockRequest<K, V> req,
        @Nullable final IgnitePredicate<Cache.Entry<K, V>>[] filter0) {
        final List<K> keys = req.keys();

        IgniteInternalFuture<Object> keyFut = null;

        if (req.onePhaseCommit()) {
            boolean forceKeys = req.hasTransforms() || req.filter() != null;

            if (!forceKeys) {
                for (int i = 0; i < req.keysCount() && !forceKeys; i++)
                    forceKeys |= req.returnValue(i);
            }

            if (forceKeys)
                keyFut = ctx.dht().dhtPreloader().request(keys, req.topologyVersion());
        }

        if (keyFut == null)
            keyFut = new GridFinishedFutureEx<>();

        return new GridEmbeddedFuture<>(true, keyFut,
            new C2<Object, Exception, IgniteInternalFuture<GridNearLockResponse<K,V>>>() {
                @Override public IgniteInternalFuture<GridNearLockResponse<K, V>> apply(Object o, Exception exx) {
                    if (exx != null)
                        return new GridDhtFinishedFuture<>(ctx.kernalContext(), exx);

                    IgnitePredicate<Cache.Entry<K, V>>[] filter = filter0;

                    // Set message into thread context.
                    GridDhtTxLocal<K, V> tx = null;

                    try {
                        int cnt = keys.size();

                        if (req.inTx()) {
                            GridCacheVersion dhtVer = ctx.tm().mappedVersion(req.version());

                            if (dhtVer != null)
                                tx = ctx.tm().tx(dhtVer);
                        }

                        final List<GridCacheEntryEx<K, V>> entries = new ArrayList<>(cnt);

                        // Unmarshal filter first.
                        if (filter == null)
                            filter = req.filter();

                        GridDhtLockFuture<K, V> fut = null;

                        if (!req.inTx()) {
                            fut = new GridDhtLockFuture<>(ctx,
                                nearNode.id(),
                                req.version(),
                                req.topologyVersion(),
                                cnt,
                                req.txRead(),
                                req.timeout(),
                                tx,
                                req.threadId(),
                                req.accessTtl(),
                                filter);

                            // Add before mapping.
                            if (!ctx.mvcc().addFuture(fut))
                                throw new IllegalStateException("Duplicate future ID: " + fut);
                        }

                        boolean timedout = false;

                        for (K key : keys) {
                            if (timedout)
                                break;

                            while (true) {
                                // Specify topology version to make sure containment is checked
                                // based on the requested version, not the latest.
                                GridDhtCacheEntry<K, V> entry = entryExx(key, req.topologyVersion());

                                try {
                                    if (fut != null) {
                                        // This method will add local candidate.
                                        // Entry cannot become obsolete after this method succeeded.
                                        fut.addEntry(key == null ? null : entry);

                                        if (fut.isDone()) {
                                            timedout = true;

                                            break;
                                        }
                                    }

                                    entries.add(entry);

                                    break;
                                }
                                catch (GridCacheEntryRemovedException ignore) {
                                    if (log.isDebugEnabled())
                                        log.debug("Got removed entry when adding lock (will retry): " + entry);
                                }
                                catch (GridDistributedLockCancelledException e) {
                                    if (log.isDebugEnabled())
                                        log.debug("Got lock request for cancelled lock (will ignore): " +
                                            entry);

                                    fut.onError(e);

                                    return new GridDhtFinishedFuture<>(ctx.kernalContext(), e);
                                }
                            }
                        }

                        // Handle implicit locks for pessimistic transactions.
                        if (req.inTx()) {
                            if (tx == null) {
                                tx = new GridDhtTxLocal<>(
                                    ctx.shared(),
                                    nearNode.id(),
                                    req.version(),
                                    req.futureId(),
                                    req.miniId(),
                                    req.threadId(),
                                    req.implicitTx(),
                                    req.implicitSingleTx(),
                                    ctx.system(),
                                    PESSIMISTIC,
                                    req.isolation(),
                                    req.timeout(),
                                    req.isInvalidate(),
                                    false,
                                    req.txSize(),
                                    req.groupLockKey(),
                                    req.partitionLock(),
                                    null,
                                    req.subjectId(),
                                    req.taskNameHash());

                                tx.syncCommit(req.syncCommit());

                                tx = ctx.tm().onCreated(tx);

                                if (tx == null || !tx.init()) {
                                    String msg = "Failed to acquire lock (transaction has been completed): " +
                                        req.version();

                                    U.warn(log, msg);

                                    if (tx != null)
                                        tx.rollback();

                                    return new GridDhtFinishedFuture<>(ctx.kernalContext(), new IgniteCheckedException(msg));
                                }

                                tx.topologyVersion(req.topologyVersion());
                            }

                            ctx.tm().txContext(tx);

                            if (log.isDebugEnabled())
                                log.debug("Performing DHT lock [tx=" + tx + ", entries=" + entries + ']');

                            assert req.writeEntries() == null || req.writeEntries().size() == entries.size();

                            IgniteInternalFuture<GridCacheReturn<V>> txFut = tx.lockAllAsync(
                                cacheCtx,
                                entries,
                                req.writeEntries(),
                                req.onePhaseCommit(),
                                req.drVersions(),
                                req.messageId(),
                                req.implicitTx(),
                                req.txRead(),
                                req.accessTtl());

                            final GridDhtTxLocal<K, V> t = tx;

                            return new GridDhtEmbeddedFuture<>(
                                txFut,
                                new C2<GridCacheReturn<V>, Exception, IgniteInternalFuture<GridNearLockResponse<K, V>>>() {
                                    @Override public IgniteInternalFuture<GridNearLockResponse<K, V>> apply(
                                        GridCacheReturn<V> o, Exception e) {
                                        if (e != null)
                                            e = U.unwrap(e);

                                        assert !t.empty();

                                        // Create response while holding locks.
                                        final GridNearLockResponse<K, V> resp = createLockReply(nearNode,
                                            entries,
                                            req,
                                            t,
                                            t.xidVersion(),
                                            e);

                                        if (resp.error() == null && t.onePhaseCommit()) {
                                            assert t.implicit();

                                            return t.commitAsync().chain(
                                                new C1<IgniteInternalFuture<IgniteInternalTx>, GridNearLockResponse<K, V>>() {
                                                    @Override public GridNearLockResponse<K, V> apply(IgniteInternalFuture<IgniteInternalTx> f) {
                                                        try {
                                                            // Check for error.
                                                            f.get();
                                                        }
                                                        catch (IgniteCheckedException e1) {
                                                            resp.error(e1);
                                                        }

                                                        sendLockReply(nearNode, t, req, resp);

                                                        return resp;
                                                    }
                                                });
                                        }
                                        else {
                                            sendLockReply(nearNode, t, req, resp);

                                            return new GridFinishedFutureEx<>(resp);
                                        }
                                    }
                                },
                                ctx.kernalContext());
                        }
                        else {
                            assert fut != null;

                            // This will send remote messages.
                            fut.map();

                            final GridCacheVersion mappedVer = fut.version();

                            return new GridDhtEmbeddedFuture<>(
                                ctx.kernalContext(),
                                fut,
                                new C2<Boolean, Exception, GridNearLockResponse<K, V>>() {
                                    @Override public GridNearLockResponse<K, V> apply(Boolean b, Exception e) {
                                        if (e != null)
                                            e = U.unwrap(e);
                                        else if (!b)
                                            e = new GridCacheLockTimeoutException(req.version());

                                        GridNearLockResponse<K, V> res = createLockReply(nearNode,
                                            entries,
                                            req,
                                            null,
                                            mappedVer,
                                            e);

                                        sendLockReply(nearNode, null, req, res);

                                        return res;
                                    }
                                });
                        }
                    }
                    catch (IgniteCheckedException e) {
                        String err = "Failed to unmarshal at least one of the keys for lock request message: " + req;

                        U.error(log, err, e);

                        if (tx != null) {
                            try {
                                tx.rollback();
                            }
                            catch (IgniteCheckedException ex) {
                                U.error(log, "Failed to rollback the transaction: " + tx, ex);
                            }
                        }

                        return new GridDhtFinishedFuture<>(ctx.kernalContext(),
                            new IgniteCheckedException(err, e));
                    }
                }
            },
            ctx.kernalContext());
    }

    /**
     * @param nearNode Near node.
     * @param entries Entries.
     * @param req Lock request.
     * @param tx Transaction.
     * @param mappedVer Mapped version.
     * @param err Error.
     * @return Response.
     */
    private GridNearLockResponse<K, V> createLockReply(
        ClusterNode nearNode,
        List<GridCacheEntryEx<K, V>> entries,
        GridNearLockRequest<K, V> req,
        @Nullable GridDhtTxLocalAdapter<K,V> tx,
        GridCacheVersion mappedVer,
        Throwable err) {
        assert mappedVer != null;
        assert tx == null || tx.xidVersion().equals(mappedVer);

        try {
            // Send reply back to originating near node.
            GridNearLockResponse<K, V> res = new GridNearLockResponse<>(ctx.cacheId(),
                req.version(), req.futureId(), req.miniId(), tx != null && tx.onePhaseCommit(), entries.size(), err);

            if (err == null) {
                res.pending(localDhtPendingVersions(entries, mappedVer));

                // We have to add completed versions for cases when nearLocal and remote transactions
                // execute concurrently.
                res.completedVersions(ctx.tm().committedVersions(req.version()),
                    ctx.tm().rolledbackVersions(req.version()));

                int i = 0;

                for (ListIterator<GridCacheEntryEx<K, V>> it = entries.listIterator(); it.hasNext();) {
                    GridCacheEntryEx<K, V> e = it.next();

                    assert e != null;

                    while (true) {
                        try {
                            // Don't return anything for invalid partitions.
                            if (tx == null || !tx.isRollbackOnly()) {
                                GridCacheVersion dhtVer = req.dhtVersion(i);

                                try {
                                    GridCacheVersion ver = e.version();

                                    boolean ret = req.returnValue(i) || dhtVer == null || !dhtVer.equals(ver);

                                    V val = null;

                                    if (ret)
                                        val = e.innerGet(tx,
                                            /*swap*/true,
                                            /*read-through*/ctx.loadPreviousValue(),
                                            /*fail-fast.*/false,
                                            /*unmarshal*/false,
                                            /*update-metrics*/true,
                                            /*event notification*/req.returnValue(i),
                                            /*temporary*/false,
                                            CU.subjectId(tx, ctx.shared()),
                                            null,
                                            tx != null ? tx.resolveTaskName() : null,
                                            CU.<K, V>empty(),
                                            null);

                                    assert e.lockedBy(mappedVer) ||
                                        (ctx.mvcc().isRemoved(e.context(), mappedVer) && req.timeout() > 0) :
                                        "Entry does not own lock for tx [locNodeId=" + ctx.localNodeId() +
                                            ", entry=" + e +
                                            ", mappedVer=" + mappedVer + ", ver=" + ver +
                                            ", tx=" + tx + ", req=" + req +
                                            ", err=" + err + ']';

                                    boolean filterPassed = false;

                                    if (tx != null && tx.onePhaseCommit()) {
                                        IgniteTxEntry<K, V> writeEntry = tx.entry(ctx.txKey(e.key()));

                                        assert writeEntry != null :
                                            "Missing tx entry for locked cache entry: " + e;

                                        filterPassed = writeEntry.filtersPassed();
                                    }

                                    GridCacheValueBytes valBytes = ret ? e.valueBytes(null) : GridCacheValueBytes.nil();

                                    // We include values into response since they are required for local
                                    // calls and won't be serialized. We are also including DHT version.
                                    res.addValueBytes(
                                        val != null ? val : (V)valBytes.getIfPlain(),
                                        ret ? valBytes.getIfMarshaled() : null,
                                        filterPassed,
                                        ver,
                                        mappedVer,
                                        ctx);
                                }
                                catch (GridCacheFilterFailedException ex) {
                                    assert false : "Filter should never fail if fail-fast is false.";

                                    ex.printStackTrace();
                                }
                            }
                            else {
                                // We include values into response since they are required for local
                                // calls and won't be serialized. We are also including DHT version.
                                res.addValueBytes(null, null, false, e.version(), mappedVer, ctx);
                            }

                            break;
                        }
                        catch (GridCacheEntryRemovedException ignore) {
                            if (log.isDebugEnabled())
                                log.debug("Got removed entry when sending reply to DHT lock request " +
                                    "(will retry): " + e);

                            e = entryExx(e.key());

                            it.set(e);
                        }
                    }

                    i++;
                }
            }

            return res;
        }
        catch (IgniteCheckedException e) {
            U.error(log, "Failed to get value for lock reply message for node [node=" +
                U.toShortString(nearNode) + ", req=" + req + ']', e);

            return new GridNearLockResponse<>(ctx.cacheId(), req.version(), req.futureId(), req.miniId(), false,
                entries.size(), e);
        }
    }

    /**
     * Send lock reply back to near node.
     *
     * @param nearNode Near node.
     * @param tx Transaction.
     * @param req Lock request.
     * @param res Lock response.
     */
    private void sendLockReply(
        ClusterNode nearNode,
        @Nullable IgniteInternalTx<K,V> tx,
        GridNearLockRequest<K, V> req,
        GridNearLockResponse<K, V> res
    ) {
        Throwable err = res.error();

        // Log error before sending reply.
        if (err != null && !(err instanceof GridCacheLockTimeoutException))
            U.error(log, "Failed to acquire lock for request: " + req, err);

        try {
            // Don't send reply message to this node or if lock was cancelled.
            if (!nearNode.id().equals(ctx.nodeId()) && !X.hasCause(err, GridDistributedLockCancelledException.class))
                ctx.io().send(nearNode, res, ctx.ioPolicy());
        }
        catch (IgniteCheckedException e) {
            U.error(log, "Failed to send lock reply to originating node (will rollback transaction) [node=" +
                U.toShortString(nearNode) + ", req=" + req + ']', e);

            if (tx != null)
                tx.rollbackAsync();

            // Convert to closure exception as this method is only called form closures.
            throw new GridClosureException(e);
        }
    }

    /**
     * Collects versions of pending candidates versions less then base.
     *
     * @param entries Tx entries to process.
     * @param baseVer Base version.
     * @return Collection of pending candidates versions.
     */
    private Collection<GridCacheVersion> localDhtPendingVersions(Iterable<GridCacheEntryEx<K, V>> entries,
        GridCacheVersion baseVer) {
        Collection<GridCacheVersion> lessPending = new GridLeanSet<>(5);

        for (GridCacheEntryEx<K, V> entry : entries) {
            // Since entries were collected before locks are added, some of them may become obsolete.
            while (true) {
                try {
                    for (GridCacheMvccCandidate cand : entry.localCandidates()) {
                        if (cand.version().isLess(baseVer))
                            lessPending.add(cand.version());
                    }

                    break; // While.
                }
                catch (GridCacheEntryRemovedException ignored) {
                    if (log.isDebugEnabled())
                        log.debug("Got removed entry is localDhtPendingVersions (will retry): " + entry);

                    entry = entryExx(entry.key());
                }
            }
        }

        return lessPending;
    }

    /**
     * @param nodeId Node ID.
     * @param req Request.
     */
    @SuppressWarnings({"RedundantTypeArguments"})
    private void clearLocks(UUID nodeId, GridDistributedUnlockRequest<K, V> req) {
        assert nodeId != null;

        List<K> keys = req.keys();

        if (keys != null) {
            for (K key : keys) {
                while (true) {
                    GridDistributedCacheEntry<K, V> entry = peekExx(key);

                    boolean created = false;

                    if (entry == null) {
                        entry = entryExx(key);

                        created = true;
                    }

                    try {
                        entry.doneRemote(
                            req.version(),
                            req.version(),
                            null,
                            null,
                            null,
                            /*system invalidate*/false);

                        // Note that we don't reorder completed versions here,
                        // as there is no point to reorder relative to the version
                        // we are about to remove.
                        if (entry.removeLock(req.version())) {
                            if (log.isDebugEnabled())
                                log.debug("Removed lock [lockId=" + req.version() + ", key=" + key + ']');
                        }
                        else {
                            if (log.isDebugEnabled())
                                log.debug("Received unlock request for unknown candidate " +
                                    "(added to cancelled locks set): " + req);
                        }

                        if (created && entry.markObsolete(req.version()))
                            removeEntry(entry);

                        ctx.evicts().touch(entry, ctx.affinity().affinityTopologyVersion());

                        break;
                    }
                    catch (GridCacheEntryRemovedException ignored) {
                        if (log.isDebugEnabled())
                            log.debug("Received remove lock request for removed entry (will retry) [entry=" +
                                entry + ", req=" + req + ']');
                    }
                }
            }
        }
    }

    /**
     * @param nodeId Sender ID.
     * @param req Request.
     */
    @SuppressWarnings({"RedundantTypeArguments", "TypeMayBeWeakened"})
    private void processNearUnlockRequest(UUID nodeId, GridNearUnlockRequest<K, V> req) {
        assert isAffinityNode(cacheCfg);
        assert nodeId != null;

        removeLocks(nodeId, req.version(), req.keys(), true);
    }

    /**
     * @param nodeId Sender node ID.
     * @param topVer Topology version.
     * @param cached Entry.
     * @param readers Readers for this entry.
     * @param dhtMap DHT map.
     * @param nearMap Near map.
     * @throws IgniteCheckedException If failed.
     */
    private void map(UUID nodeId,
        long topVer,
        GridCacheEntryEx<K,V> cached,
        Collection<UUID> readers,
        Map<ClusterNode, List<T2<K, byte[]>>> dhtMap,
        Map<ClusterNode, List<T2<K, byte[]>>> nearMap)
        throws IgniteCheckedException {
        Collection<ClusterNode> dhtNodes = ctx.dht().topology().nodes(cached.partition(), topVer);

        ClusterNode primary = F.first(dhtNodes);

        assert primary != null;

        if (!primary.id().equals(ctx.nodeId())) {
            if (log.isDebugEnabled())
                log.debug("Primary node mismatch for unlock [entry=" + cached + ", expected=" + ctx.nodeId() +
                    ", actual=" + U.toShortString(primary) + ']');

            return;
        }

        if (log.isDebugEnabled())
            log.debug("Mapping entry to DHT nodes [nodes=" + U.toShortString(dhtNodes) + ", entry=" + cached + ']');

        Collection<ClusterNode> nearNodes = null;

        if (!F.isEmpty(readers)) {
            nearNodes = ctx.discovery().nodes(readers, F0.not(F.idForNodeId(nodeId)));

            if (log.isDebugEnabled())
                log.debug("Mapping entry to near nodes [nodes=" + U.toShortString(nearNodes) + ", entry=" + cached +
                    ']');
        }
        else {
            if (log.isDebugEnabled())
                log.debug("Entry has no near readers: " + cached);
        }

        map(cached, F.view(dhtNodes, F.remoteNodes(ctx.nodeId())), dhtMap); // Exclude local node.
        map(cached, nearNodes, nearMap);
    }

    /**
     * @param entry Entry.
     * @param nodes Nodes.
     * @param map Map.
     * @throws IgniteCheckedException If failed.
     */
    @SuppressWarnings( {"MismatchedQueryAndUpdateOfCollection"})
    private void map(GridCacheEntryEx<K, V> entry,
        @Nullable Iterable<? extends ClusterNode> nodes,
        Map<ClusterNode, List<T2<K, byte[]>>> map) throws IgniteCheckedException {
        if (nodes != null) {
            for (ClusterNode n : nodes) {
                List<T2<K, byte[]>> keys = map.get(n);

                if (keys == null)
                    map.put(n, keys = new LinkedList<>());

                keys.add(new T2<>(entry.key(), entry.getOrMarshalKeyBytes()));
            }
        }
    }

    /**
     * @param nodeId Node ID.
     * @param ver Version.
     * @param keys Keys.
     * @param unmap Flag for un-mapping version.
     */
    public void removeLocks(UUID nodeId, GridCacheVersion ver, Iterable<? extends K> keys, boolean unmap) {
        assert nodeId != null;
        assert ver != null;

        if (F.isEmpty(keys))
            return;

        // Remove mapped versions.
        GridCacheVersion dhtVer = unmap ? ctx.mvcc().unmapVersion(ver) : ver;

        Map<ClusterNode, List<T2<K, byte[]>>> dhtMap = new HashMap<>();
        Map<ClusterNode, List<T2<K, byte[]>>> nearMap = new HashMap<>();

        GridCacheVersion obsoleteVer = null;

        for (K key : keys) {
            while (true) {
                boolean created = false;

                GridDhtCacheEntry<K, V> entry = peekExx(key);

                if (entry == null) {
                    entry = entryExx(key);

                    created = true;
                }

                try {
                    GridCacheMvccCandidate<K> cand = null;

                    if (dhtVer == null) {
                        cand = entry.localCandidateByNearVersion(ver, true);

                        if (cand != null)
                            dhtVer = cand.version();
                        else {
                            if (log.isDebugEnabled())
                                log.debug("Failed to locate lock candidate based on dht or near versions [nodeId=" +
                                    nodeId + ", ver=" + ver + ", unmap=" + unmap + ", keys=" + keys + ']');

                            entry.removeLock(ver);

                            if (created) {
                                if (obsoleteVer == null)
                                    obsoleteVer = ctx.versions().next();

                                if (entry.markObsolete(obsoleteVer))
                                    removeEntry(entry);
                            }

                            break;
                        }
                    }

                    if (cand == null)
                        cand = entry.candidate(dhtVer);

                    long topVer = cand == null ? -1 : cand.topologyVersion();

                    // Note that we obtain readers before lock is removed.
                    // Even in case if entry would be removed just after lock is removed,
                    // we must send release messages to backups and readers.
                    Collection<UUID> readers = entry.readers();

                    // Note that we don't reorder completed versions here,
                    // as there is no point to reorder relative to the version
                    // we are about to remove.
                    if (entry.removeLock(dhtVer)) {
                        // Map to backups and near readers.
                        map(nodeId, topVer, entry, readers, dhtMap, nearMap);

                        if (log.isDebugEnabled())
                            log.debug("Removed lock [lockId=" + ver + ", key=" + key + ']');
                    }
                    else if (log.isDebugEnabled())
                        log.debug("Received unlock request for unknown candidate " +
                            "(added to cancelled locks set) [ver=" + ver + ", entry=" + entry + ']');

                    if (created && entry.markObsolete(dhtVer))
                        removeEntry(entry);

                    ctx.evicts().touch(entry, topVer);

                    break;
                }
                catch (GridCacheEntryRemovedException ignored) {
                    if (log.isDebugEnabled())
                        log.debug("Received remove lock request for removed entry (will retry): " + entry);
                }
                catch (IgniteCheckedException e) {
                    U.error(log, "Failed to remove locks for keys: " + keys, e);
                }
            }
        }

        Collection<GridCacheVersion> committed = ctx.tm().committedVersions(ver);
        Collection<GridCacheVersion> rolledback = ctx.tm().rolledbackVersions(ver);

        // Backups.
        for (Map.Entry<ClusterNode, List<T2<K, byte[]>>> entry : dhtMap.entrySet()) {
            ClusterNode n = entry.getKey();

            List<T2<K, byte[]>> keyBytes = entry.getValue();

            GridDhtUnlockRequest<K, V> req = new GridDhtUnlockRequest<>(ctx.cacheId(), keyBytes.size());

            req.version(dhtVer);

            try {
                for (T2<K, byte[]> key : keyBytes)
                    req.addKey(key.get1(), key.get2(), ctx);

                keyBytes = nearMap.get(n);

                if (keyBytes != null)
                    for (T2<K, byte[]> key : keyBytes)
                        req.addNearKey(key.get1(), key.get2(), ctx.shared());

                req.completedVersions(committed, rolledback);

                ctx.io().send(n, req, ctx.ioPolicy());
            }
            catch (ClusterTopologyCheckedException ignore) {
                if (log.isDebugEnabled())
                    log.debug("Node left while sending unlock request: " + n);
            }
            catch (IgniteCheckedException e) {
                U.error(log, "Failed to send unlock request to node (will make best effort to complete): " + n, e);
            }
        }

        // Readers.
        for (Map.Entry<ClusterNode, List<T2<K, byte[]>>> entry : nearMap.entrySet()) {
            ClusterNode n = entry.getKey();

            if (!dhtMap.containsKey(n)) {
                List<T2<K, byte[]>> keyBytes = entry.getValue();

                GridDhtUnlockRequest<K, V> req = new GridDhtUnlockRequest<>(ctx.cacheId(), keyBytes.size());

                req.version(dhtVer);

                try {
                    for (T2<K, byte[]> key : keyBytes)
                        req.addNearKey(key.get1(), key.get2(), ctx.shared());

                    req.completedVersions(committed, rolledback);

                    ctx.io().send(n, req, ctx.ioPolicy());
                }
                catch (ClusterTopologyCheckedException ignore) {
                    if (log.isDebugEnabled())
                        log.debug("Node left while sending unlock request: " + n);
                }
                catch (IgniteCheckedException e) {
                    U.error(log, "Failed to send unlock request to node (will make best effort to complete): " + n, e);
                }
            }
        }
    }

    /**
     * @param key Key
     * @param ver Version.
     * @throws IgniteCheckedException If invalidate failed.
     */
    private void invalidateNearEntry(K key, GridCacheVersion ver) throws IgniteCheckedException {
        GridCacheEntryEx<K, V> nearEntry = near().peekEx(key);

        if (nearEntry != null)
            nearEntry.invalidate(null, ver);
    }

    /**
     * @param key Key
     * @param ver Version.
     */
    private void obsoleteNearEntry(K key, GridCacheVersion ver) {
        GridCacheEntryEx<K, V> nearEntry = near().peekEx(key);

        if (nearEntry != null)
            nearEntry.markObsolete(ver);
    }
}
