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

package org.apache.ignite.internal.processors.cache.distributed.near;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.processors.cache.distributed.*;
import org.apache.ignite.internal.processors.cache.distributed.dht.*;
import org.apache.ignite.internal.processors.cache.transactions.*;
import org.apache.ignite.internal.processors.cache.version.*;
import org.apache.ignite.internal.transactions.*;
import org.apache.ignite.internal.util.future.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.plugin.security.*;
import org.apache.ignite.transactions.*;
import org.jetbrains.annotations.*;

import javax.cache.*;
import java.io.*;
import java.util.*;

import static org.apache.ignite.internal.processors.cache.CacheFlag.*;
import static org.apache.ignite.transactions.IgniteTxConcurrency.*;

/**
 * Near cache for transactional cache.
 */
public class GridNearTransactionalCache<K, V> extends GridNearCacheAdapter<K, V> {
    /** */
    private static final long serialVersionUID = 0L;

    /** DHT cache. */
    private GridDhtCache<K, V> dht;

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    public GridNearTransactionalCache() {
        // No-op.
    }

    /**
     * @param ctx Context.
     */
    public GridNearTransactionalCache(GridCacheContext<K, V> ctx) {
        super(ctx);
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteCheckedException {
        super.start();

        ctx.io().addHandler(ctx.cacheId(), GridNearGetResponse.class, new CI2<UUID, GridNearGetResponse<K, V>>() {
            @Override public void apply(UUID nodeId, GridNearGetResponse<K, V> res) {
                processGetResponse(nodeId, res);
            }
        });

        ctx.io().addHandler(ctx.cacheId(), GridNearLockResponse.class, new CI2<UUID, GridNearLockResponse<K, V>>() {
            @Override public void apply(UUID nodeId, GridNearLockResponse<K, V> res) {
                processLockResponse(nodeId, res);
            }
        });
    }

    /**
     * @param dht DHT cache.
     */
    public void dht(GridDhtCache<K, V> dht) {
        this.dht = dht;
    }

    /** {@inheritDoc} */
    @Override public GridDhtCache<K, V> dht() {
        return dht;
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Map<K, V>> getAllAsync(
        @Nullable final Collection<? extends K> keys,
        boolean forcePrimary,
        boolean skipTx,
        @Nullable final GridCacheEntryEx<K, V> entry,
        @Nullable UUID subjId,
        String taskName,
        final boolean deserializePortable,
        @Nullable final IgnitePredicate<Cache.Entry<K, V>>[] filter
    ) {
        ctx.denyOnFlag(LOCAL);
        ctx.checkSecurity(GridSecurityPermission.CACHE_READ);

        if (F.isEmpty(keys))
            return new GridFinishedFuture<>(ctx.kernalContext(), Collections.<K, V>emptyMap());

        IgniteTxLocalAdapter<K, V> tx = ctx.tm().threadLocalTx();

        if (tx != null && !tx.implicit() && !skipTx) {
            return asyncOp(tx, new AsyncOp<Map<K, V>>(keys) {
                @Override public IgniteInternalFuture<Map<K, V>> op(IgniteTxLocalAdapter<K, V> tx) {
                    return ctx.wrapCloneMap(tx.getAllAsync(ctx, keys, entry, deserializePortable, filter));
                }
            });
        }

        GridCacheProjectionImpl<K, V> prj = ctx.projectionPerCall();

        subjId = ctx.subjectIdPerCall(subjId, prj);

        return loadAsync(null,
            keys,
            false,
            forcePrimary,
            filter,
            subjId,
            taskName,
            deserializePortable,
            prj != null ? prj.expiry() : null);
    }

    /**
     * @param tx Transaction.
     * @param keys Keys to load.
     * @param readThrough Read through flag.
     * @param filter Filter.
     * @param deserializePortable Deserialize portable flag.
     * @param expiryPlc Expiry policy.
     * @return Future.
     */
    IgniteInternalFuture<Map<K, V>> txLoadAsync(GridNearTxLocal<K, V> tx,
        @Nullable Collection<? extends K> keys,
        boolean readThrough,
        @Nullable IgnitePredicate<Cache.Entry<K, V>>[] filter,
        boolean deserializePortable,
        @Nullable IgniteCacheExpiryPolicy expiryPlc) {
        assert tx != null;

        GridNearGetFuture<K, V> fut = new GridNearGetFuture<>(ctx,
            keys,
            readThrough,
            false,
            false,
            tx,
            filter,
            CU.subjectId(tx, ctx.shared()),
            tx.resolveTaskName(),
            deserializePortable,
            expiryPlc);

        // init() will register future for responses if it has remote mappings.
        fut.init();

        return fut;
    }

    /**
     * @param nodeId Node ID.
     * @param req Request.
     */
    @SuppressWarnings({"RedundantTypeArguments"})
    public void clearLocks(UUID nodeId, GridDhtUnlockRequest<K, V> req) {
        assert nodeId != null;

        GridCacheVersion obsoleteVer = ctx.versions().next();

        List<K> keys = req.nearKeys();

        if (keys != null) {
            long topVer = ctx.affinity().affinityTopologyVersion();

            for (K key : keys) {
                while (true) {
                    GridDistributedCacheEntry<K, V> entry = peekExx(key);

                    try {
                        if (entry != null) {
                            entry.doneRemote(
                                req.version(),
                                req.version(),
                                null,
                                req.committedVersions(),
                                req.rolledbackVersions(),
                                /*system invalidate*/false);

                            // Note that we don't reorder completed versions here,
                            // as there is no point to reorder relative to the version
                            // we are about to remove.
                            if (entry.removeLock(req.version())) {
                                if (log.isDebugEnabled())
                                    log.debug("Removed lock [lockId=" + req.version() + ", key=" + key + ']');

                                // Try to evict near entry dht-mapped locally.
                                evictNearEntry(entry, obsoleteVer, topVer);
                            }
                            else {
                                if (log.isDebugEnabled())
                                    log.debug("Received unlock request for unknown candidate " +
                                        "(added to cancelled locks set): " + req);
                            }

                            ctx.evicts().touch(entry, topVer);
                        }
                        else if (log.isDebugEnabled())
                            log.debug("Received unlock request for entry that could not be found: " + req);

                        break;
                    }
                    catch (GridCacheEntryRemovedException ignored) {
                        if (log.isDebugEnabled())
                            log.debug("Received remove lock request for removed entry (will retry) [entry=" + entry +
                                ", req=" + req + ']');
                    }
                }
            }
        }
    }

    /**
     * @param nodeId Primary node ID.
     * @param req Request.
     * @return Remote transaction.
     * @throws IgniteCheckedException If failed.
     * @throws GridDistributedLockCancelledException If lock has been cancelled.
     */
    @SuppressWarnings({"RedundantTypeArguments"})
    @Nullable public GridNearTxRemote<K, V> startRemoteTx(UUID nodeId, GridDhtLockRequest<K, V> req)
        throws IgniteCheckedException, GridDistributedLockCancelledException {
        List<K> nearKeys = req.nearKeys();
        List<byte[]> keyBytes = req.nearKeyBytes();

        assert keyBytes != null;

        GridNearTxRemote<K, V> tx = null;

        ClassLoader ldr = ctx.deploy().globalLoader();

        if (ldr != null) {
            Collection<IgniteTxKey<K>> evicted = null;

            for (int i = 0; i < nearKeys.size(); i++) {
                K key = nearKeys.get(i);

                if (key == null)
                    continue;

                IgniteTxKey<K> txKey = ctx.txKey(key);

                byte[] bytes = !keyBytes.isEmpty() ? keyBytes.get(i) : null;

                Collection<GridCacheMvccCandidate<K>> cands = req.candidatesByIndex(i);
                GridCacheVersion drVer = req.drVersionByIndex(i);

                if (log.isDebugEnabled())
                    log.debug("Unmarshalled key: " + key);

                GridNearCacheEntry<K, V> entry = null;

                while (true) {
                    try {
                        entry = peekExx(key);

                        if (entry != null) {
                            entry.keyBytes(bytes);

                            // Handle implicit locks for pessimistic transactions.
                            if (req.inTx()) {
                                tx = ctx.tm().nearTx(req.version());

                                if (tx == null) {
                                    tx = new GridNearTxRemote<>(
                                        ctx.shared(),
                                        nodeId,
                                        req.nearNodeId(),
                                        req.nearXidVersion(),
                                        req.threadId(),
                                        req.version(),
                                        null,
                                        ctx.system(),
                                        PESSIMISTIC,
                                        req.isolation(),
                                        req.isInvalidate(),
                                        req.timeout(),
                                        req.txSize(),
                                        req.groupLockKey(),
                                        req.subjectId(),
                                        req.taskNameHash()
                                    );

                                    if (req.groupLock())
                                        tx.groupLockKey(txKey);

                                    tx = ctx.tm().onCreated(tx);

                                    if (tx == null || !ctx.tm().onStarted(tx))
                                        throw new IgniteTxRollbackCheckedException("Failed to acquire lock " +
                                            "(transaction has been completed): " + req.version());
                                }

                                tx.addEntry(ctx, txKey, bytes, GridCacheOperation.NOOP, /*Value.*/null,
                                    /*Value byts.*/null, drVer);
                            }

                            // Add remote candidate before reordering.
                            // Owned candidates should be reordered inside entry lock.
                            entry.addRemote(
                                req.nodeId(),
                                nodeId,
                                req.threadId(),
                                req.version(),
                                req.timeout(),
                                tx != null,
                                tx != null && tx.implicitSingle(),
                                req.owned(entry.key())
                            );

                            assert cands.isEmpty() : "Received non-empty candidates in dht lock request: " + cands;

                            if (!req.inTx())
                                ctx.evicts().touch(entry, req.topologyVersion());
                        }
                        else {
                            if (evicted == null)
                                evicted = new LinkedList<>();

                            evicted.add(txKey);
                        }

                        // Double-check in case if sender node left the grid.
                        if (ctx.discovery().node(req.nodeId()) == null) {
                            if (log.isDebugEnabled())
                                log.debug("Node requesting lock left grid (lock request will be ignored): " + req);

                            if (tx != null)
                                tx.rollback();

                            return null;
                        }

                        // Entry is legit.
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

            if (tx != null && evicted != null) {
                assert !evicted.isEmpty();

                for (IgniteTxKey<K> evict : evicted)
                    tx.addEvicted(evict);
            }
        }
        else {
            String err = "Failed to acquire deployment class loader for message: " + req;

            U.warn(log, err);

            throw new IgniteCheckedException(err);
        }

        return tx;
    }

    /**
     * @param nodeId Node ID.
     * @param res Response.
     */
    private void processLockResponse(UUID nodeId, GridNearLockResponse<K, V> res) {
        assert nodeId != null;
        assert res != null;

        GridNearLockFuture<K, V> fut = (GridNearLockFuture<K, V>)ctx.mvcc().<Boolean>future(res.version(),
            res.futureId());

        if (fut != null)
            fut.onResult(nodeId, res);
    }

    /** {@inheritDoc} */
    @Override protected IgniteInternalFuture<Boolean> lockAllAsync(
        Collection<? extends K> keys,
        long timeout,
        IgniteTxLocalEx<K, V> tx,
        boolean isInvalidate,
        boolean isRead,
        boolean retval,
        IgniteTxIsolation isolation,
        long accessTtl,
        IgnitePredicate<Cache.Entry<K, V>>[] filter
    ) {
        GridNearLockFuture<K, V> fut = new GridNearLockFuture<>(ctx,
            keys,
            (GridNearTxLocal<K, V>)tx,
            isRead,
            retval,
            timeout,
            accessTtl,
            filter);

        if (!ctx.mvcc().addFuture(fut))
            throw new IllegalStateException("Duplicate future ID: " + fut);

        fut.map();

        return fut;
    }

    /**
     * @param e Transaction entry.
     * @param topVer Topology version.
     * @return {@code True} if entry is locally mapped as a primary or back up node.
     */
    protected boolean isNearLocallyMapped(GridCacheEntryEx<K, V> e, long topVer) {
        return ctx.affinity().belongs(ctx.localNode(), e.key(), topVer);
    }

    /**
     *
     * @param e Entry to evict if it qualifies for eviction.
     * @param obsoleteVer Obsolete version.
     * @param topVer Topology version.
     * @return {@code True} if attempt was made to evict the entry.
     */
    protected boolean evictNearEntry(GridCacheEntryEx<K, V> e, GridCacheVersion obsoleteVer, long topVer) {
        assert e != null;
        assert obsoleteVer != null;

        if (isNearLocallyMapped(e, topVer)) {
            if (log.isDebugEnabled())
                log.debug("Evicting dht-local entry from near cache [entry=" + e + ", tx=" + this + ']');

            if (e.markObsolete(obsoleteVer))
                return true;
        }

        return false;
    }

    /** {@inheritDoc} */
    @Override public void unlockAll(Collection<? extends K> keys, IgnitePredicate<Cache.Entry<K, V>>[] filter) {
        if (keys.isEmpty())
            return;

        try {
            GridCacheVersion ver = null;

            int keyCnt = -1;

            Map<ClusterNode, GridNearUnlockRequest<K, V>> map = null;

            Collection<K> locKeys = new LinkedList<>();

            for (K key : keys) {
                while (true) {
                    GridDistributedCacheEntry<K, V> entry = peekExx(key);

                    if (entry == null || !ctx.isAll(entry.wrapLazyValue(), filter))
                        break; // While.

                    try {
                        GridCacheMvccCandidate<K> cand = entry.candidate(ctx.nodeId(), Thread.currentThread().getId());

                        long topVer = -1;

                        if (cand != null) {
                            assert cand.nearLocal() : "Got non-near-local candidate in near cache: " + cand;

                            ver = cand.version();

                            if (map == null) {
                                Collection<ClusterNode> affNodes = CU.allNodes(ctx, cand.topologyVersion());

                                if (F.isEmpty(affNodes))
                                    return;

                                keyCnt = (int)Math.ceil((double)keys.size() / affNodes.size());

                                map = U.newHashMap(affNodes.size());
                            }

                            topVer = cand.topologyVersion();

                            // Send request to remove from remote nodes.
                            ClusterNode primary = ctx.affinity().primary(key, topVer);

                            GridNearUnlockRequest<K, V> req = map.get(primary);

                            if (req == null) {
                                map.put(primary, req = new GridNearUnlockRequest<>(ctx.cacheId(), keyCnt));

                                req.version(ver);
                            }

                            // Remove candidate from local node first.
                            GridCacheMvccCandidate<K> rmv = entry.removeLock();

                            if (rmv != null) {
                                if (!rmv.reentry()) {
                                    if (ver != null && !ver.equals(rmv.version()))
                                        throw new IgniteCheckedException("Failed to unlock (if keys were locked separately, " +
                                            "then they need to be unlocked separately): " + keys);

                                    if (!primary.isLocal()) {
                                        assert req != null;

                                        req.addKey(
                                            entry.key(),
                                            entry.getOrMarshalKeyBytes(),
                                            ctx);
                                    }
                                    else
                                        locKeys.add(key);

                                    if (log.isDebugEnabled())
                                        log.debug("Removed lock (will distribute): " + rmv);
                                }
                                else if (log.isDebugEnabled())
                                    log.debug("Current thread still owns lock (or there are no other nodes)" +
                                        " [lock=" + rmv + ", curThreadId=" + Thread.currentThread().getId() + ']');
                            }
                        }

                        assert topVer != -1 || cand == null;

                        if (topVer == -1)
                            topVer = ctx.affinity().affinityTopologyVersion();

                        ctx.evicts().touch(entry, topVer);

                        break;
                    }
                    catch (GridCacheEntryRemovedException ignore) {
                        if (log.isDebugEnabled())
                            log.debug("Attempted to unlock removed entry (will retry): " + entry);
                    }
                }
            }

            if (ver == null)
                return;

            for (Map.Entry<ClusterNode, GridNearUnlockRequest<K, V>> mapping : map.entrySet()) {
                ClusterNode n = mapping.getKey();

                GridDistributedUnlockRequest<K, V> req = mapping.getValue();

                if (n.isLocal())
                    dht.removeLocks(ctx.nodeId(), req.version(), locKeys, true);
                else if (!F.isEmpty(req.keyBytes()) || !F.isEmpty(req.keys()))
                    // We don't wait for reply to this message.
                    ctx.io().send(n, req, ctx.ioPolicy());
            }
        }
        catch (IgniteCheckedException ex) {
            U.error(log, "Failed to unlock the lock for keys: " + keys, ex);
        }
    }

    /**
     * Removes locks regardless of whether they are owned or not for given
     * version and keys.
     *
     * @param ver Lock version.
     * @param keys Keys.
     */
    @SuppressWarnings({"unchecked"})
    public void removeLocks(GridCacheVersion ver, Collection<? extends K> keys) {
        if (keys.isEmpty())
            return;

        try {
            int keyCnt = -1;

            Map<ClusterNode, GridNearUnlockRequest<K, V>> map = null;

            for (K key : keys) {
                // Send request to remove from remote nodes.
                GridNearUnlockRequest<K, V> req = null;

                while (true) {
                    GridDistributedCacheEntry<K, V> entry = peekExx(key);

                    try {
                        if (entry != null) {
                            GridCacheMvccCandidate<K> cand = entry.candidate(ver);

                            if (cand != null) {
                                if (map == null) {
                                    Collection<ClusterNode> affNodes = CU.allNodes(ctx, cand.topologyVersion());

                                    if (F.isEmpty(affNodes))
                                        return;

                                    keyCnt = (int)Math.ceil((double)keys.size() / affNodes.size());

                                    map = U.newHashMap(affNodes.size());
                                }

                                ClusterNode primary = ctx.affinity().primary(key, cand.topologyVersion());

                                if (!primary.isLocal()) {
                                    req = map.get(primary);

                                    if (req == null) {
                                        map.put(primary, req = new GridNearUnlockRequest<>(ctx.cacheId(), keyCnt));

                                        req.version(ver);
                                    }
                                }

                                // Remove candidate from local node first.
                                if (entry.removeLock(cand.version())) {
                                    if (primary.isLocal()) {
                                        dht.removeLocks(primary.id(), ver, F.asList(key), true);

                                        assert req == null;

                                        continue;
                                    }

                                    req.addKey(
                                        entry.key(),
                                        entry.getOrMarshalKeyBytes(),
                                        ctx);
                                }
                            }
                        }

                        break;
                    }
                    catch (GridCacheEntryRemovedException ignored) {
                        if (log.isDebugEnabled())
                            log.debug("Attempted to remove lock from removed entry (will retry) [rmvVer=" +
                                ver + ", entry=" + entry + ']');
                    }
                }
            }

            if (map == null || map.isEmpty())
                return;

            Collection<GridCacheVersion> committed = ctx.tm().committedVersions(ver);
            Collection<GridCacheVersion> rolledback = ctx.tm().rolledbackVersions(ver);

            for (Map.Entry<ClusterNode, GridNearUnlockRequest<K, V>> mapping : map.entrySet()) {
                ClusterNode n = mapping.getKey();

                GridDistributedUnlockRequest<K, V> req = mapping.getValue();

                if (!F.isEmpty(req.keyBytes()) || !F.isEmpty(req.keys())) {
                    req.completedVersions(committed, rolledback);

                    // We don't wait for reply to this message.
                    ctx.io().send(n, req, ctx.ioPolicy());
                }
            }
        }
        catch (IgniteCheckedException ex) {
            U.error(log, "Failed to unlock the lock for keys: " + keys, ex);
        }
    }

    /** {@inheritDoc} */
    @Override public void onDeferredDelete(GridCacheEntryEx<K, V> entry, GridCacheVersion ver) {
        assert false : "Should not be called";
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridNearTransactionalCache.class, this);
    }
}
