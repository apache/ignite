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
package org.apache.ignite.internal.processors.cache.mvcc;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedManagerAdapter;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.mvcc.txlog.TxKey;
import org.apache.ignite.internal.processors.cache.query.continuous.CacheContinuousQueryListener;
import org.apache.ignite.internal.processors.cache.query.continuous.CacheContinuousQueryManager;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.typedef.F;
import org.jetbrains.annotations.Nullable;
import org.jsr166.ConcurrentLinkedHashMap;

import static org.apache.ignite.internal.processors.dr.GridDrType.DR_BACKUP;
import static org.apache.ignite.internal.processors.dr.GridDrType.DR_PRIMARY;

/**
 * Manager for caching MVCC transaction updates.
 * This updates can be used further in CQ, DR and other places.
 */
public class MvccTransactionEnlistCachingManager extends GridCacheSharedManagerAdapter {
    /** Maximum possible transaction size when caching is enabled. */
    private static final int TX_SIZE_THRESHOLD = 20_000;

    /** Cached enlist values*/
    private Map<GridCacheVersion, Map<KeyCacheObject, MvccTxEnlistEntry>> enlistCache = new ConcurrentHashMap<>();

    /** Counters map. Used for OOM prevention caused by the big transactions. */
    private Map<TxKey, AtomicInteger> cntrs = new ConcurrentHashMap<>();

    // TODO Remove.
    private AtomicLong fakeUpdCntr = new AtomicLong();

    /**
     * Adds enlisted tx entry to cache.
     * @param key Key.
     * @param val Value.
     * @param ttl Time to live.
     * @param expireTime Expire time.
     * @param ver Version.
     * @param oldVal Old value.
     * @param primary Flag whether this is a primary node.
     * @param topVer Topology version.
     * @param mvccVer Mvcc version.
     * @param cacheId Cache id.
     * @param tx Transaction.
     * @throws IgniteCheckedException If failed.
     */
    public void addEnlisted(KeyCacheObject key,
        @Nullable CacheObject val,
        long ttl,
        long expireTime,
        GridCacheVersion ver,
        CacheObject oldVal,
        boolean primary,
        AffinityTopologyVersion topVer,
        MvccVersion mvccVer,
        int cacheId,
        IgniteInternalTx tx) throws IgniteCheckedException {
        assert key != null;
        assert mvccVer != null;
        assert tx != null;

        GridCacheContext ctx0 = cctx.cacheContext(cacheId);
        CacheContinuousQueryManager contQryMgr = ctx0.continuousQueries();

        // Do not cache updates if no DR or CQ enabled.
        if ((!ctx0.isDrEnabled() || key.internal()) &&
            (!contQryMgr.notifyContinuousQueries(tx) || F.isEmpty(contQryMgr.updateListeners(false, false))))
            return;

        AtomicInteger cntr = cntrs.computeIfAbsent(new TxKey(mvccVer.coordinatorVersion(), mvccVer.counter()),
            v -> new AtomicInteger());

        if (cntr.incrementAndGet() > TX_SIZE_THRESHOLD)
            throw new IgniteCheckedException("Transaction is too large. Consider reducing transaction size or " +
                "turning off continuous queries and datacenter replication [size=" + cntr.get() + ", txXid=" + ver + ']');

        MvccTxEnlistEntry e = new MvccTxEnlistEntry(key, val, ttl, expireTime, ver, oldVal, primary, topVer, mvccVer, cacheId);

        Map<KeyCacheObject, MvccTxEnlistEntry> cached = enlistCache.computeIfAbsent(ver,
            v -> new ConcurrentLinkedHashMap<>());

        // TODO REORDERING!!!!
        cached.put(key, e);
    }

    /**
     *
     * @param tx Transaction.
     * @param commit {@code True} if commit.
     */
    public void onTxFinished(IgniteInternalTx tx, boolean commit) {
        if (tx.system() || tx.internal() || tx.mvccSnapshot() == null)
            return;

        cntrs.remove(new TxKey(tx.mvccSnapshot().coordinatorVersion(), tx.mvccSnapshot().counter()));

        Map<KeyCacheObject, MvccTxEnlistEntry> cached = enlistCache.remove(tx.xidVersion());

        if (F.isEmpty(cached) || !commit)
            return;

        // Feed CQ & DR with entries.
        for (Map.Entry<KeyCacheObject, MvccTxEnlistEntry> entry : cached.entrySet()) {
            MvccTxEnlistEntry e = entry.getValue();

            assert e.key().partition() != -1;

            GridCacheContext ctx0 = cctx.cacheContext(e.cacheId());

            assert ctx0 != null;

            // DR
            if (ctx0.isDrEnabled()) {
                try {
                    ctx0.dr().replicate(e.key(), e.value(), e.ttl(), e.expireTime(), e.version(),
                        tx.local() ? DR_PRIMARY : DR_BACKUP, e.topologyVersion());
                }
                catch (IgniteCheckedException ex) {
                    log.error("Failed to replicate entry [entry=" + e + ']', ex);
                }
            }

            // CQ
            CacheContinuousQueryManager contQryMgr = ctx0.continuousQueries();

            if (contQryMgr.notifyContinuousQueries(tx)) {
                contQryMgr.getListenerReadLock().lock();

                try {
                    Map<UUID, CacheContinuousQueryListener> lsnrCol = contQryMgr.updateListeners(false, false);

                    if (!F.isEmpty(lsnrCol)) {
                        try {
                            contQryMgr.onEntryUpdated(
                                lsnrCol,
                                e.key(),
                                e.value(),
                                e.oldValue(),
                                false,
                                e.key().partition(),
                                tx.local(),
                                false,
                                fakeUpdCntr.incrementAndGet(),
                                null,
                                e.topologyVersion());
                        }
                        catch (IgniteCheckedException ex) {
                            log.error("Failed to notify listeners [entry=" + e + ']', ex);
                        }
                    }
                }
                finally {
                    contQryMgr.getListenerReadLock().unlock();
                }
            }
        }
    }
}
