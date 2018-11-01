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

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedManagerAdapter;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.distributed.dht.PartitionUpdateCountersMessage;
import org.apache.ignite.internal.processors.cache.mvcc.txlog.TxKey;
import org.apache.ignite.internal.processors.cache.query.continuous.CacheContinuousQueryListener;
import org.apache.ignite.internal.processors.cache.query.continuous.CacheContinuousQueryManager;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.processors.cache.transactions.TxCounters;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_MVCC_TX_SIZE_CACHING_THRESHOLD;
import static org.apache.ignite.internal.processors.dr.GridDrType.DR_BACKUP;
import static org.apache.ignite.internal.processors.dr.GridDrType.DR_PRIMARY;

/**
 * Manager for caching MVCC transaction updates.
 * This updates can be used further in CQ, DR and other places.
 */
public class MvccCachingManager extends GridCacheSharedManagerAdapter {
    /** Maximum possible transaction size when caching is enabled. */
    public static final int TX_SIZE_THRESHOLD = IgniteSystemProperties.getInteger(IGNITE_MVCC_TX_SIZE_CACHING_THRESHOLD,
        20_000);

    /** Cached enlist values*/
    private final Map<GridCacheVersion, EnlistBuffer> enlistCache = new ConcurrentHashMap<>();

    /** Counters map. Used for OOM prevention caused by the big transactions. */
    private final Map<TxKey, AtomicInteger> cntrs = new ConcurrentHashMap<>();

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
     * @param futId Dht future id.
     * @param batchNum Batch number (for batches reordering prevention).
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
        IgniteInternalTx tx,
        IgniteUuid futId,
        int batchNum) throws IgniteCheckedException {
        assert key != null;
        assert mvccVer != null;
        assert tx != null;

        if (log.isDebugEnabled()) {
            log.debug("Added entry to mvcc cache: [key=" + key + ", val=" + val + ", oldVal=" + oldVal +
                ", primary=" + primary + ", mvccVersion=" + mvccVer + ", cacheId=" + cacheId + ", ver=" + ver +']');
        }

        GridCacheContext ctx0 = cctx.cacheContext(cacheId);

        // Do not cache updates if there is no DR or CQ enabled.
        if (!needDrReplicate(ctx0, key) &&
            F.isEmpty(continuousQueryListeners(ctx0, tx, key)) &&
            !ctx0.group().hasContinuousQueryCaches())
            return;

        AtomicInteger cntr = cntrs.computeIfAbsent(new TxKey(mvccVer.coordinatorVersion(), mvccVer.counter()),
            v -> new AtomicInteger());

        if (cntr.incrementAndGet() > TX_SIZE_THRESHOLD)
            throw new IgniteCheckedException("Transaction is too large. Consider reducing transaction size or " +
                "turning off continuous queries and datacenter replication [size=" + cntr.get() + ", txXid=" + ver + ']');

        MvccTxEntry e = new MvccTxEntry(key, val, ttl, expireTime, ver, oldVal, primary, topVer, mvccVer, cacheId);

        EnlistBuffer cached = enlistCache.computeIfAbsent(ver, v -> new EnlistBuffer());

        cached.add(primary ? null : futId, primary ? -1 : batchNum, key, e);
    }

    /**
     *
     * @param tx Transaction.
     * @param commit {@code True} if commit.
     */
    public void onTxFinished(IgniteInternalTx tx, boolean commit) throws IgniteCheckedException {
        if (log.isDebugEnabled())
            log.debug("Transaction finished: [commit=" + commit + ", tx=" + tx  + ']');

        if (tx.system() || tx.internal() || tx.mvccSnapshot() == null)
            return;

        cntrs.remove(new TxKey(tx.mvccSnapshot().coordinatorVersion(), tx.mvccSnapshot().counter()));

        EnlistBuffer buf = enlistCache.remove(tx.xidVersion());

        if (buf == null)
            return;

        Map<KeyCacheObject, MvccTxEntry> cached = buf.getCached();

        if (F.isEmpty(cached) || !commit)
            return;

        TxCounters txCntrs = tx.txCounters(false);

        assert txCntrs != null;

        Collection<PartitionUpdateCountersMessage> cntrsColl =  txCntrs.updateCounters();

        assert  !F.isEmpty(cntrsColl) : cntrsColl;

        // cacheId -> partId -> initCntr -> cntr + delta.
        Map<Integer, Map<Integer, T2<AtomicLong, Long>>> cntrsMap = new HashMap<>();

        for (PartitionUpdateCountersMessage msg : cntrsColl) {
            for (int i = 0; i < msg.size(); i++) {
                Map<Integer, T2<AtomicLong, Long>> cntrPerPart =
                    cntrsMap.computeIfAbsent(msg.cacheId(), k -> new HashMap<>());

                T2 prev = cntrPerPart.put(msg.partition(i),
                    new T2<>(new AtomicLong(msg.initialCounter(i)), msg.initialCounter(i) + msg.updatesCount(i)));

                assert prev == null;
            }
        }

        // Feed CQ & DR with entries.
        for (Map.Entry<KeyCacheObject, MvccTxEntry> entry : cached.entrySet()) {
            MvccTxEntry e = entry.getValue();

            assert e.key().partition() != -1;

            Map<Integer, T2<AtomicLong, Long>> cntrPerCache = cntrsMap.get(e.cacheId());

            GridCacheContext ctx0 = cctx.cacheContext(e.cacheId());

            assert ctx0 != null && cntrPerCache != null;

            T2<AtomicLong, Long> cntr = cntrPerCache.get(e.key().partition());

            long resCntr = cntr.getKey().incrementAndGet();

            assert resCntr <= cntr.getValue();

            e.updateCounter(resCntr);

            if (ctx0.group().sharedGroup()) {
                ctx0.group().onPartitionCounterUpdate(ctx0.cacheId(), e.key().partition(), resCntr,
                    tx.topologyVersion(), tx.local());
            }

            if (log.isDebugEnabled())
                log.debug("Process cached entry:" + e);

            // DR
            if (ctx0.isDrEnabled()) {
                ctx0.dr().replicate(e.key(), e.value(), e.ttl(), e.expireTime(), e.version(),
                    tx.local() ? DR_PRIMARY : DR_BACKUP, e.topologyVersion());
            }

            // CQ
            CacheContinuousQueryManager contQryMgr = ctx0.continuousQueries();

            if (ctx0.continuousQueries().notifyContinuousQueries(tx)) {
                contQryMgr.getListenerReadLock().lock();

                try {
                    Map<UUID, CacheContinuousQueryListener> lsnrCol = continuousQueryListeners(ctx0, tx, e.key());

                    if (!F.isEmpty(lsnrCol)) {
                        contQryMgr.onEntryUpdated(
                            lsnrCol,
                            e.key(),
                            e.value(),
                            e.oldValue(),
                            false,
                            e.key().partition(),
                            tx.local(),
                            false,
                            e.updateCounter(),
                            null,
                            e.topologyVersion());
                    }
                }
                finally {
                    contQryMgr.getListenerReadLock().unlock();
                }
            }
        }
    }

    /**
     * @param ctx0 Cache context.
     * @param key Key.
     * @return {@code True} if need to replicate this value.
     */
    public boolean needDrReplicate(GridCacheContext ctx0, KeyCacheObject key) {
        return ctx0.isDrEnabled() && !key.internal();
    }

    /**
     * @param ctx0 Cache context.
     * @param tx Transaction.
     * @param key Key.
     * @return Map of listeners to be notified by this update.
     */
    public Map<UUID, CacheContinuousQueryListener> continuousQueryListeners(GridCacheContext ctx0, @Nullable IgniteInternalTx tx, KeyCacheObject key) {
        boolean internal = key.internal() || !ctx0.userCache();

        return ctx0.continuousQueries().notifyContinuousQueries(tx) ?
            ctx0.continuousQueries().updateListeners(internal, false) : null;
    }

    /**
     * Buffer for collecting enlisted entries. The main goal of this buffer is to fix reordering of dht enlist requests
     * on backups.
     */
    private static class EnlistBuffer {
        /** Last DHT future id. */
        private IgniteUuid lastFutId;

        /** Main buffer for entries. */
        private Map<KeyCacheObject, MvccTxEntry> cached = new LinkedHashMap<>();

        /** Pending entries. */
        private SortedMap<Integer, Map<KeyCacheObject, MvccTxEntry>> pending;

        /**
         * Adds entry to caching buffer.
         *
         * @param futId Future id.
         * @param batchNum Batch number.
         * @param key Key.
         * @param e Entry.
         */
        synchronized void add(IgniteUuid futId, int batchNum, KeyCacheObject key, MvccTxEntry e) {
            if (batchNum >= 0) {
                /*
                 * Assume that batches within one future may be reordered. But batches between futures cannot be
                 * reordered. This means that if batches from the new DHT future has arrived, all batches from the
                 * previous one has already been collected.
                 */
                if (lastFutId != null && !lastFutId.equals(futId)) { // Request from new DHT future arrived.
                    lastFutId = futId;

                    // Flush pending for previous future.
                    flushPending();
                }

                if (pending == null)
                    pending = new TreeMap<>() ;

                MvccTxEntry prev = pending.computeIfAbsent(batchNum, k -> new LinkedHashMap<>()).put(key, e);

                if (prev != null && prev.oldValue() != null)
                    e.oldValue(prev.oldValue());
            }
            else { // batchNum == -1 means no reordering (e.g. this is a primary node).
                assert batchNum == -1;

                MvccTxEntry prev = cached.put(key, e);

                if (prev != null && prev.oldValue() != null)
                    e.oldValue(prev.oldValue());
            }
        }

        /**
         * @return Cached entries map.
         */
        synchronized Map<KeyCacheObject, MvccTxEntry> getCached() {
            flushPending();

            return cached;
        }

        /**
         * Flush pending updates to cached map.
         */
        private void flushPending() {
            if (F.isEmpty(pending))
                return;

            for (Map.Entry<Integer, Map<KeyCacheObject, MvccTxEntry>> entry : pending.entrySet()) {
                Map<KeyCacheObject, MvccTxEntry> vals = entry.getValue();

                cached.putAll(vals);
            }

            pending.clear();
        }
    }
}
