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
import org.apache.ignite.internal.processors.cache.KeyCacheObjectImpl;
import org.apache.ignite.internal.processors.cache.distributed.dht.PartitionUpdateCountersMessage;
import org.apache.ignite.internal.processors.cache.mvcc.txlog.TxKey;
import org.apache.ignite.internal.processors.cache.query.continuous.CacheContinuousQueryListener;
import org.apache.ignite.internal.processors.cache.query.continuous.CacheContinuousQueryManager;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.processors.cache.transactions.TxCounters;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.GridIntList;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_MVCC_TX_SIZE_CACHING_THRESHOLD;
import static org.apache.ignite.internal.processors.dr.GridDrType.DR_BACKUP;
import static org.apache.ignite.internal.processors.dr.GridDrType.DR_PRIMARY;

/**
 * Manager for caching MVCC transaction updates. This updates can be used further in CQ, DR and other places.
 */
public class MvccCachingManager extends GridCacheSharedManagerAdapter {
    /** Maximum possible transaction size when caching is enabled. */
    public static final int TX_SIZE_THRESHOLD = IgniteSystemProperties.getInteger(IGNITE_MVCC_TX_SIZE_CACHING_THRESHOLD,
        20_000);

    /** Cached enlist values. */
    private final Map<GridCacheVersion, EnlistBuffer> enlistCache = new ConcurrentHashMap<>();

    /** Counters map. Used for OOM prevention caused by the big transactions. */
    private final Map<TxKey, AtomicInteger> cntrs = new ConcurrentHashMap<>();

    /**
     * Adds enlisted tx entry to cache.
     *
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
                ", primary=" + primary + ", mvccVer=" + mvccVer + ", cacheId=" + cacheId + ", ver=" + ver + ']');
        }

        // Do not cache updates if there are no DR or CQ were enabled when cache was added as active for the current tx.
        if (!tx.txState().useMvccCaching(cacheId))
            return;

        AtomicInteger cntr = cntrs.computeIfAbsent(new TxKey(mvccVer.coordinatorVersion(), mvccVer.counter()),
            v -> new AtomicInteger());

        if (cntr.incrementAndGet() > TX_SIZE_THRESHOLD)
            throw new IgniteCheckedException("Transaction is too large. Consider reducing transaction size or " +
                "turning off continuous queries and datacenter replication [size=" + cntr.get() + ", txXid=" + ver + ']');

        MvccTxEntry e = new MvccTxEntry(key, val, ttl, expireTime, ver, oldVal, primary, topVer, mvccVer, cacheId);

        EnlistBuffer cached = enlistCache.computeIfAbsent(ver, v -> new EnlistBuffer());

        cached.add(primary ? null : futId, primary ? -1 : batchNum, e);
    }

    /**
     * @param tx Transaction.
     * @param commit {@code True} if commit.
     */
    public void onTxFinished(IgniteInternalTx tx, boolean commit) throws IgniteCheckedException {
        if (log.isDebugEnabled())
            log.debug("Transaction finished: [commit=" + commit + ", tx=" + tx + ']');

        if (tx.system() || tx.internal() || tx.mvccSnapshot() == null)
            return;

        cntrs.remove(new TxKey(tx.mvccSnapshot().coordinatorVersion(), tx.mvccSnapshot().counter()));

        EnlistBuffer buf = enlistCache.remove(tx.xidVersion());

        Map<Integer, Map<KeyCacheObject, MvccTxEntry>> allCached = buf == null ? null : buf.getCached();

        TxCounters txCntrs = tx.txCounters(false);

        Collection<PartitionUpdateCountersMessage> cntrsColl = txCntrs == null ? null : txCntrs.updateCounters();

        if (txCntrs == null || F.isEmpty(cntrsColl))
            return;

        GridIntList cacheIds = tx.txState().cacheIds();

        assert cacheIds != null;

        for (int i = 0; i < cacheIds.size(); i++) {
            int cacheId = cacheIds.get(i);

            GridCacheContext ctx0 = cctx.cacheContext(cacheId);

            assert ctx0 != null;

            ctx0.group().listenerLock().readLock().lock();

            try {
                boolean hasListeners = ctx0.hasContinuousQueryListeners(tx);
                boolean drEnabled = ctx0.isDrEnabled();

                if (!hasListeners && !drEnabled)
                    continue; // There are no listeners to notify.

                // Get cached entries for the given cache.
                Map<KeyCacheObject, MvccTxEntry> cached = allCached == null ? null : allCached.get(cacheId);

                Map<Integer, Map<Integer, T2<AtomicLong, Long>>> cntrsMap = countersPerPartition(cntrsColl);

                Map<Integer, T2<AtomicLong, Long>> cntrPerCache = cntrsMap.get(cacheId);

                if (F.isEmpty(cntrPerCache))
                    continue; // No updates were made for this cache.

                boolean fakeEntries = false;

                if (F.isEmpty(cached)) {
                    if (log.isDebugEnabled())
                        log.debug("Transaction updates were not cached fully (this can happen when listener started" +
                            " during the transaction execution). [tx=" + tx + ']');

                    if (hasListeners) {
                        cached = createFakeCachedEntries(cntrPerCache, tx, cacheId); // Create fake update entries if we have CQ listeners.

                        fakeEntries = true;
                    }
                    else
                        continue; // Nothing to do further if tx is not cached entirely and there are no any CQ listeners.
                }

                if (F.isEmpty(cached))
                    continue;

                // Feed CQ & DR with entries.
                for (Map.Entry<KeyCacheObject, MvccTxEntry> entry : cached.entrySet()) {
                    MvccTxEntry e = entry.getValue();

                    assert e.key().partition() != -1;

                    assert cntrPerCache != null;
                    assert e.cacheId() == cacheId;

                    T2<AtomicLong, Long> cntr = cntrPerCache.get(e.key().partition());

                    long resCntr = cntr.getKey().incrementAndGet();

                    assert resCntr <= cntr.getValue();

                    e.updateCounter(resCntr);

                    if (ctx0.group().sharedGroup()) {
                        ctx0.group().onPartitionCounterUpdate(cacheId, e.key().partition(), resCntr,
                            tx.topologyVersion(), tx.local());
                    }

                    if (log.isDebugEnabled())
                        log.debug("Process cached entry:" + e);

                    // DR
                    if (ctx0.isDrEnabled() && !fakeEntries) {
                        ctx0.dr().replicate(e.key(), e.value(), e.ttl(), e.expireTime(), e.version(),
                            tx.local() ? DR_PRIMARY : DR_BACKUP, e.topologyVersion());
                    }

                    // CQ
                    CacheContinuousQueryManager contQryMgr = ctx0.continuousQueries();

                    if (ctx0.continuousQueries().notifyContinuousQueries(tx)) {
                        Map<UUID, CacheContinuousQueryListener> lsnrCol = continuousQueryListeners(ctx0, tx);

                        if (!F.isEmpty(lsnrCol)) {
                            contQryMgr.onEntryUpdated(
                                lsnrCol,
                                e.key(),
                                commit ? e.value() : null, // Force skip update counter if rolled back.
                                commit ? e.oldValue() : null, // Force skip update counter if rolled back.
                                false,
                                e.key().partition(),
                                tx.local(),
                                false,
                                e.updateCounter(),
                                null,
                                e.topologyVersion());
                        }
                    }
                }
            }
            finally {
                ctx0.group().listenerLock().readLock().unlock();
            }
        }
    }

    /**
     * Calculates counters updates per cache and partition: cacheId -> partId -> initCntr -> cntr + delta.
     *
     * @param cntrsColl Counters collection.
     * @return Counters updates per cache and partition.
     */
    private Map<Integer, Map<Integer, T2<AtomicLong, Long>>> countersPerPartition(
        Collection<PartitionUpdateCountersMessage> cntrsColl) {
        //
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

        return cntrsMap;
    }

    /**
     * If transaction was not cached entirely (if listener was set during tx execution), we should feed the CQ engine
     * with a fake entries prepared by this method.
     *
     * @param cntrPerCache Update counters deltas made by transaction.
     * @param tx Transaction.
     * @param cacheId Cache id.
     * @return Fake entries for each tx update.
     */
    private Map<KeyCacheObject, MvccTxEntry> createFakeCachedEntries(Map<Integer, T2<AtomicLong, Long>> cntrPerCache,
        IgniteInternalTx tx, int cacheId) {
        Map<KeyCacheObject, MvccTxEntry> fakeCached = new HashMap<>();

        for (Map.Entry<Integer, T2<AtomicLong, Long>> e : cntrPerCache.entrySet()) {
            int part = e.getKey();

            long startCntr = e.getValue().get1().get(); // Init update counter.
            long endCntr = e.getValue().get1().get() + e.getValue().get2(); // Init update counter + delta.

            for (long i = startCntr; i < endCntr; i++) {
                KeyCacheObject fakeKey = new KeyCacheObjectImpl("", null, part);

                MvccTxEntry fakeEntry = new MvccTxEntry(fakeKey, null, 0, 0, tx.xidVersion(), null,
                    tx.local(), tx.topologyVersion(), tx.mvccSnapshot(), cacheId);

                fakeCached.put(fakeKey, fakeEntry);
            }
        }

        return fakeCached;
    }

    /**
     * @param ctx0 Cache context.
     * @param tx Transaction.
     * @return Map of listeners to be notified by this update.
     */
    public Map<UUID, CacheContinuousQueryListener> continuousQueryListeners(GridCacheContext ctx0,
        @Nullable IgniteInternalTx tx) {
        return ctx0.continuousQueries().notifyContinuousQueries(tx) ?
            ctx0.continuousQueries().updateListeners(!ctx0.userCache(), false) : null;
    }

    /**
     * Buffer for collecting enlisted entries. The main goal of this buffer is to fix reordering of dht enlist requests
     * on backups.
     */
    private static class EnlistBuffer {
        /** Last DHT future id. */
        private IgniteUuid lastFutId;

        /** Main buffer for entries. CacheId -> entriesMap. */
        @GridToStringInclude
        private Map<Integer, Map<KeyCacheObject, MvccTxEntry>> cached = new TreeMap<>();

        /** Pending entries. BatchId -> entriesMap. */
        @GridToStringInclude
        private SortedMap<Integer, Map<KeyCacheObject, MvccTxEntry>> pending;

        /**
         * Adds entry to caching buffer.
         *
         * @param futId Future id.
         * @param batchNum Batch number.
         * @param e Entry.
         */
        synchronized void add(IgniteUuid futId, int batchNum, MvccTxEntry e) {
            KeyCacheObject key = e.key();

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
                    pending = new TreeMap<>();

                MvccTxEntry prev = pending.computeIfAbsent(batchNum, k -> new LinkedHashMap<>()).put(key, e);

                if (prev != null && prev.oldValue() != null)
                    e.oldValue(prev.oldValue());
            }
            else { // batchNum == -1 means no reordering (e.g. this is a primary node).
                assert batchNum == -1;

                Map<KeyCacheObject, MvccTxEntry> entriesForCache = cached.computeIfAbsent(e.cacheId(), k -> new LinkedHashMap<>());

                MvccTxEntry prev = entriesForCache.put(key, e);

                // If key is updated more than once within transaction, we should copy old value
                // (the value existed before tx started) from the previous entry to the new one.
                if (prev != null && prev.oldValue() != null)
                    e.oldValue(prev.oldValue());
            }
        }

        /**
         * @return Cached entries map.
         */
        synchronized Map<Integer, Map<KeyCacheObject, MvccTxEntry>> getCached() {
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

                for (Map.Entry<KeyCacheObject, MvccTxEntry> e : vals.entrySet()) {
                    Map<KeyCacheObject, MvccTxEntry> entriesForCache = cached
                        .computeIfAbsent(e.getValue().cacheId(), k -> new LinkedHashMap<>());

                    MvccTxEntry prev = entriesForCache.put(e.getKey(), e.getValue());

                    // If key is updated more than once within transaction, we should copy old value
                    // (the value existed before tx started) from the previous entry to the new one.
                    if (prev != null && prev.oldValue() != null)
                        e.getValue().oldValue(prev.oldValue());
                }
            }

            pending.clear();
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(EnlistBuffer.class, this);
        }
    }
}
