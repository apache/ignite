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

package org.apache.ignite.internal.processors.cache.query;

import java.util.NoSuchElementException;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.events.CacheQueryReadEvent;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheMetricsImpl;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.processors.cache.CacheObjectUtils;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheEntryEx;
import org.apache.ignite.internal.processors.cache.GridCacheEntryRemovedException;
import org.apache.ignite.internal.processors.cache.IgniteCacheExpiryPolicy;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtCacheAdapter;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.platform.cache.PlatformCacheEntryFilter;
import org.apache.ignite.internal.processors.security.SecurityUtils;
import org.apache.ignite.internal.util.GridCloseableIteratorAdapter;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.internal.util.lang.GridIterator;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteClosure;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.events.EventType.EVT_CACHE_QUERY_OBJECT_READ;
import static org.apache.ignite.internal.processors.cache.query.GridCacheQueryManager.injectResources;
import static org.apache.ignite.internal.processors.security.SecurityUtils.securitySubjectId;

/** */
public final class ScanQueryIterator<K, V> extends GridCloseableIteratorAdapter<Object> {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private final GridDhtCacheAdapter dht;

    /** */
    private final GridDhtLocalPartition locPart;

    /** */
    private final IgniteBiPredicate<K, V> scanFilter;

    /** */
    private final boolean statsEnabled;

    /** */
    private final GridIterator<CacheDataRow> it;

    /** */
    private final GridCacheAdapter cache;

    /** */
    private final AffinityTopologyVersion topVer;

    /** */
    private final boolean keepBinary;

    /** */
    private final boolean readEvt;

    /** */
    private final String cacheName;

    /** */
    private final UUID subjId;

    /** */
    private final String taskName;

    /** */
    private final IgniteClosure transform;

    /** */
    private final CacheObjectContext objCtx;

    /** */
    private final GridCacheContext cctx;

    /** */
    private final IgniteLogger log;

    /** */
    private Object next;

    /** */
    private boolean needAdvance;

    /** */
    private IgniteCacheExpiryPolicy expiryPlc;

    /** */
    private final boolean locNode;

    /** */
    private final boolean incBackups;

    /** */
    private final long startTime;

    /** */
    private final int pageSize;

    /** */
    @Nullable private final GridConcurrentHashSet<ScanQueryIterator> locIters;

    /**
     * @param it Iterator.
     * @param qry Query.
     * @param topVer Topology version.
     * @param locPart Local partition.
     * @param transformer Transformer.
     * @param locNode Local node flag.
     * @param locIters Local iterators set.
     * @param cctx Cache context.
     * @param log Logger.
     */
    ScanQueryIterator(
        GridIterator<CacheDataRow> it,
        CacheQuery qry,
        AffinityTopologyVersion topVer,
        GridDhtLocalPartition locPart,
        IgniteClosure transformer,
        boolean locNode,
        @Nullable GridConcurrentHashSet<ScanQueryIterator> locIters,
        GridCacheContext cctx,
        IgniteLogger log) throws IgniteCheckedException {
        assert !locNode || locIters != null : "Local iterators can't be null for local query.";

        this.it = it;
        this.topVer = topVer;
        this.locPart = locPart;
        this.cctx = cctx;

        this.log = log;
        this.locNode = locNode;
        this.locIters = locIters;

        incBackups = qry.includeBackups();

        statsEnabled = cctx.statisticsEnabled();

        readEvt = cctx.events().isRecordable(EVT_CACHE_QUERY_OBJECT_READ) &&
            cctx.gridEvents().hasListener(EVT_CACHE_QUERY_OBJECT_READ);

        taskName = readEvt ? cctx.kernalContext().task().resolveTaskName(qry.taskHash()) : null;

        subjId = securitySubjectId(cctx);

        dht = cctx.isNear() ? cctx.near().dht() : cctx.dht();
        cache = dht != null ? dht : cctx.cache();
        objCtx = cctx.cacheObjectContext();
        cacheName = cctx.name();

        needAdvance = true;
        expiryPlc = this.cctx.cache().expiryPolicy(null);

        startTime = U.currentTimeMillis();
        pageSize = qry.pageSize();
        transform = SecurityUtils.sandboxedProxy(cctx.kernalContext(), IgniteClosure.class, injectResources(transformer, cctx));
        scanFilter = prepareScanFilter(qry.scanFilter());
        // keep binary for remote scans if possible
        keepBinary = (!locNode && scanFilter == null && transformer == null && !readEvt) || qry.keepBinary();
    }

    /** {@inheritDoc} */
    @Override protected Object onNext() {
        if (needAdvance)
            advance();
        else
            needAdvance = true;

        if (next == null)
            throw new NoSuchElementException();

        return next;
    }

    /** {@inheritDoc} */
    @Override protected boolean onHasNext() {
        if (needAdvance) {
            advance();

            needAdvance = false;
        }

        return next != null;
    }

    /** {@inheritDoc} */
    @Override protected void onClose() {
        if (expiryPlc != null && dht != null) {
            dht.sendTtlUpdateRequest(expiryPlc);

            expiryPlc = null;
        }

        if (locPart != null)
            locPart.release();

        closeFilter(scanFilter);

        if (locIters != null)
            locIters.remove(this);
    }

    /**
     * Moves the iterator to the next cache entry.
     */
    private void advance() {
        long start = statsEnabled ? System.nanoTime() : 0L;

        Object next0 = null;

        while (it.hasNext()) {
            CacheDataRow row = it.next();

            KeyCacheObject key = row.key();
            CacheObject val;

            if (expiryPlc != null) {
                try {
                    CacheDataRow tmp = row;

                    while (true) {
                        try {
                            GridCacheEntryEx entry = cache.entryEx(key);

                            entry.unswap(tmp);

                            val = entry.peek(true, true, topVer, expiryPlc);

                            entry.touch();

                            break;
                        }
                        catch (GridCacheEntryRemovedException ignore) {
                            tmp = null;
                        }
                    }
                }
                catch (IgniteCheckedException e) {
                    if (log.isDebugEnabled())
                        log.debug("Failed to peek value: " + e);

                    val = null;
                }

                if (dht != null && expiryPlc.readyToFlush(100))
                    dht.sendTtlUpdateRequest(expiryPlc);
            }
            else
                val = row.value();

            // Filter backups for SCAN queries, if it isn't partition scan.
            // Other types are filtered in indexing manager.
            if (!cctx.isReplicated() && /*qry.partition()*/this.locPart == null && !incBackups &&
                !cctx.affinity().primaryByKey(cctx.localNode(), key, topVer)) {
                if (log.isDebugEnabled())
                    log.debug("Ignoring backup element [row=" + row +
                        ", cacheMode=" + cctx.config().getCacheMode() + ", incBackups=" + incBackups +
                        ", primary=" + cctx.affinity().primaryByKey(cctx.localNode(), key, topVer) + ']');

                continue;
            }

            if (log.isDebugEnabled()) {
                ClusterNode primaryNode = cctx.affinity().primaryByKey(key,
                    cctx.affinity().affinityTopologyVersion());

                log.debug(S.toString("Record",
                    "key", key, true,
                    "val", val, true,
                    "incBackups", incBackups, false,
                    "priNode", primaryNode != null ? U.id8(primaryNode.id()) : null, false,
                    "node", U.id8(cctx.localNode().id()), false));
            }

            if (val != null) {
                K key0 = (K)CacheObjectUtils.unwrapBinaryIfNeeded(objCtx, key, keepBinary, false);
                V val0 = (V)CacheObjectUtils.unwrapBinaryIfNeeded(objCtx, val, keepBinary, false);

                if (statsEnabled) {
                    CacheMetricsImpl metrics = cctx.cache().metrics0();

                    metrics.onRead(true);

                    metrics.addGetTimeNanos(System.nanoTime() - start);
                }

                if (filter(key0, val0)) {
                    if (readEvt) {
                        cctx.gridEvents().record(new CacheQueryReadEvent<>(
                            cctx.localNode(),
                            "Scan query entry read.",
                            EVT_CACHE_QUERY_OBJECT_READ,
                            CacheQueryType.SCAN.name(),
                            cacheName,
                            null,
                            null,
                            scanFilter,
                            null,
                            null,
                            subjId,
                            taskName,
                            key0,
                            val0,
                            null,
                            null));
                    }

                    if (transform != null) {
                        try {
                            next0 = transform.apply(new CacheQueryEntry<>(key0, val0));
                        }
                        catch (Throwable e) {
                            throw new IgniteException(e);
                        }
                    }
                    else
                        next0 = !locNode ? new T2<>(key0, val0) :
                            new CacheQueryEntry<>(key0, val0);

                    break;
                }
            }
        }

        if ((this.next = next0) == null && expiryPlc != null && dht != null) {
            dht.sendTtlUpdateRequest(expiryPlc);

            expiryPlc = null;
        }
    }

    /** */
    @Nullable public IgniteBiPredicate<K, V> filter() {
        return scanFilter;
    }

    /** */
    public AffinityTopologyVersion topVer() {
        return topVer;
    }

    /** */
    public GridDhtLocalPartition localPartition() {
        return locPart;
    }

    /** */
    public IgniteClosure transformer() {
        return transform;
    }

    /** */
    public long startTime() {
        return startTime;
    }

    /** */
    public boolean local() {
        return locNode;
    }

    /** */
    public boolean keepBinary() {
        return keepBinary;
    }

    /** */
    public UUID subjectId() {
        return subjId;
    }

    /** */
    public String taskName() {
        return taskName;
    }

    /** */
    public GridCacheContext cacheContext() {
        return cctx;
    }

    /** */
    public int pageSize() {
        return pageSize;
    }

    /** */
    private @Nullable IgniteBiPredicate<K, V> prepareScanFilter(IgniteBiPredicate<K, V> keyValFilter) throws IgniteCheckedException {
        if (keyValFilter == null)
            return null;

        try {
            if (keyValFilter instanceof PlatformCacheEntryFilter)
                ((PlatformCacheEntryFilter)keyValFilter).cacheContext(cctx);
            else
                injectResources(keyValFilter, cctx);

            return SecurityUtils.sandboxedProxy(cctx.kernalContext(), IgniteBiPredicate.class, keyValFilter);
        }
        catch (IgniteCheckedException | RuntimeException e) {
            closeFilter(keyValFilter);

            throw e;
        }
    }

    /** */
    private boolean filter(K k, V v) {
        try {
            return scanFilter == null || scanFilter.apply(k, v);
        }
        catch (Throwable e) {
            throw new IgniteException(e);
        }
    }

    /** */
    static void closeFilter(IgniteBiPredicate<?, ?> filter) {
        if (filter instanceof PlatformCacheEntryFilter)
            ((PlatformCacheEntryFilter)filter).onClose();
    }
}