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

import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheEntryEx;
import org.apache.ignite.internal.processors.cache.GridCacheEntryRemovedException;
import org.apache.ignite.internal.processors.cache.IgniteCacheExpiryPolicy;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtCacheAdapter;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.internal.util.lang.GridIterator;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteClosure;
import org.jetbrains.annotations.Nullable;

/** */
public final class ScanQueryIterator<K, V, R> extends AbstractScanQueryIterator<K, V, R> {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private final GridDhtCacheAdapter dht;

    /** */
    private final GridDhtLocalPartition locPart;

    /** */
    private final GridIterator<CacheDataRow> it;

    /** */
    private final GridCacheAdapter cache;

    /** */
    private final AffinityTopologyVersion topVer;

    /** */
    private final IgniteLogger log;

    /** */
    private IgniteCacheExpiryPolicy expiryPlc;

    /** */
    private final boolean incBackups;

    /** */
    private final long startTime;

    /** */
    private final int pageSize;

    /** */
    @Nullable
    private final GridConcurrentHashSet<ScanQueryIterator> locIters;

    /**
     * @param it            Iterator.
     * @param qry           Query.
     * @param topVer        Topology version.
     * @param locPart       Local partition.
     * @param scanFilter    Scan filter.
     * @param transformer   Transformer.
     * @param locNode       Local node flag.
     * @param locIters      Local iterators set.
     * @param cctx          Cache context.
     * @param log           Logger.
     */
    ScanQueryIterator(
        GridIterator<CacheDataRow> it,
        GridCacheQueryAdapter qry,
        AffinityTopologyVersion topVer,
        GridDhtLocalPartition locPart,
        IgniteBiPredicate<K, V> scanFilter,
        IgniteClosure transformer,
        boolean locNode,
        @Nullable GridConcurrentHashSet<ScanQueryIterator> locIters,
        GridCacheContext cctx,
        IgniteLogger log) throws IgniteCheckedException {
        super(cctx, qry, scanFilter, transformer, locNode);

        assert !locNode || locIters != null : "Local iterators can't be null for local query.";

        this.it = it;
        this.topVer = topVer;
        this.locPart = locPart;

        this.log = log;
        this.locIters = locIters;

        incBackups = qry.includeBackups();

        dht = cctx.isNear() ? cctx.near().dht() : cctx.dht();
        cache = dht != null ? dht : cctx.cache();

        expiryPlc = this.cctx.cache().expiryPolicy(null);

        startTime = U.currentTimeMillis();
        pageSize = qry.pageSize();
    }

    /** {@inheritDoc} */
    @Override protected void onClose() {
        if (expiryPlc != null && dht != null) {
            dht.sendTtlUpdateRequest(expiryPlc);

            expiryPlc = null;
        }

        if (locPart != null)
            locPart.release();

        if (intScanFilter != null)
            intScanFilter.close();

        if (locIters != null)
            locIters.remove(this);
    }

    /** {@inheritDoc} */
    @Override protected R advance() {
        long start = statsEnabled ? System.nanoTime() : 0L;

        R next0 = null;

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
                next0 = filterAndTransform(key, val, start);

                if (next0 != null)
                    break;
            }
        }

        if (next0 == null && expiryPlc != null && dht != null) {
            dht.sendTtlUpdateRequest(expiryPlc);

            expiryPlc = null;
        }

        return next0;
    }

    /** */
    @Nullable
    public IgniteBiPredicate<K, V> filter() {
        return intScanFilter == null ? null : intScanFilter.scanFilter;
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
}
