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

package org.apache.ignite.spi.indexing;

import java.util.HashSet;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.util.typedef.F;
import org.jetbrains.annotations.Nullable;

/**
 * Indexing query filter.
 */
public class IndexingQueryFilterImpl implements IndexingQueryFilter {
    /** Kernal context. */
    private final GridKernalContext ctx;

    /** Topology version. */
    private final AffinityTopologyVersion topVer;

    /** Partitions. */
    private final HashSet<Integer> parts;

    /**
     * Treat replicated as partitioned.
     * This was introduced as a partial solution for "[IGNITE-8732] SQL: REPLICATED cache cannot be left-joined
     * to PARTITIONED".
     *
     * If this flag is set, only primary partitions of the REPLICATED caches will be scanned (same as with
     * PARTITIONED caches).
     *
     * This flag requires the REPLICATED and PARTITIONED to have the same number of partitions and affinity,
     * and requires the JOIN to be on an affinity key of both caches.
     *
     * This flag is incompatible with distributed joins.
     */
    private final boolean treatReplicatedAsPartitioned;

    /**
     * Constructor.
     *
     * @param ctx Kernal context.
     * @param topVer Topology version.
     * @param partsArr Partitions array.
     * @param treatReplicatedAsPartitioned If true, only primary partitions of replicated caches will be used.
     */
    public IndexingQueryFilterImpl(GridKernalContext ctx, @Nullable AffinityTopologyVersion topVer,
        @Nullable int[] partsArr, boolean treatReplicatedAsPartitioned) {
        this.ctx = ctx;

        this.topVer = topVer != null ? topVer : AffinityTopologyVersion.NONE;

        if (F.isEmpty(partsArr))
            parts = null;
        else {
            parts = new HashSet<>();

            for (int part : partsArr)
                parts.add(part);
        }

        this.treatReplicatedAsPartitioned = treatReplicatedAsPartitioned;
    }

    /**
     * Constructor.
     *
     * @param ctx Kernal context.
     * @param topVer Topology version.
     * @param partsArr Partitions array.
     */
    public IndexingQueryFilterImpl(GridKernalContext ctx, @Nullable AffinityTopologyVersion topVer,
            @Nullable int[] partsArr) {
        this(ctx, topVer, partsArr, false);
    }

    /** {@inheritDoc} */
    @Nullable @Override public IndexingQueryCacheFilter forCache(String cacheName) {
        final GridCacheAdapter<Object, Object> cache = ctx.cache().internalCache(cacheName);

        if (cache == null) {
            throw new IgniteSQLException("Failed to find cache [cacheName=" + cacheName + ']',
                IgniteQueryErrorCode.TABLE_NOT_FOUND);
        }

        // REPLICATED -> nothing to filter (explicit partitions are not supported).
        if (cache.context().isReplicated() && !treatReplicatedAsPartitioned)
            return null;

        return new IndexingQueryCacheFilter(cache.context().affinity(), parts, topVer,
            ctx.discovery().localNode());
    }
}
