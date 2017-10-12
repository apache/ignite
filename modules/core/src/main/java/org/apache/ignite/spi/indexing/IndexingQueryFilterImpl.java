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

import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.cache.GridCacheAffinityManager;
import org.apache.ignite.internal.util.typedef.F;
import org.jetbrains.annotations.Nullable;

import java.util.HashSet;
import java.util.Set;

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
     * Constructor.
     *
     * @param ctx Kernal context.
     * @param topVer Topology version.
     * @param partsArr Partitions array.
     */
    public IndexingQueryFilterImpl(GridKernalContext ctx, @Nullable AffinityTopologyVersion topVer,
        @Nullable int[] partsArr) {
        this.ctx = ctx;

        this.topVer = topVer != null ? topVer : AffinityTopologyVersion.NONE;

        if (F.isEmpty(partsArr))
            parts = null;
        else {
            parts = new HashSet<>();

            for (int part : partsArr)
                parts.add(part);
        }
    }

    /** {@inheritDoc} */
    @Nullable @Override public IndexingQueryFilterPredicate forCache(String cacheName) {
        final GridCacheAdapter<Object, Object> cache = ctx.cache().internalCache(cacheName);

        // REPLICATED -> nothing to filter (explicit partitions are not supported).
        if (cache.context().isReplicated())
            return null;

        // No backups and explicit partitions -> nothing to filter.
//        if (cache.configuration().getBackups() == 0 && parts == null)
//            return null;

        final GridCacheAffinityManager aff = cache.context().affinity();

        Set<Integer> locParts = aff.primaryPartitions(ctx.discovery().localNode().id(), topVer);

        Set<Integer> parts0 = new HashSet<>(locParts);

        if (parts != null)
            parts0.retainAll(parts);

        return new IndexingQueryFilterPredicate() {
            @Override public boolean apply(Object key, Object val) {
                int part = aff.partition(key);

//                boolean primary = aff.primaryByPartition(ctx.discovery().localNode(), part, topVer);
//
//                return primary && (parts == null || parts.contains(part));


                boolean primary = aff.primaryByPartition(ctx.discovery().localNode(), part, topVer);

                return primary;


//                return aff.primaryByKey(ctx.discovery().localNode(), key, topVer);



//                int part = aff.partition(key);
//
//                return parts0.contains(part);
            }
        };
    }
}
