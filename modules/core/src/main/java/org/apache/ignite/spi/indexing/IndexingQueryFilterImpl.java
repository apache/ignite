/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
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
import org.apache.ignite.internal.util.typedef.F;
import org.jetbrains.annotations.Nullable;

import java.util.HashSet;

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
    @Nullable @Override public IndexingQueryCacheFilter forCache(String cacheName) {
        final GridCacheAdapter<Object, Object> cache = ctx.cache().internalCache(cacheName);

        // REPLICATED -> nothing to filter (explicit partitions are not supported).
        if (cache.context().isReplicated())
            return null;

        // No backups and explicit partitions -> nothing to filter.
        if (cache.configuration().getBackups() == 0 && parts == null)
            return null;

        return new IndexingQueryCacheFilter(cache.context().affinity(), parts, topVer,
            ctx.discovery().localNode());
    }
}
