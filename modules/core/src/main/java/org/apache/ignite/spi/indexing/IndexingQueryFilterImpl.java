/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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
