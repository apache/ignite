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

import java.util.Set;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheAffinityManager;

/**
 * Indexing query filter for specific cache.
 */
public class IndexingQueryCacheFilter {
    /** Affinity manager. */
    private final GridCacheAffinityManager aff;

    /** Partitions. */
    private final Set<Integer> parts;

    /** Topology version. */
    private final AffinityTopologyVersion topVer;

    /** Local node. */
    private final ClusterNode locNode;

    /**
     * Constructor.
     *
     * @param aff Affinity.
     * @param parts Partitions.
     * @param topVer Topology version.
     * @param locNode Local node.
     */
    public IndexingQueryCacheFilter(GridCacheAffinityManager aff, Set<Integer> parts,
        AffinityTopologyVersion topVer, ClusterNode locNode) {
        this.aff = aff;
        this.parts = parts;
        this.topVer = topVer;
        this.locNode = locNode;
    }

    /**
     * Apply filter.
     *
     * @param key Key.
     * @return {@code True} if passed.
     */
    public boolean apply(Object key) {
        int part = aff.partition(key);

        return applyPartition(part);
    }

    /**
     * Apply filter.
     *
     * @param part Partition.
     * @return {@code True} if passed.
     */
    public boolean applyPartition(int part) {
        if (parts == null)
            return aff.primaryByPartition(locNode, part, topVer);
        else
            return parts.contains(part);
    }
}
