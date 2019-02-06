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

package org.apache.ignite.internal.processors.query.h2.twostep;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.cache.CacheException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.CacheServerNotFoundException;
import org.apache.ignite.cache.PartitionLossPolicy;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState;
import org.apache.ignite.internal.util.GridIntIterator;
import org.apache.ignite.internal.util.GridIntList;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.h2.util.IntArray;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.cache.PartitionLossPolicy.READ_ONLY_SAFE;
import static org.apache.ignite.cache.PartitionLossPolicy.READ_WRITE_SAFE;
import static org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion.NONE;

/**
 * Reduce partition mapper.
 */
public class ReducePartitionMapper {
    /** */
    private static final Set<ClusterNode> UNMAPPED_PARTS = Collections.emptySet();

    /** Kernal context. */
    private final GridKernalContext ctx;

    /** */
    private final IgniteLogger log;

    /**
     * Constructor.
     *
     * @param ctx Kernal context.
     */
    public ReducePartitionMapper(GridKernalContext ctx, IgniteLogger log) {
        this.ctx = ctx;
        this.log = log;
    }

    /**
     * Evaluates nodes and nodes to partitions map given a list of cache ids, topology version and partitions.
     *
     * @param cacheIds Cache ids.
     * @param topVer Topology version.
     * @param parts Partitions array.
     * @param isReplicatedOnly Allow only replicated caches.
     * @param qryId Query ID.
     * @return Result.
     */
    public ReducePartitionMapResult nodesForPartitions(List<Integer> cacheIds, AffinityTopologyVersion topVer,
        int[] parts, boolean isReplicatedOnly, long qryId) {
        Collection<ClusterNode> nodes = null;
        Map<ClusterNode, IntArray> partsMap = null;
        Map<ClusterNode, IntArray> qryMap = null;

        for (int cacheId : cacheIds) {
            GridCacheContext<?, ?> cctx = cacheContext(cacheId);

            PartitionLossPolicy plc = cctx.config().getPartitionLossPolicy();

            if (plc != READ_ONLY_SAFE && plc != READ_WRITE_SAFE)
                continue;

            Collection<Integer> lostParts = cctx.topology().lostPartitions();

            for (int part : lostParts) {
                if (parts == null || Arrays.binarySearch(parts, part) >= 0) {
                    throw new CacheException("Failed to execute query because cache partition has been " +
                        "lost [cacheName=" + cctx.name() + ", part=" + part + ']');
                }
            }
        }

        if (isPreloadingActive(cacheIds)) {
            if (isReplicatedOnly)
                nodes = replicatedUnstableDataNodes(cacheIds, qryId);
            else {
                partsMap = partitionedUnstableDataNodes(cacheIds, qryId);

                if (partsMap != null) {
                    qryMap = narrowForQuery(partsMap, parts);

                    nodes = qryMap == null ? null : qryMap.keySet();
                }
            }
        }
        else {
            qryMap = stableDataNodes(isReplicatedOnly, topVer, cacheIds, parts, qryId);

            if (qryMap != null)
                nodes = qryMap.keySet();
        }

        return new ReducePartitionMapResult(nodes, partsMap, qryMap);
    }

    /**
     * @param cacheId Cache ID.
     * @return Cache context.
     */
    private GridCacheContext<?,?> cacheContext(Integer cacheId) {
        GridCacheContext<?, ?> cctx = ctx.cache().context().cacheContext(cacheId);

        if (cctx == null)
            throw new CacheException(String.format("Cache not found on local node (was concurrently destroyed?) " +
                "[cacheId=%d]", cacheId));

        return cctx;
    }

    /**
     * @param cacheIds Cache IDs.
     * @return {@code true} If preloading is active.
     */
    private boolean isPreloadingActive(List<Integer> cacheIds) {
        for (Integer cacheId : cacheIds) {
            if (hasMovingPartitions(cacheContext(cacheId)))
                return true;
        }

        return false;
    }

    /**
     * @param cctx Cache context.
     * @return {@code True} If cache has partitions in {@link GridDhtPartitionState#MOVING} state.
     */
    private static boolean hasMovingPartitions(GridCacheContext<?, ?> cctx) {
        assert cctx != null;

        return !cctx.isLocal() && cctx.topology().hasMovingPartitions();
    }

    /**
     * @param isReplicatedOnly If we must only have replicated caches.
     * @param topVer Topology version.
     * @param cacheIds Participating cache IDs.
     * @param parts Partitions.
     * @param qryId Query ID.
     * @return Data nodes or {@code null} if repartitioning started and we need to retry.
     */
    private Map<ClusterNode, IntArray> stableDataNodes(boolean isReplicatedOnly, AffinityTopologyVersion topVer,
        List<Integer> cacheIds, int[] parts, long qryId) {
        GridCacheContext<?, ?> cctx = cacheContext(cacheIds.get(0));

        // If the first cache is not partitioned, find it (if it's present) and move it to index 0.
        if (!cctx.isPartitioned()) {
            for (int cacheId = 1; cacheId < cacheIds.size(); cacheId++) {
                GridCacheContext<?, ?> currCctx = cacheContext(cacheIds.get(cacheId));

                if (currCctx.isPartitioned()) {
                    Collections.swap(cacheIds, 0, cacheId);

                    cctx = currCctx;

                    break;
                }
            }
        }

        Map<ClusterNode, IntArray> map = stableDataNodesMap(topVer, cctx, parts);

        Set<ClusterNode> nodes = map.keySet();

        if (F.isEmpty(map))
            throw new CacheServerNotFoundException("Failed to find data nodes for cache: " + cctx.name());

        for (int i = 1; i < cacheIds.size(); i++) {
            GridCacheContext<?,?> extraCctx = cacheContext(cacheIds.get(i));

            String extraCacheName = extraCctx.name();

            if (extraCctx.isLocal())
                continue; // No consistency guaranties for local caches.

            if (isReplicatedOnly && !extraCctx.isReplicated())
                throw new CacheException("Queries running on replicated cache should not contain JOINs " +
                    "with partitioned tables [replicatedCache=" + cctx.name() +
                    ", partitionedCache=" + extraCacheName + "]");

            Set<ClusterNode> extraNodes = stableDataNodesMap(topVer, extraCctx, parts).keySet();

            if (F.isEmpty(extraNodes))
                throw new CacheServerNotFoundException("Failed to find data nodes for cache: " + extraCacheName);

            boolean disjoint;

            if (extraCctx.isReplicated()) {
                if (isReplicatedOnly) {
                    nodes.retainAll(extraNodes);

                    disjoint = map.isEmpty();
                }
                else
                    disjoint = !extraNodes.containsAll(nodes);
            }
            else
                disjoint = !extraNodes.equals(nodes);

            if (disjoint) {
                if (isPreloadingActive(cacheIds)) {
                    logRetry("Failed to calculate nodes for SQL query (got disjoint node map during rebalance) " +
                        "[qryId=" + qryId + ", affTopVer=" + topVer + ", cacheIds=" + cacheIds +
                        ", parts=" + (parts == null ? "[]" : Arrays.toString(parts)) +
                        ", replicatedOnly=" + isReplicatedOnly + ", lastCache=" + extraCctx.name() +
                        ", lastCacheId=" + extraCctx.cacheId() + ']');

                    return null; // Retry.
                }
                else
                    throw new CacheException("Caches have distinct sets of data nodes [cache1=" + cctx.name() +
                        ", cache2=" + extraCacheName + "]");
            }
        }

        return map;
    }

    /**
     * @param topVer Topology version.
     * @param cctx Cache context.
     * @param parts Partitions.
     */
    private Map<ClusterNode, IntArray> stableDataNodesMap(AffinityTopologyVersion topVer,
        final GridCacheContext<?, ?> cctx, @Nullable final int[] parts) {

        Map<ClusterNode, IntArray> mapping = new HashMap<>();

        // Explicit partitions mapping is not applicable to replicated cache.
        if (cctx.isReplicated()) {
            for (ClusterNode clusterNode : cctx.affinity().assignment(topVer).nodes())
                mapping.put(clusterNode, null);

            return mapping;
        }

        List<List<ClusterNode>> assignment = cctx.affinity().assignment(topVer).assignment();

        boolean needPartsFilter = parts != null;

        GridIntIterator iter = needPartsFilter ? new GridIntList(parts).iterator() :
            U.forRange(0, cctx.affinity().partitions());

        while(iter.hasNext()) {
            int partId = iter.next();

            List<ClusterNode> partNodes = assignment.get(partId);

            if (!partNodes.isEmpty()) {
                ClusterNode prim = partNodes.get(0);

                if (!needPartsFilter) {
                    mapping.put(prim, null);

                    continue;
                }

                IntArray partIds = mapping.get(prim);

                if (partIds == null) {
                    partIds = new IntArray();

                    mapping.put(prim, partIds);
                }

                partIds.add(partId);
            }
        }

        return mapping;
    }

    /**
     * Calculates partition mapping for partitioned cache on unstable topology.
     *
     * @param cacheIds Cache IDs.
     * @param qryId Query ID.
     * @return Partition mapping or {@code null} if we can't calculate it due to repartitioning and we need to retry.
     */
    @SuppressWarnings("unchecked")
    private Map<ClusterNode, IntArray> partitionedUnstableDataNodes(List<Integer> cacheIds, long qryId) {
        // If the main cache is replicated, just replace it with the first partitioned.
        GridCacheContext<?,?> cctx = findFirstPartitioned(cacheIds);

        final int partsCnt = cctx.affinity().partitions();

        if (cacheIds.size() > 1) { // Check correct number of partitions for partitioned caches.
            for (Integer cacheId : cacheIds) {
                GridCacheContext<?, ?> extraCctx = cacheContext(cacheId);

                if (extraCctx.isReplicated() || extraCctx.isLocal())
                    continue;

                int parts = extraCctx.affinity().partitions();

                if (parts != partsCnt)
                    throw new CacheException("Number of partitions must be the same for correct collocation [cache1=" +
                        cctx.name() + ", parts1=" + partsCnt + ", cache2=" + extraCctx.name() +
                        ", parts2=" + parts + "]");
            }
        }

        Set<ClusterNode>[] partLocs = new Set[partsCnt];

        // Fill partition locations for main cache.
        for (int p = 0; p < partsCnt; p++) {
            List<ClusterNode> owners = cctx.topology().owners(p);

            if (F.isEmpty(owners)) {
                // Handle special case: no mapping is configured for a partition.
                if (F.isEmpty(cctx.affinity().assignment(NONE).get(p))) {
                    partLocs[p] = UNMAPPED_PARTS; // Mark unmapped partition.

                    continue;
                }
                else if (!F.isEmpty(dataNodes(cctx.groupId(), NONE))) {
                    logRetry("Failed to calculate nodes for SQL query (partition has no owners, but corresponding " +
                        "cache group has data nodes) [qryId=" + qryId + ", cacheIds=" + cacheIds +
                        ", cacheName=" + cctx.name() + ", cacheId=" + cctx.cacheId() + ", part=" + p +
                        ", cacheGroupId=" + cctx.groupId() + ']');

                    return null; // Retry.
                }

                throw new CacheServerNotFoundException("Failed to find data nodes [cache=" + cctx.name() + ", part=" + p + "]");
            }

            partLocs[p] = new HashSet<>(owners);
        }

        if (cacheIds.size() > 1) {
            // Find owner intersections for each participating partitioned cache partition.
            // We need this for logical collocation between different partitioned caches with the same affinity.
            for (Integer cacheId : cacheIds) {
                GridCacheContext<?, ?> extraCctx = cacheContext(cacheId);

                // This is possible if we have replaced a replicated cache with a partitioned one earlier.
                if (cctx == extraCctx)
                    continue;

                if (extraCctx.isReplicated() || extraCctx.isLocal())
                    continue;

                for (int p = 0, parts = extraCctx.affinity().partitions(); p < parts; p++) {
                    List<ClusterNode> owners = extraCctx.topology().owners(p);

                    if (partLocs[p] == UNMAPPED_PARTS)
                        continue; // Skip unmapped partitions.

                    if (F.isEmpty(owners)) {
                        if (!F.isEmpty(dataNodes(extraCctx.groupId(), NONE))) {
                            logRetry("Failed to calculate nodes for SQL query (partition has no owners, but " +
                                "corresponding cache group has data nodes) [qryId=" + qryId +
                                ", cacheIds=" + cacheIds + ", cacheName=" + extraCctx.name() +
                                ", cacheId=" + extraCctx.cacheId() + ", part=" + p +
                                ", cacheGroupId=" + extraCctx.groupId() + ']');

                            return null; // Retry.
                        }

                        throw new CacheServerNotFoundException("Failed to find data nodes [cache=" + extraCctx.name() +
                            ", part=" + p + "]");
                    }

                    if (partLocs[p] == null)
                        partLocs[p] = new HashSet<>(owners);
                    else {
                        partLocs[p].retainAll(owners); // Intersection of owners.

                        if (partLocs[p].isEmpty()) {
                            logRetry("Failed to calculate nodes for SQL query (caches have no common data nodes for " +
                                "partition) [qryId=" + qryId + ", cacheIds=" + cacheIds +
                                ", lastCacheName=" + extraCctx.name() + ", lastCacheId=" + extraCctx.cacheId() +
                                ", part=" + p + ']');

                            return null; // Intersection is empty -> retry.
                        }
                    }
                }
            }

            // Filter nodes where not all the replicated caches loaded.
            for (Integer cacheId : cacheIds) {
                GridCacheContext<?, ?> extraCctx = cacheContext(cacheId);

                if (!extraCctx.isReplicated())
                    continue;

                Set<ClusterNode> dataNodes = replicatedUnstableDataNodes(extraCctx, qryId);

                if (F.isEmpty(dataNodes))
                    return null; // Retry.

                int part = 0;

                for (Set<ClusterNode> partLoc : partLocs) {
                    if (partLoc == UNMAPPED_PARTS)
                        continue; // Skip unmapped partition.

                    partLoc.retainAll(dataNodes);

                    if (partLoc.isEmpty()) {
                        logRetry("Failed to calculate nodes for SQL query (caches have no common data nodes for " +
                            "partition) [qryId=" + qryId + ", cacheIds=" + cacheIds +
                            ", lastReplicatedCacheName=" + extraCctx.name() +
                            ", lastReplicatedCacheId=" + extraCctx.cacheId() + ", part=" + part + ']');

                        return null; // Retry.
                    }

                    part++;
                }
            }
        }

        // Collect the final partitions mapping.
        Map<ClusterNode, IntArray> res = new HashMap<>();

        // Here partitions in all IntArray's will be sorted in ascending order, this is important.
        for (int p = 0; p < partLocs.length; p++) {
            Set<ClusterNode> pl = partLocs[p];

            // Skip unmapped partitions.
            if (pl == UNMAPPED_PARTS)
                continue;

            assert !F.isEmpty(pl) : pl;

            ClusterNode n = pl.size() == 1 ? F.first(pl) : F.rand(pl);

            IntArray parts = res.get(n);

            if (parts == null)
                res.put(n, parts = new IntArray());

            parts.add(p);
        }

        return res;
    }

    /**
     * Calculates data nodes for replicated caches on unstable topology.
     *
     * @param cacheIds Cache IDs.
     * @param qryId Query ID.
     * @return Collection of all data nodes owning all the caches or {@code null} for retry.
     */
    private Collection<ClusterNode> replicatedUnstableDataNodes(List<Integer> cacheIds, long qryId) {
        int i = 0;

        GridCacheContext<?, ?> cctx = cacheContext(cacheIds.get(i++));

        // The main cache is allowed to be partitioned.
        if (!cctx.isReplicated()) {
            assert cacheIds.size() > 1: "no extra replicated caches with partitioned main cache";

            // Just replace the main cache with the first one extra.
            cctx = cacheContext(cacheIds.get(i++));

            assert cctx.isReplicated(): "all the extra caches must be replicated here";
        }

        Set<ClusterNode> nodes = replicatedUnstableDataNodes(cctx, qryId);

        if (F.isEmpty(nodes))
            return null; // Retry.

        for (;i < cacheIds.size(); i++) {
            GridCacheContext<?, ?> extraCctx = cacheContext(cacheIds.get(i));

            if (extraCctx.isLocal())
                continue;

            if (!extraCctx.isReplicated())
                throw new CacheException("Queries running on replicated cache should not contain JOINs " +
                    "with tables in partitioned caches [replicatedCache=" + cctx.name() + ", " +
                    "partitionedCache=" + extraCctx.name() + "]");

            Set<ClusterNode> extraOwners = replicatedUnstableDataNodes(extraCctx, qryId);

            if (F.isEmpty(extraOwners))
                return null; // Retry.

            nodes.retainAll(extraOwners);

            if (nodes.isEmpty()) {
                logRetry("Failed to calculate nodes for SQL query (got disjoint node map for REPLICATED caches " +
                    "during rebalance) [qryId=" + qryId + ", cacheIds=" + cacheIds +
                    ", lastCache=" + extraCctx.name() + ", lastCacheId=" + extraCctx.cacheId() + ']');

                return null; // Retry.
            }
        }

        return nodes;
    }

    /**
     * Collects all the nodes owning all the partitions for the given replicated cache.
     *
     * @param cctx Cache context.
     * @param qryId Query ID.
     * @return Owning nodes or {@code null} if we can't find owners for some partitions.
     */
    private Set<ClusterNode> replicatedUnstableDataNodes(GridCacheContext<?,?> cctx, long qryId) {
        assert cctx.isReplicated() : cctx.name() + " must be replicated";

        String cacheName = cctx.name();

        Set<ClusterNode> dataNodes = new HashSet<>(dataNodes(cctx.groupId(), NONE));

        if (dataNodes.isEmpty())
            throw new CacheServerNotFoundException("Failed to find data nodes for cache: " + cacheName);

        // Find all the nodes owning all the partitions for replicated cache.
        for (int p = 0, parts = cctx.affinity().partitions(); p < parts; p++) {
            List<ClusterNode> owners = cctx.topology().owners(p);

            if (F.isEmpty(owners)) {
                logRetry("Failed to calculate nodes for SQL query (partition of a REPLICATED cache has no owners) [" +
                    "qryId=" + qryId + ", cacheName=" + cctx.name() + ", cacheId=" + cctx.cacheId() +
                    ", part=" + p + ']');

                return null; // Retry.
            }

            dataNodes.retainAll(owners);

            if (dataNodes.isEmpty()) {
                logRetry("Failed to calculate nodes for SQL query (partitions of a REPLICATED has no common owners) [" +
                    "qryId=" + qryId + ", cacheName=" + cctx.name() + ", cacheId=" + cctx.cacheId() +
                    ", lastPart=" + p + ']');

                return null; // Retry.
            }
        }

        return dataNodes;
    }

    /**
     * @param grpId Cache group ID.
     * @param topVer Topology version.
     * @return Collection of data nodes.
     */
    private Collection<ClusterNode> dataNodes(int grpId, AffinityTopologyVersion topVer) {
        Collection<ClusterNode> res = ctx.discovery().cacheGroupAffinityNodes(grpId, topVer);

        return res != null ? res : Collections.emptySet();
    }

    /**
     *
     * @param partsMap Partitions map.
     * @param parts Partitions.
     * @return Result.
     */
    private static Map<ClusterNode, IntArray> narrowForQuery(Map<ClusterNode, IntArray> partsMap, int[] parts) {
        if (parts == null)
            return partsMap;

        Map<ClusterNode, IntArray> cp = U.newHashMap(partsMap.size());

        for (Map.Entry<ClusterNode, IntArray> entry : partsMap.entrySet()) {
            IntArray filtered = new IntArray(parts.length);

            IntArray orig = entry.getValue();

            for (int i = 0; i < orig.size(); i++) {
                int p = orig.get(i);

                if (Arrays.binarySearch(parts, p) >= 0)
                    filtered.add(p);
            }

            if (filtered.size() > 0)
                cp.put(entry.getKey(), filtered);
        }

        return cp.isEmpty() ? null : cp;
    }

    /**
     * @param cacheIds Cache IDs.
     * @return The first partitioned cache context.
     */
    public GridCacheContext<?,?> findFirstPartitioned(List<Integer> cacheIds) {
        for (int i = 0; i < cacheIds.size(); i++) {
            GridCacheContext<?, ?> cctx = cacheContext(cacheIds.get(i));

            if (i == 0 && cctx.isLocal())
                throw new CacheException("Cache is LOCAL: " + cctx.name());

            if (!cctx.isReplicated() && !cctx.isLocal())
                return cctx;
        }

        throw new IllegalStateException("Failed to find partitioned cache.");
    }

    /**
     * Load failed partition reservation.
     *
     * @param msg Message.
     */
    private void logRetry(String msg) {
        log.info(msg);
    }
}
