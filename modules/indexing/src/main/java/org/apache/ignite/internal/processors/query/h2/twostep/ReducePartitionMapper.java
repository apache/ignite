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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PrimitiveIterator;
import java.util.Set;
import java.util.stream.IntStream;
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
import org.apache.ignite.lang.IgniteBiTuple;
import org.h2.util.IntArray;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.cache.PartitionLossPolicy.READ_ONLY_SAFE;
import static org.apache.ignite.cache.PartitionLossPolicy.READ_WRITE_SAFE;
import static org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion.NONE;

/**
 * Reduce partition mapper.
 */
public class ReducePartitionMapper {
    /** Kernal context. */
    private final GridKernalContext ctx;

    /** */
    private final IgniteLogger log;

    /**
     * Constructor.
     *
     * @param ctx Kernal context.
     * @param log Logger.
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
                partsMap = partitionedUnstableDataNodes(cacheIds, parts, qryId);

                if (partsMap != null)
                    nodes = partsMap.keySet();

                if (parts != null)
                    qryMap = partsMap;
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
     * @param parts Explicit partitions.
     * @param qryId Query ID.
     * @return Partition mapping or {@code null} if we can't calculate it due to repartitioning and we need to retry.
     */
    private Map<ClusterNode, IntArray> partitionedUnstableDataNodes(List<Integer> cacheIds, int[] parts, long qryId) {
        List<GridCacheContext<?, ?>> cctxs = new ArrayList<>(cacheIds.size());

        GridCacheContext<?, ?> firstPartitioned = null; Set<ClusterNode> replicatedOwners = null;

        // 1) Check whether all involved caches have the same partitions number and
        // find nodes owning all partitions of involved replicated caches if needed
        for (int i = 0; i < cacheIds.size(); i++) {
            GridCacheContext<?, ?> cctx = cacheContext(cacheIds.get(i));

            if (cctx.isLocal()) {
                if (i == 0)
                    throw new CacheException("Cache is LOCAL: " + cctx.name());

                continue;
            }

            if (cctx.isPartitioned()) {
                if (firstPartitioned == null)
                    firstPartitioned = cctx;
                else if (firstPartitioned.affinity().partitions() != cctx.affinity().partitions()) {
                    throw new CacheException(
                        "Number of partitions must be the same for correct collocation [cache1=" +
                            firstPartitioned.name() + ", parts1=" + firstPartitioned.affinity().partitions() +
                            ", cache2=" + firstPartitioned.name() + ", parts2=" + cctx.affinity().partitions() + "]");
                }

                cctxs.add(cctx);

                continue;
            }

            Set<ClusterNode> nodes = replicatedUnstableDataNodes(cctx, qryId);

            if (!F.isEmpty(replicatedOwners) && !F.isEmpty(nodes))
                nodes.retainAll(replicatedOwners);

            if (F.isEmpty(replicatedOwners = nodes)) {
                logRetry("Failed to calculate nodes for SQL query (caches have no common data nodes) " +
                    "[qryId=" + qryId + ", cacheIds=" + cacheIds + ']');

                return null; // Retry.
            }
        }

        if (firstPartitioned == null)
            throw new IllegalStateException("Failed to find partitioned cache.");

        PrimitiveIterator.OfInt it = (parts != null ? Arrays.stream(parts) :
                    IntStream.range(0, firstPartitioned.affinity().partitions())).iterator();

        Map<ClusterNode, Set<Integer>> nodeToParts = new HashMap<>();

        // 2) Iterate over involved partitions and find all nodes having it excluding nodes
        // which do not have all partitions of involved replicated caches if needed
        while (it.hasNext()) {
            int p = it.nextInt();

            Set<ClusterNode> nodes = replicatedOwners;

            for (GridCacheContext<?, ?> cctx : cctxs) {
                List<ClusterNode> partOwners = cctx.topology().owners(p);

                if (F.isEmpty(partOwners)) {
                    // Handle special case: no mapping is configured for a partition.
                    if (F.isEmpty(cctx.affinity().assignment(NONE).get(p)))
                        continue;

                    if (!F.isEmpty(dataNodes(cctx.groupId()))) {
                        logRetry("Failed to calculate nodes for SQL query (partition has no owners, but corresponding " +
                            "cache group has data nodes) [qryId=" + qryId + ", cacheIds=" + cacheIds +
                            ", cacheName=" + cctx.name() + ", cacheId=" + cctx.cacheId() + ", part=" + p +
                            ", cacheGroupId=" + cctx.groupId() + ']');

                        return null; // Retry.
                    }

                    throw new CacheServerNotFoundException("Failed to find data nodes [cache=" + cctx.name() + ", part=" + p + "]");
                }

                if (!F.isEmpty(nodes))
                    partOwners.retainAll(nodes);

                if (F.isEmpty(partOwners)) {
                    logRetry("Failed to calculate nodes for SQL query (caches have no common data nodes for " +
                        "partition) [qryId=" + qryId + ", cacheIds=" + cacheIds + ", part=" + p + ']');

                    return null; // Retry.
                }

                nodes = new HashSet<>(partOwners);
            }

            if (F.isEmpty(nodes)) {
                logRetry("Failed to calculate nodes for SQL query (caches have no common data nodes for " +
                    "partition) [qryId=" + qryId + ", cacheIds=" + cacheIds + ", part=" + p + ']');

                return null; // Retry.
            }

            for (ClusterNode node : nodes)
                nodeToParts.computeIfAbsent(node, n -> new HashSet<>()).add(p);
        }

        return processPartitionsMappingTest(nodeToParts);
    }

    /** */
    @NotNull private Map<ClusterNode, IntArray> processPartitionsMappingTest(Map<ClusterNode, Set<Integer>> nodeToParts) {
        Map<Integer, List<ClusterNode>> partToNodes = nodeToParts.entrySet().stream()
            .flatMap(e -> e.getValue().stream().map(p -> new IgniteBiTuple<>(e.getKey(), p)))
            .collect(groupingBy(IgniteBiTuple::get2, mapping(IgniteBiTuple::get1, toList())));

        Map<ClusterNode, IntArray> res = new HashMap<>();

        partToNodes.forEach((key, value) -> res.computeIfAbsent(F.rand(value), n -> new IntArray()).add(key));

        return res;
    }

    /**
     * Here we try to reduce nodes amount as possible. We prefer nodes having maximum number of involved partitions.
     * All nodes have all involved partitions. Partitions lists do not intersect between nodes.
     *
     * @param nodeToParts All available for query executing nodes mapped to their owned partitions.
     * @return Nodes to execute query.
     */
    @NotNull private Map<ClusterNode, IntArray> processPartitionsMapping(Map<ClusterNode, Set<Integer>> nodeToParts) {
        Map<ClusterNode, IntArray> res = new HashMap<>();

        Set<Integer> lastProcessed = null;

        while (!nodeToParts.isEmpty()) {
            Map.Entry<ClusterNode, Set<Integer>> curr = null;

            for (Iterator<Map.Entry<ClusterNode, Set<Integer>>> it = nodeToParts.entrySet().iterator(); it.hasNext(); ) {
                Map.Entry<ClusterNode, Set<Integer>> e = it.next();

                if (lastProcessed != null)
                    e.getValue().removeAll(lastProcessed);

                if (e.getValue().isEmpty()) {
                    it.remove();

                    continue;
                }

                if (curr == null || e.getValue().size() > curr.getValue().size())
                    curr = e;
            }

            if (curr == null)
                break;

            IntArray array = res.computeIfAbsent(curr.getKey(), n -> new IntArray());

            for (Integer p : curr.getValue())
                array.add(p);

            lastProcessed = nodeToParts.remove(curr.getKey());
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

        Set<ClusterNode> dataNodes = new HashSet<>(dataNodes(cctx.groupId()));

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
     * @return Collection of data nodes.
     */
    private Collection<ClusterNode> dataNodes(int grpId) {
        Collection<ClusterNode> res = ctx.discovery().cacheGroupAffinityNodes(grpId, AffinityTopologyVersion.NONE);

        return res != null ? res : Collections.emptySet();
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
