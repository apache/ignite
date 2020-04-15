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

package org.apache.ignite.internal.processors.cache.distributed.dht.topology;

import java.util.AbstractMap;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.CachePartitionPartialCountersMap;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsSingleMessage;
import org.apache.ignite.internal.util.lang.IgnitePair;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.apache.ignite.lang.IgniteProductVersion;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.events.EventType.EVT_NODE_JOINED;

/**
 * Class to validate partitions update counters and cache sizes during exchange
 * process.
 */
public class GridDhtPartitionsStateValidator {
    /** Version since node is able to send cache sizes in {@link GridDhtPartitionsSingleMessage}. */
    private static final IgniteProductVersion SIZES_VALIDATION_AVAILABLE_SINCE = IgniteProductVersion.fromString("2.5.0");

    /** Cache shared context. */
    private final GridCacheSharedContext<?, ?> cctx;

    /**
     * Collection of partitions that did not pass validation.
     * This collection is supported and updated by coordinator node only.
     * Represents the following mapping: group id -> set of partitions.
     */
    private Map<Integer, Set<Integer>> invalidParts = new ConcurrentHashMap<>();

    /**
     * Constructor.
     *
     * @param cctx Cache shared context.
     */
    public GridDhtPartitionsStateValidator(GridCacheSharedContext<?, ?> cctx) {
        this.cctx = cctx;
    }

    /**
     * Validates partition states - update counters and cache sizes
     * for all nodes.
     * If update counter value or cache size for the same partitions are
     * different on some nodes
     * method throws exception with full information about inconsistent
     * partitions.
     *
     * @param fut Current exchange future.
     * @param top Topology to validate.
     * @param messages Single messages received from all nodes.
     * @throws PartitionStateValidationException If validation failed.
     * Exception message contains full information about all partitions which
     * update counters or cache sizes are not consistent.
     */
    public void validatePartitionCountersAndSizes(
        GridDhtPartitionsExchangeFuture fut,
        GridDhtPartitionTopology top,
        Map<UUID, GridDhtPartitionsSingleMessage> messages
    ) throws PartitionStateValidationException {
        final Set<UUID> ignoringNodes = new HashSet<>();

        // Ignore just joined nodes.
        for (DiscoveryEvent evt : fut.events().events()) {
            if (evt.type() == EVT_NODE_JOINED)
                ignoringNodes.add(evt.eventNode().id());
        }

        AffinityTopologyVersion topVer = fut.context().events().topologyVersion();

        // Validate update counters.
        Map<Integer, Map<UUID, Long>> result = validatePartitionsUpdateCounters(top, messages, ignoringNodes);

        if (!result.isEmpty()) {
            Set<Integer> parts = new HashSet<>(result.keySet());

            invalidParts.putIfAbsent(top.groupId(), parts);

            throw new PartitionStateValidationException(
                "Partitions update counters are inconsistent for " + fold(topVer, result),
                topVer,
                top.groupId(),
                parts);
        }

        // For sizes validation ignore also nodes which are not able to send
        // cache sizes.
        for (UUID id : messages.keySet()) {
            ClusterNode node = cctx.discovery().node(id);
            if (node != null && node.version().compareTo(SIZES_VALIDATION_AVAILABLE_SINCE) < 0)
                ignoringNodes.add(id);
        }

            result = validatePartitionsSizes(top, messages, ignoringNodes);

            if (!result.isEmpty()) {
                Set<Integer> parts = new HashSet<>(result.keySet());

            invalidParts.putIfAbsent(top.groupId(), parts);

                throw new PartitionStateValidationException(
                    "Partitions cache sizes are inconsistent for " + fold(topVer, result),
                    topVer,
                    top.groupId(),
                    parts);
            }
    }

    /**
     * Returns set of partitions that did not pass validation for all caches
     * that were checked.
     *
     * @return Collection of invalid partitions.
     * @see GridDhtPartitionsStateValidator#validatePartitionCountersAndSizes
     */
    public Map<Integer, Set<Integer>> invalidPartitions() {
        return invalidParts;
    }

    /**
     * Returns set of partitions that did not pass validation for the given cache group.
     *
     * @param groupId Cache group id.
     * @return Set of invalid partitions.
     * @see GridDhtPartitionsStateValidator#validatePartitionCountersAndSizes
     */
    public Set<Integer> invalidPartitions(int groupId) {
        return invalidParts.getOrDefault(groupId, Collections.emptySet());
    }

    /**
     * Cleans up resources to avoid excessive memory usage.
     */
    public void cleanUp() {
        invalidParts = null;
    }

    /**
     * Checks what partitions from given {@code singleMsg} message should be
     * excluded from validation.
     *
     * @param top Topology to validate.
     * @param nodeId Node which sent single message.
     * @param countersMap Counters map.
     * @param sizesMap Sizes map.
     * @return Set of partition ids should be excluded from validation.
     */
    @Nullable private Set<Integer> shouldIgnore(
        GridDhtPartitionTopology top,
        UUID nodeId,
        CachePartitionPartialCountersMap countersMap,
        Map<Integer, Long> sizesMap
    ) {
        Set<Integer> ignore = null;

        for (int i = 0; i < countersMap.size(); i++) {
            int p = countersMap.partitionAt(i);

            if (top.partitionState(nodeId, p) != GridDhtPartitionState.OWNING) {
                if (ignore == null)
                    ignore = new HashSet<>();

                ignore.add(p);

                continue;
            }

            long updateCounter = countersMap.updateCounterAt(i);
            long size = sizesMap.getOrDefault(p, 0L);

            // Do not validate partitions with zero update counter and size.
            if (updateCounter == 0 && size == 0) {
                if (ignore == null)
                    ignore = new HashSet<>();

                ignore.add(p);
            }
        }

        return ignore;
    }

    /**
     * Validate partitions update counters for given {@code top}.
     *
     * @param top Topology to validate.
     * @param messages Single messages received from all nodes.
     * @param ignoringNodes Nodes for what we ignore validation.
     * @return Invalid partitions map with following structure:
     * (partId, (nodeId, updateCounter)).
     * If map is empty validation is successful.
     */
    public Map<Integer, Map<UUID, Long>> validatePartitionsUpdateCounters(
        GridDhtPartitionTopology top,
        Map<UUID, GridDhtPartitionsSingleMessage> messages,
        Set<UUID> ignoringNodes
    ) {
        Map<Integer, Map<UUID, Long>> invalidPartitions = new HashMap<>();

        Map<Integer, AbstractMap.Entry<UUID, Long>> updateCountersAndNodesByPartitions = new HashMap<>();

        // Populate counters statistics from local node partitions.
        for (GridDhtLocalPartition part : top.currentLocalPartitions()) {
            if (part.state() != GridDhtPartitionState.OWNING)
                continue;

            if (part.updateCounter() == 0 && part.fullSize() == 0)
                continue;

            updateCountersAndNodesByPartitions.put(part.id(), new AbstractMap.SimpleEntry<>(cctx.localNodeId(), part.updateCounter()));
        }

        int partitions = top.partitions();

        // Then process and validate counters from other nodes.
        for (Map.Entry<UUID, GridDhtPartitionsSingleMessage> e : messages.entrySet()) {
            UUID nodeId = e.getKey();
            if (ignoringNodes.contains(nodeId))
                continue;

            final GridDhtPartitionsSingleMessage message = e.getValue();

            CachePartitionPartialCountersMap countersMap = message.partitionUpdateCounters(top.groupId(), partitions);

            Map<Integer, Long> sizesMap = message.partitionSizes(top.groupId());

            Set<Integer> ignorePartitions = shouldIgnore(top, nodeId, countersMap, sizesMap);

            for (int i = 0; i < countersMap.size(); i++) {
                int p = countersMap.partitionAt(i);

                if (ignorePartitions != null && ignorePartitions.contains(p))
                    continue;

                long currentCounter = countersMap.updateCounterAt(i);

                process(invalidPartitions, updateCountersAndNodesByPartitions, p, nodeId, currentCounter);
            }
        }

        return invalidPartitions;
    }

    /**
     * Validate partitions cache sizes for given {@code top}.
     *
     * @param top Topology to validate.
     * @param messages Single messages received from all nodes.
     * @param ignoringNodes Nodes for what we ignore validation.
     * @return Invalid partitions map with following structure:
     * (partId, (nodeId, cacheSize)).
     * If map is empty validation is successful.
     */
    public Map<Integer, Map<UUID, Long>> validatePartitionsSizes(
        GridDhtPartitionTopology top,
        Map<UUID, GridDhtPartitionsSingleMessage> messages,
        Set<UUID> ignoringNodes
    ) {
        Map<Integer, Map<UUID, Long>> invalidPartitions = new HashMap<>();

        Map<Integer, AbstractMap.Entry<UUID, Long>> sizesAndNodesByPartitions = new HashMap<>();

        // Populate sizes statistics from local node partitions.
        for (GridDhtLocalPartition part : top.currentLocalPartitions()) {
            if (part.state() != GridDhtPartitionState.OWNING)
                continue;

            if (part.updateCounter() == 0 && part.fullSize() == 0)
                continue;

            sizesAndNodesByPartitions.put(part.id(), new AbstractMap.SimpleEntry<>(cctx.localNodeId(), part.fullSize()));
        }

        int partitions = top.partitions();

        // Then process and validate sizes from other nodes.
        for (Map.Entry<UUID, GridDhtPartitionsSingleMessage> e : messages.entrySet()) {
            UUID nodeId = e.getKey();
            if (ignoringNodes.contains(nodeId))
                continue;

            final GridDhtPartitionsSingleMessage message = e.getValue();

            CachePartitionPartialCountersMap countersMap = message.partitionUpdateCounters(top.groupId(), partitions);

            Map<Integer, Long> sizesMap = message.partitionSizes(top.groupId());

            Set<Integer> ignorePartitions = shouldIgnore(top, nodeId, countersMap, sizesMap);

            for (int i = 0; i < countersMap.size(); i++) {
                int p = countersMap.partitionAt(i);

                if (ignorePartitions != null && ignorePartitions.contains(p))
                    continue;

                long currentSize = sizesMap.getOrDefault(p, 0L);

                process(invalidPartitions, sizesAndNodesByPartitions, p, nodeId, currentSize);
            }
        }

        return invalidPartitions;
    }

    /**
     * Processes given {@code counter} for partition {@code part}
     * reported by {@code node}.
     * Populates {@code invalidPartitions} map if existing counter
     * and current {@code counter} are different.
     *
     * @param invalidPartitions Invalid partitions map.
     * @param countersAndNodes Current map of counters and nodes by partitions.
     * @param part Processing partition.
     * @param node Node id.
     * @param counter Counter value reported by {@code node}.
     */
    private void process(
        Map<Integer, Map<UUID, Long>> invalidPartitions,
        Map<Integer, AbstractMap.Entry<UUID, Long>> countersAndNodes,
        int part,
        UUID node,
        long counter
    ) {
        AbstractMap.Entry<UUID, Long> existingData = countersAndNodes.get(part);

        if (existingData == null)
            countersAndNodes.put(part, new AbstractMap.SimpleEntry<>(node, counter));

        if (existingData != null && counter != existingData.getValue()) {
            if (!invalidPartitions.containsKey(part)) {
                Map<UUID, Long> map = new HashMap<>();
                map.put(existingData.getKey(), existingData.getValue());
                invalidPartitions.put(part, map);
            }

            invalidPartitions.get(part).put(node, counter);
        }
    }

    /**
     * Folds given map of invalid partition states to string representation
     * in the following format:
     * Part [id]: [consistentId=value*]
     *
     * Value can be both update counter or cache size.
     *
     * @param topVer Last topology version.
     * @param invalidPartitions Invalid partitions map.
     * @return String representation of invalid partitions.
     */
    private String fold(AffinityTopologyVersion topVer, Map<Integer, Map<UUID, Long>> invalidPartitions) {
        SB sb = new SB();

        NavigableMap<Integer, Map<UUID, Long>> sortedPartitions = new TreeMap<>(invalidPartitions);

        for (Map.Entry<Integer, Map<UUID, Long>> p : sortedPartitions.entrySet()) {
            sb.a("Part ").a(p.getKey()).a(": [");
            for (Map.Entry<UUID, Long> e : p.getValue().entrySet()) {
                Object consistentId = cctx.discovery().node(topVer, e.getKey()).consistentId();
                sb.a(consistentId).a("=").a(e.getValue()).a(" ");
            }
            sb.a("] ");
        }

        return sb.toString();
    }

    /**
     * Folds given map of invalid partition states to string representation
     * in the following format:
     * Part [id]: [consistentId=value meta=[updCnt=value, size=value]]
     * @param topVer Topology version.
     * @param invalidPartitionsCounters Invalid partitions counters map.
     * @param invalidPartitionsSize Invalid partitions size map.
     * @return value is String in the following format: Part [id]:
     * [consistentId=value meta=[updCnt=value, size=value]]
     */
    private String fold(AffinityTopologyVersion topVer, Map<Integer, Map<UUID, Long>> invalidPartitionsCounters,
        Map<Integer, Map<UUID, Long>> invalidPartitionsSize) {
        SB sb = new SB();

        NavigableMap<Integer, Map<UUID, IgnitePair<Long>>> sortedAllPartitions = new TreeMap<>();

        Set<Integer> allKeys = new HashSet<>(invalidPartitionsCounters.keySet());

        allKeys.addAll(invalidPartitionsSize.keySet());

        for (Integer p : allKeys) {
            Map<UUID, IgnitePair<Long>> map = new HashMap<>();

            fillMapForPartition(invalidPartitionsCounters.get(p), map, true);
            fillMapForPartition(invalidPartitionsSize.get(p), map, false);

            sortedAllPartitions.put(p, map);
        }

        for (Map.Entry<Integer, Map<UUID, IgnitePair<Long>>> p : sortedAllPartitions.entrySet()) {
            sb.a("Part ").a(p.getKey()).a(": [");
            for (Map.Entry<UUID, IgnitePair<Long>> e : p.getValue().entrySet()) {
                Object consistentId = cctx.discovery().node(topVer, e.getKey()).consistentId();
                sb.a("consistentId=").a(consistentId).a(" meta=[updCnt=").a(e.getValue().get1())
                    .a(", size=").a(e.getValue().get2()) .a("] ");
            }
            sb.a("] ");
        }

        return sb.toString();
    }

    /**
     * Add pair of counters and size in result map.
     * @param sourceMap PartitionCounters or PartitionSize
     * @param resultMap  result map with pair of values
     */
    private void fillMapForPartition(Map<UUID, Long> sourceMap,
        Map<UUID, IgnitePair<Long>> resultMap, boolean isFirst) {
        if (sourceMap!=null) {
            sourceMap.forEach((uuid, val) -> {
                IgnitePair<Long> pair = resultMap.computeIfAbsent(uuid, u -> new IgnitePair<>());
                if (isFirst)
                    pair.set1(val);
                else
                    pair.set2(val);
            });
        }
    }
}
