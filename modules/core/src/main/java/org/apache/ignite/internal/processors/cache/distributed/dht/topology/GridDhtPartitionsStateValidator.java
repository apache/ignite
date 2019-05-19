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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.CachePartitionPartialCountersMap;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsSingleMessage;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.apache.ignite.lang.IgniteProductVersion;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.events.EventType.EVT_NODE_JOINED;

/**
 * Class to validate partitions update counters and cache sizes during exchange process.
 */
public class GridDhtPartitionsStateValidator {
    /** Version since node is able to send cache sizes in {@link GridDhtPartitionsSingleMessage}. */
    private static final IgniteProductVersion SIZES_VALIDATION_AVAILABLE_SINCE = IgniteProductVersion.fromString("2.5.0");

    /** Cache shared context. */
    private final GridCacheSharedContext<?, ?> cctx;

    /**
     * Constructor.
     *
     * @param cctx Cache shared context.
     */
    public GridDhtPartitionsStateValidator(GridCacheSharedContext<?, ?> cctx) {
        this.cctx = cctx;
    }

    /**
     * Validates partition states - update counters and cache sizes for all nodes.
     * If update counter value or cache size for the same partitions are different on some nodes
     * method throws exception with full information about inconsistent partitions.
     *
     * @param fut Current exchange future.
     * @param top Topology to validate.
     * @param messages Single messages received from all nodes.
     * @throws IgniteCheckedException If validation failed. Exception message contains
     * full information about all partitions which update counters or cache sizes are not consistent.
     */
    public void validatePartitionCountersAndSizes(
        GridDhtPartitionsExchangeFuture fut,
        GridDhtPartitionTopology top,
        Map<UUID, GridDhtPartitionsSingleMessage> messages
    ) throws IgniteCheckedException {
        final Set<UUID> ignoringNodes = new HashSet<>();

        // Ignore just joined nodes.
        for (DiscoveryEvent evt : fut.events().events()) {
            if (evt.type() == EVT_NODE_JOINED)
                ignoringNodes.add(evt.eventNode().id());
        }

        AffinityTopologyVersion topVer = fut.context().events().topologyVersion();

        // Validate update counters.
        Map<Integer, Map<UUID, Long>> result = validatePartitionsUpdateCounters(top, messages, ignoringNodes);

        if (!result.isEmpty())
            throw new IgniteCheckedException("Partitions update counters are inconsistent for " + fold(topVer, result));

        // For sizes validation ignore also nodes which are not able to send cache sizes.
        for (UUID id : messages.keySet()) {
            ClusterNode node = cctx.discovery().node(id);
            if (node != null && node.version().compareTo(SIZES_VALIDATION_AVAILABLE_SINCE) < 0)
                ignoringNodes.add(id);
        }

        if (!cctx.cache().cacheGroup(top.groupId()).mvccEnabled()) { // TODO: Remove "if" clause in IGNITE-9451.
            // Validate cache sizes.
            result = validatePartitionsSizes(top, messages, ignoringNodes);

            if (!result.isEmpty())
                throw new IgniteCheckedException("Partitions cache sizes are inconsistent for " + fold(topVer, result));
        }
    }

    /**
     * Checks what partitions from given {@code singleMsg} message should be excluded from validation.
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
     * @return Invalid partitions map with following structure: (partId, (nodeId, updateCounter)).
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
     * @return Invalid partitions map with following structure: (partId, (nodeId, cacheSize)).
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
     * Processes given {@code counter} for partition {@code part} reported by {@code node}.
     * Populates {@code invalidPartitions} map if existing counter and current {@code counter} are different.
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
     * Folds given map of invalid partition states to string representation in the following format:
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
}
