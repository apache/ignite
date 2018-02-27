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

package org.apache.ignite.internal.processors.cache.distributed.dht;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.CachePartitionPartialCountersMap;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsSingleMessage;
import org.apache.ignite.internal.util.typedef.internal.SB;

import static org.apache.ignite.events.EventType.EVT_NODE_JOINED;

/**
 * Class to validate partitions update counters and cache sizes during exchange process.
 */
public class GridDhtPartitionsStateValidator {
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
    public void validatePartitionCountersAndSizes(GridDhtPartitionsExchangeFuture fut,
                                                  GridDhtPartitionTopology top,
                                                  Map<UUID, GridDhtPartitionsSingleMessage> messages) throws IgniteCheckedException {
        // Ignore just joined nodes.
        final Set<UUID> ignoringNodes = fut.events().events()
                .stream()
                .filter(evt -> evt.type() == EVT_NODE_JOINED)
                .map(evt -> evt.eventNode().id())
                .collect(Collectors.toSet());

        AffinityTopologyVersion topVer = fut.context().events().topologyVersion();

        // Validate update counters.
        Map<Integer, Map<UUID, Long>> result = validatePartitionsUpdateCounters(top, messages, ignoringNodes);
        if (!result.isEmpty())
            throw new IgniteCheckedException("Partitions update counters are inconsistent for " + fold(topVer, result));

        // Validate cache sizes.
        result = validatePartitionsSizes(top, messages, ignoringNodes);
        if (!result.isEmpty())
            throw new IgniteCheckedException("Partitions cache sizes are inconsistent for " + fold(topVer, result));
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
     Map<Integer, Map<UUID, Long>> validatePartitionsUpdateCounters(
            GridDhtPartitionTopology top,
            Map<UUID, GridDhtPartitionsSingleMessage> messages,
            Set<UUID> ignoringNodes) {
        Map<Integer, Map<UUID, Long>> invalidPartitions = new HashMap<>();

        Map<Integer, Long> updateCountersByPartitions = new HashMap<>();
        Map<Integer, UUID> updateCounterNodesByPartitions = new HashMap<>();

        // Populate counters statistics from local node partitions.
        for (GridDhtLocalPartition part : top.currentLocalPartitions()) {
            if (top.partitionState(cctx.localNodeId(), part.id()) != GridDhtPartitionState.OWNING)
                continue;

            updateCountersByPartitions.put(part.id(), part.updateCounter());
            updateCounterNodesByPartitions.put(part.id(), cctx.localNodeId());
        }

        // Then process and validate counters from other nodes.
        for (Map.Entry<UUID, GridDhtPartitionsSingleMessage> e : messages.entrySet()) {
            UUID nodeId = e.getKey();
            if (ignoringNodes.contains(nodeId))
                continue;

            GridDhtPartitionsSingleMessage msg = e.getValue();
            CachePartitionPartialCountersMap countersMap = msg.partitionUpdateCounters(top.groupId(), top.partitions());

            for (int i = 0; i < countersMap.size(); i++) {
                int part = countersMap.partitionAt(i);

                if (top.partitionState(nodeId, part) != GridDhtPartitionState.OWNING)
                    continue;

                long currentCounter = countersMap.updateCounterAt(i);

                Long existingCounter = updateCountersByPartitions.get(part);
                if (existingCounter == null) {
                    updateCountersByPartitions.put(part, currentCounter);
                    updateCounterNodesByPartitions.put(part, nodeId);
                }
                else {
                    if (existingCounter != currentCounter) {
                        UUID existingNodeId = updateCounterNodesByPartitions.get(part);

                        invalidPartitions.computeIfAbsent(part, p -> {
                            Map<UUID, Long> counters = new HashMap<>();
                            counters.put(existingNodeId, existingCounter);
                            return counters;
                        });

                        invalidPartitions.get(part).put(nodeId, currentCounter);
                    }
                }
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
     Map<Integer, Map<UUID, Long>> validatePartitionsSizes(
            GridDhtPartitionTopology top,
            Map<UUID, GridDhtPartitionsSingleMessage> messages,
            Set<UUID> ignoringNodes) {
        Map<Integer, Map<UUID, Long>> invalidPartitions = new HashMap<>();

        Map<Integer, Long> sizesByPartitions = new HashMap<>();
        Map<Integer, UUID> sizeNodesByPartitions = new HashMap<>();

        // Populate sizes statistics from local node partitions.
        for (GridDhtLocalPartition part : top.currentLocalPartitions()) {
            if (top.partitionState(cctx.localNodeId(), part.id()) != GridDhtPartitionState.OWNING)
                continue;

            sizesByPartitions.put(part.id(), part.fullSize());
            sizeNodesByPartitions.put(part.id(), cctx.localNodeId());
        }

        // Then process and validate counters from other nodes.
        for (Map.Entry<UUID, GridDhtPartitionsSingleMessage> e : messages.entrySet()) {
            UUID nodeId = e.getKey();
            if (ignoringNodes.contains(nodeId))
                continue;

            GridDhtPartitionsSingleMessage msg = e.getValue();
            Map<Integer, Long> sizesMap = msg.partitionSizes(top.groupId());
            for (Map.Entry<Integer, Long> entry : sizesMap.entrySet()) {
                int part = entry.getKey();

                if (top.partitionState(nodeId, part) != GridDhtPartitionState.OWNING)
                    continue;

                long currentSize = entry.getValue();

                Long existingSize = sizesByPartitions.get(part);
                if (existingSize == null) {
                    sizesByPartitions.put(part, currentSize);
                    sizeNodesByPartitions.put(part, nodeId);
                }
                else {
                    if (existingSize != currentSize) {
                        UUID existingNodeId = sizeNodesByPartitions.get(part);

                        invalidPartitions.computeIfAbsent(part, p -> {
                            Map<UUID, Long> counters = new HashMap<>();
                            counters.put(existingNodeId, existingSize);
                            return counters;
                        });

                        invalidPartitions.get(part).put(nodeId, currentSize);
                    }
                }
            }
        }

        return invalidPartitions;
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
        for (Map.Entry<Integer, Map<UUID, Long>> p : invalidPartitions.entrySet()) {
            sb.a("Part ").a(p).a(": [");
            for (Map.Entry<UUID, Long> e : p.getValue().entrySet()) {
                Object consistentId = cctx.discovery().node(topVer, e.getKey()).consistentId();
                sb.a(consistentId).a("=").a(e.getValue()).a(" ");
            }
            sb.a("] ");
        }

        return sb.toString();
    }
}
