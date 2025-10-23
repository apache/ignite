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

package org.apache.ignite.internal.processors.query.calcite.metadata;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState;
import org.apache.ignite.internal.processors.query.calcite.message.CalciteMessage;
import org.apache.ignite.internal.processors.query.calcite.message.MessageType;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.util.GridIntList;
import org.apache.ignite.internal.util.UUIDCollectionMessage;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

/** */
public class ColocationGroup implements CalciteMessage {
    /** */
    @Order(value = 0, method = "sourceIds")
    private long[] sourceIds;

    /** */
    @Order(1)
    private List<UUID> nodeIds;

    /** */
    private List<List<UUID>> assignments;

    /** */
    @Order(value = 2, method = "assigmentsMessage")
    private @Nullable List<UUIDCollectionMessage> assigmentsMsg;

    /**
     * Flag, indacating that assignment is formed by original cache assignment for given topology.
     * In case of {@code true} value we can skip assignment marshalling and calc assignment on remote nodes.
     */
    private boolean primaryAssignment;

    /** */
    public static ColocationGroup forNodes(List<UUID> nodeIds) {
        return new ColocationGroup(null, nodeIds, null);
    }

    /** */
    public static ColocationGroup forAssignments(List<List<UUID>> assignments) {
        return new ColocationGroup(null, null, assignments, true);
    }

    /** */
    public static ColocationGroup forSourceId(long sourceId) {
        return new ColocationGroup(new long[] {sourceId}, null, null);
    }

    /** */
    public ColocationGroup local(UUID nodeId) {
        List<List<UUID>> locAssignments = null;
        if (assignments != null) {
            locAssignments = assignments.stream()
                    .map(l -> nodeId.equals(l.get(0)) ? l : Collections.<UUID>emptyList())
                    .collect(Collectors.toList());
        }

        return new ColocationGroup(sourceIds == null ? null : Arrays.copyOf(sourceIds, sourceIds.length),
            Collections.singletonList(nodeId), locAssignments);
    }

    /** */
    public ColocationGroup() {
    }

    /** */
    private ColocationGroup(long[] sourceIds, List<UUID> nodeIds, List<List<UUID>> assignments) {
        this.sourceIds = sourceIds;
        this.nodeIds = nodeIds;
        this.assignments = assignments;
    }

    /** */
    private ColocationGroup(long[] sourceIds, List<UUID> nodeIds, List<List<UUID>> assignments, boolean primaryAssignment) {
        this(sourceIds, nodeIds, assignments);

        this.primaryAssignment = primaryAssignment;

        if (primaryAssignment && assignments != null) {
            assigmentsMsg = new ArrayList<>(assignments.size());

            assignments.forEach(a -> assigmentsMsg.add(new UUIDCollectionMessage(a)));
        }
    }

    /**
     * @return Lists of nodes capable to execute a query fragment for what the mapping is calculated.
     */
    public List<UUID> nodeIds() {
        return nodeIds == null ? Collections.emptyList() : nodeIds;
    }

    /** */
    public void nodeIds(List<UUID> nodeIds) {
        this.nodeIds = nodeIds;
    }

    /**
     * @return List of partitions (index) and nodes (items) having an appropriate partition in
     * {@link GridDhtPartitionState#OWNING} state, calculated for distributed tables, involved in query execution.
     */
    public List<List<UUID>> assignments() {
        if (assignments != null)
            return assignments;

        if (!F.isEmpty(nodeIds))
            return nodeIds.stream().map(Collections::singletonList).collect(Collectors.toList());

        return Collections.emptyList();
    }

    /** */
    public @Nullable List<UUIDCollectionMessage> assigmentsMessage() {
        return assigmentsMsg;
    }

    /** */
    public void assigmentsMessage(@Nullable List<UUIDCollectionMessage> assigmentsMsg) {
        if (assigmentsMsg == null)
            return;

        assignments = new ArrayList<>(assigmentsMsg.size());

        assigmentsMsg.forEach(am -> assignments.add(am.uuids() instanceof List ? (List<UUID>)am.uuids()
            : new ArrayList<>(am.uuids())));
    }

    /** */
    public long[] sourceIds() {
        return sourceIds;
    }

    /** */
    public void sourceIds(long[] srcIds) {
        this.sourceIds = srcIds;
    }

    /** */
    public boolean belongs(long sourceId) {
        if (sourceIds == null)
            return false;

        for (long i : sourceIds) {
            if (i == sourceId)
                return true;
        }

        return false;
    }

    /**
     * Merges this mapping with given one.
     * @param other Mapping to merge with.
     * @return Merged nodes mapping.
     * @throws ColocationMappingException If involved nodes intersection is empty, hence there is no nodes capable to execute
     * being calculated fragment.
     */
    public ColocationGroup colocate(ColocationGroup other) throws ColocationMappingException {
        long[] srcIds;
        if (sourceIds == null || other.sourceIds == null)
            srcIds = U.firstNotNull(sourceIds, other.sourceIds);
        else
            srcIds = LongStream.concat(Arrays.stream(sourceIds), Arrays.stream(other.sourceIds)).distinct().toArray();

        List<UUID> nodeIds;
        if (this.nodeIds == null || other.nodeIds == null)
            nodeIds = U.firstNotNull(this.nodeIds, other.nodeIds);
        else
            nodeIds = Commons.intersect(other.nodeIds, this.nodeIds);

        if (nodeIds != null && nodeIds.isEmpty()) {
            throw new ColocationMappingException("Failed to map fragment to location. " +
                "Replicated query parts are not co-located on all nodes");
        }

        boolean primaryAssignment = this.primaryAssignment || other.primaryAssignment;

        List<List<UUID>> assignments;
        if (this.assignments == null || other.assignments == null) {
            assignments = U.firstNotNull(this.assignments, other.assignments);

            if (assignments != null && nodeIds != null) {
                Set<UUID> filter = new HashSet<>(nodeIds);
                List<List<UUID>> assignments0 = new ArrayList<>(assignments.size());

                for (int i = 0; i < assignments.size(); i++) {
                    List<UUID> assignment = Commons.intersect(filter, assignments.get(i));

                    if (assignment.isEmpty()) {
                        throw new ColocationMappingException("Failed to map fragment to location. " +
                            "Partition mapping is empty [part=" + i + "]");
                    }

                    if (!assignment.get(0).equals(assignments.get(i).get(0)))
                        primaryAssignment = false;

                    assignments0.add(assignment);
                }

                assignments = assignments0;
            }
        }
        else {
            assert this.assignments.size() == other.assignments.size();
            assignments = new ArrayList<>(this.assignments.size());
            Set<UUID> filter = nodeIds == null ? null : new HashSet<>(nodeIds);
            for (int i = 0; i < this.assignments.size(); i++) {
                List<UUID> assignment = Commons.intersect(this.assignments.get(i), other.assignments.get(i));

                if (filter != null)
                    assignment.retainAll(filter);

                if (assignment.isEmpty()) {
                    throw new ColocationMappingException("Failed to map fragment to location. " +
                        "Partition mapping is empty [part=" + i + "]");
                }

                if (!assignment.get(0).equals(this.assignments.get(i).get(0))
                    || !assignment.get(0).equals(other.assignments.get(i).get(0)))
                    primaryAssignment = false;

                assignments.add(assignment);
            }
        }

        return new ColocationGroup(srcIds, nodeIds, assignments, primaryAssignment);
    }

    /** */
    public ColocationGroup finalizeMapping() {
        if (assignments == null)
            return this;

        List<List<UUID>> assignments = new ArrayList<>(this.assignments.size());
        Set<UUID> nodes = new HashSet<>();

        for (List<UUID> assignment : this.assignments) {
            UUID first = F.first(assignment);
            if (first != null)
                nodes.add(first);
            assignments.add(first != null ? Collections.singletonList(first) : Collections.emptyList());
        }

        return new ColocationGroup(sourceIds, new ArrayList<>(nodes), assignments, primaryAssignment);
    }

    /** */
    public ColocationGroup explicitMapping() {
        if (assignments == null || !primaryAssignment)
            return this;

        // Make a shallow copy without cacheAssignment flag.
        return new ColocationGroup(sourceIds, nodeIds, assignments, false);
    }

    /** */
    public ColocationGroup filterByPartitions(int[] parts) {
        if (!F.isEmpty(assignments)) {
            List<List<UUID>> assignments = new ArrayList<>(this.assignments.size());
            Set<UUID> nodes = new HashSet<>();

            if (parts == null)
                return this;

            if (parts.length > 0) {
                for (int i = 0; i < this.assignments.size(); ++i) {
                    UUID first = Arrays.binarySearch(parts, i) >= 0 ? F.first(this.assignments.get(i)) : null;

                    if (first != null)
                        nodes.add(first);

                    assignments.add(first != null ? this.assignments.get(i) : Collections.emptyList());
                }
            }
            else {
                for (int i = 0; i < this.assignments.size(); ++i)
                    assignments.add(Collections.emptyList());
            }

            return new ColocationGroup(sourceIds, new ArrayList<>(nodes), assignments);
        }

        return this;
    }

    /** */
    public ColocationGroup mapToNodes(List<UUID> nodeIds) {
        return !F.isEmpty(this.nodeIds) ? this : new ColocationGroup(sourceIds, nodeIds, null);
    }

    /**
     * Returns List of partitions to scan on the given node.
     *
     * @param nodeId Cluster node ID.
     * @return List of partitions to scan on the given node.
     */
    public int[] partitions(UUID nodeId) {
        if (F.isEmpty(assignments))
            return null;

        GridIntList parts = new GridIntList(assignments.size());

        for (int i = 0; i < assignments.size(); i++) {
            List<UUID> assignment = assignments.get(i);
            if (Objects.equals(nodeId, F.first(assignment)))
                parts.add(i);
        }

        return parts.arrayCopy();
    }

    /** {@inheritDoc} */
    @Override public MessageType type() {
        return MessageType.COLOCATION_GROUP;
    }
}
