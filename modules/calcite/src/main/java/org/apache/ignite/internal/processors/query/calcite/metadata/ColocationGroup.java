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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRel;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.util.IgniteIntList;
import org.jetbrains.annotations.NotNull;

import static org.apache.ignite.internal.util.ArrayUtils.asList;
import static org.apache.ignite.internal.util.CollectionUtils.first;
import static org.apache.ignite.internal.util.CollectionUtils.nullOrEmpty;
import static org.apache.ignite.internal.util.IgniteUtils.firstNotNull;

/** */
public class ColocationGroup implements Serializable {
    /** */
    private static final int SYNTHETIC_PARTITIONS_COUNT = 512;
        // TODO: IgniteSystemProperties.getInteger("IGNITE_CALCITE_SYNTHETIC_PARTITIONS_COUNT", 512);

    /** */
    private List<Long> sourceIds;

    /** */
    private List<String> nodeIds;

    /** */
    private List<List<String>> assignments;

    /** */
    public static ColocationGroup forNodes(List<String> nodeIds) {
        return new ColocationGroup(null, nodeIds, null);
    }

    /** */
    public static ColocationGroup forAssignments(List<List<String>> assignments) {
        return new ColocationGroup(null, null, assignments);
    }

    /** */
    public static ColocationGroup forSourceId(long sourceId) {
        return new ColocationGroup(Collections.singletonList(sourceId), null, null);
    }

    /** */
    private ColocationGroup(List<Long> sourceIds, List<String> nodeIds, List<List<String>> assignments) {
        this.sourceIds = sourceIds;
        this.nodeIds = nodeIds;
        this.assignments = assignments;
    }

    /**
     * @return Lists of colocation group sources.
     */
    public List<Long> sourceIds() {
        return sourceIds == null ? Collections.emptyList() : sourceIds;
    }

    /**
     * @return Lists of nodes capable to execute a query fragment for what the mapping is calculated.
     */
    public List<String> nodeIds() {
        return nodeIds == null ? Collections.emptyList() : nodeIds;
    }

    /**
     * @return List of partitions (index) and nodes (items) having an appropriate partition in
     * OWNING state, calculated for distributed tables, involved in query execution.
     */
    public List<List<String>> assignments() {
        return assignments == null ? Collections.emptyList() : assignments;
    }

    /**
     * Prunes involved partitions (hence nodes, involved in query execution) on the basis of filter,
     * its distribution, query parameters and original nodes mapping.
     * @param rel Filter.
     * @return Resulting nodes mapping.
     */
    public ColocationGroup prune(IgniteRel rel) {
        return this; // TODO https://issues.apache.org/jira/browse/IGNITE-12455
    }

    /** */
    public boolean belongs(long sourceId) {
        return sourceIds != null && sourceIds.contains(sourceId);
    }

    /**
     * Merges this mapping with given one.
     * @param other Mapping to merge with.
     * @return Merged nodes mapping.
     * @throws ColocationMappingException If involved nodes intersection is empty, hence there is no nodes capable to execute
     * being calculated fragment.
     */
    public ColocationGroup colocate(ColocationGroup other) throws ColocationMappingException {
        List<Long> sourceIds;
        if (this.sourceIds == null || other.sourceIds == null)
            sourceIds = firstNotNull(this.sourceIds, other.sourceIds);
        else
            sourceIds = Commons.combine(this.sourceIds, other.sourceIds);

        List<String> nodeIds;
        if (this.nodeIds == null || other.nodeIds == null)
            nodeIds = firstNotNull(this.nodeIds, other.nodeIds);
        else
            nodeIds = Commons.intersect(other.nodeIds, this.nodeIds);

        if (nodeIds != null && nodeIds.isEmpty()) {
            throw new ColocationMappingException("Failed to map fragment to location. " +
                "Replicated query parts are not co-located on all nodes");
        }

        List<List<String>> assignments;
        if (this.assignments == null || other.assignments == null) {
            assignments = firstNotNull(this.assignments, other.assignments);

            if (assignments != null && nodeIds != null) {
                Set<String> filter = new HashSet<>(nodeIds);
                List<List<String>> assignments0 = new ArrayList<>(assignments.size());

                for (int i = 0; i < assignments.size(); i++) {
                    List<String> assignment = Commons.intersect(filter, assignments.get(i));

                    if (assignment.isEmpty()) { // TODO check with partition filters
                        throw new ColocationMappingException("Failed to map fragment to location. " +
                            "Partition mapping is empty [part=" + i + "]");
                    }

                    assignments0.add(assignment);
                }

                assignments = assignments0;
            }
        }
        else {
            assert this.assignments.size() == other.assignments.size();
            assignments = new ArrayList<>(this.assignments.size());
            Set<String> filter = nodeIds == null ? null : new HashSet<>(nodeIds);
            for (int i = 0; i < this.assignments.size(); i++) {
                List<String> assignment = Commons.intersect(this.assignments.get(i), other.assignments.get(i));

                if (filter != null)
                    assignment.retainAll(filter);

                if (assignment.isEmpty()) // TODO check with partition filters
                    throw new ColocationMappingException("Failed to map fragment to location. Partition mapping is empty [part=" + i + "]");

                assignments.add(assignment);
            }
        }

        return new ColocationGroup(sourceIds, nodeIds, assignments);
    }

    /** */
    public ColocationGroup finalaze() {
        if (assignments == null && nodeIds == null)
            return this;

        if (assignments != null) {
            List<List<String>> assignments = new ArrayList<>(this.assignments.size());
            Set<String> nodes = new HashSet<>();
            for (List<String> assignment : this.assignments) {
                String first = first(assignment);
                if (first != null)
                    nodes.add(first);
                assignments.add(first != null ? Collections.singletonList(first) : Collections.emptyList());
            }

            return new ColocationGroup(sourceIds, new ArrayList<>(nodes), assignments);
        }

        return forNodes0(nodeIds);
    }

    /** */
    public ColocationGroup mapToNodes(List<String> nodeIds) {
        return !nullOrEmpty(this.nodeIds) ? this : forNodes0(nodeIds);
    }

    /** */
    @NotNull private ColocationGroup forNodes0(List<String> nodeIds) {
        List<List<String>> assignments = new ArrayList<>(SYNTHETIC_PARTITIONS_COUNT);
        for (int i = 0; i < SYNTHETIC_PARTITIONS_COUNT; i++)
            assignments.add(asList(nodeIds.get(i % nodeIds.size())));
        return new ColocationGroup(sourceIds, nodeIds, assignments);
    }

    /**
     * Returns List of partitions to scan on the given node.
     *
     * @param nodeId Cluster node ID.
     * @return List of partitions to scan on the given node.
     */
    public int[] partitions(String nodeId) {
        IgniteIntList parts = new IgniteIntList(assignments.size());

        for (int i = 0; i < assignments.size(); i++) {
            List<String> assignment = assignments.get(i);
            if (Objects.equals(nodeId, first(assignment)))
                parts.add(i);
        }

        return parts.array();
    }
}
