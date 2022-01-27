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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.internal.GridDirectCollection;
import org.apache.ignite.internal.GridDirectTransient;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState;
import org.apache.ignite.internal.processors.query.calcite.message.MarshalableMessage;
import org.apache.ignite.internal.processors.query.calcite.message.MarshallingContext;
import org.apache.ignite.internal.processors.query.calcite.message.MessageType;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRel;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.util.GridIntList;
import org.apache.ignite.internal.util.UUIDCollectionMessage;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.jetbrains.annotations.NotNull;

/** */
public class ColocationGroup implements MarshalableMessage {
    /** */
    private static final int SYNTHETIC_PARTITIONS_COUNT =
        IgniteSystemProperties.getInteger("IGNITE_CALCITE_SYNTHETIC_PARTITIONS_COUNT", 512);

    /** */
    @GridDirectCollection(Long.class)
    private List<Long> sourceIds;

    /** */
    @GridDirectCollection(UUID.class)
    private List<UUID> nodeIds;

    /** */
    @GridDirectTransient
    private List<List<UUID>> assignments;

    /** */
    @GridDirectCollection(Message.class)
    private List<UUIDCollectionMessage> assignments0;

    /** */
    public static ColocationGroup forNodes(List<UUID> nodeIds) {
        return new ColocationGroup(null, nodeIds, null);
    }

    /** */
    public static ColocationGroup forAssignments(List<List<UUID>> assignments) {
        return new ColocationGroup(null, null, assignments);
    }

    /** */
    public static ColocationGroup forSourceId(long sourceId) {
        return new ColocationGroup(Collections.singletonList(sourceId), null, null);
    }

    /** */
    public ColocationGroup() {
    }

    /** */
    private ColocationGroup(List<Long> sourceIds, List<UUID> nodeIds, List<List<UUID>> assignments) {
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
    public List<UUID> nodeIds() {
        return nodeIds == null ? Collections.emptyList() : nodeIds;
    }

    /**
     * @return List of partitions (index) and nodes (items) having an appropriate partition in
     * {@link GridDhtPartitionState#OWNING} state, calculated for distributed tables, involved in query execution.
     */
    public List<List<UUID>> assignments() {
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
            sourceIds = U.firstNotNull(this.sourceIds, other.sourceIds);
        else
            sourceIds = Commons.combine(this.sourceIds, other.sourceIds);

        List<UUID> nodeIds;
        if (this.nodeIds == null || other.nodeIds == null)
            nodeIds = U.firstNotNull(this.nodeIds, other.nodeIds);
        else
            nodeIds = Commons.intersect(other.nodeIds, this.nodeIds);

        if (nodeIds != null && nodeIds.isEmpty()) {
            throw new ColocationMappingException("Failed to map fragment to location. " +
                "Replicated query parts are not co-located on all nodes");
        }

        List<List<UUID>> assignments;
        if (this.assignments == null || other.assignments == null) {
            assignments = U.firstNotNull(this.assignments, other.assignments);

            if (assignments != null && nodeIds != null) {
                Set<UUID> filter = new HashSet<>(nodeIds);
                List<List<UUID>> assignments0 = new ArrayList<>(assignments.size());

                for (int i = 0; i < assignments.size(); i++) {
                    List<UUID> assignment = Commons.intersect(filter, assignments.get(i));

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
            Set<UUID> filter = nodeIds == null ? null : new HashSet<>(nodeIds);
            for (int i = 0; i < this.assignments.size(); i++) {
                List<UUID> assignment = Commons.intersect(this.assignments.get(i), other.assignments.get(i));

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
            List<List<UUID>> assignments = new ArrayList<>(this.assignments.size());
            Set<UUID> nodes = new HashSet<>();
            for (List<UUID> assignment : this.assignments) {
                UUID first = F.first(assignment);
                if (first != null)
                    nodes.add(first);
                assignments.add(first != null ? Collections.singletonList(first) : Collections.emptyList());
            }

            return new ColocationGroup(sourceIds, new ArrayList<>(nodes), assignments);
        }

        return forNodes0(nodeIds);
    }

    /** */
    public ColocationGroup mapToNodes(List<UUID> nodeIds) {
        return !F.isEmpty(this.nodeIds) ? this : forNodes0(nodeIds);
    }

    /** */
    @NotNull private ColocationGroup forNodes0(List<UUID> nodeIds) {
        List<List<UUID>> assignments = new ArrayList<>(SYNTHETIC_PARTITIONS_COUNT);
        for (int i = 0; i < SYNTHETIC_PARTITIONS_COUNT; i++)
            assignments.add(F.asList(nodeIds.get(i % nodeIds.size())));
        return new ColocationGroup(sourceIds, nodeIds, assignments);
    }

    /**
     * Returns List of partitions to scan on the given node.
     *
     * @param nodeId Cluster node ID.
     * @return List of partitions to scan on the given node.
     */
    public int[] partitions(UUID nodeId) {
        GridIntList parts = new GridIntList(assignments.size());

        for (int i = 0; i < assignments.size(); i++) {
            List<UUID> assignment = assignments.get(i);
            if (Objects.equals(nodeId, F.first(assignment)))
                parts.add(i);
        }

        return parts.array();
    }

    /** {@inheritDoc} */
    @Override public MessageType type() {
        return MessageType.COLOCATION_GROUP;
    }

    /** {@inheritDoc} */
    @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        writer.setBuffer(buf);

        if (!writer.isHeaderWritten()) {
            if (!writer.writeHeader(directType(), fieldsCount()))
                return false;

            writer.onHeaderWritten();
        }

        switch (writer.state()) {
            case 0:
                if (!writer.writeCollection("assignments0", assignments0, MessageCollectionItemType.MSG))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeCollection("nodeIds", nodeIds, MessageCollectionItemType.UUID))
                    return false;

                writer.incrementState();

            case 2:
                if (!writer.writeCollection("sourceIds", sourceIds, MessageCollectionItemType.LONG))
                    return false;

                writer.incrementState();

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean readFrom(ByteBuffer buf, MessageReader reader) {
        reader.setBuffer(buf);

        if (!reader.beforeMessageRead())
            return false;

        switch (reader.state()) {
            case 0:
                assignments0 = reader.readCollection("assignments0", MessageCollectionItemType.MSG);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                nodeIds = reader.readCollection("nodeIds", MessageCollectionItemType.UUID);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 2:
                sourceIds = reader.readCollection("sourceIds", MessageCollectionItemType.LONG);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(ColocationGroup.class);
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 3;
    }

    /** {@inheritDoc} */
    @Override public void prepareMarshal(MarshallingContext ctx) {
        if (assignments != null && assignments0 == null)
            assignments0 = Commons.transform(assignments, this::transform);
    }

    /** {@inheritDoc} */
    @Override public void prepareUnmarshal(MarshallingContext ctx) {
        if (assignments0 != null && assignments == null)
            assignments = Commons.transform(assignments0, this::transform);
    }

    /** */
    private List<UUID> transform(UUIDCollectionMessage message) {
        return message.uuids() instanceof List ? (List<UUID>)message.uuids() : new ArrayList<>(message.uuids());
    }

    /** */
    private UUIDCollectionMessage transform(List<UUID> uuids) {
        return new UUIDCollectionMessage(uuids);
    }
}
