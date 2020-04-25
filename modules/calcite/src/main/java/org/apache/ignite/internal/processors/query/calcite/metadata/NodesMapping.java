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
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.GridDirectCollection;
import org.apache.ignite.internal.GridDirectTransient;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState;
import org.apache.ignite.internal.processors.query.calcite.message.MarshalableMessage;
import org.apache.ignite.internal.processors.query.calcite.message.MessageType;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteFilter;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.util.GridIntList;
import org.apache.ignite.internal.util.UUIDCollectionMessage;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/**
 * Represents a list of nodes capable to execute a query over particular partitions.
 */
public class NodesMapping implements MarshalableMessage {
    /** */
    public static final byte HAS_MOVING_PARTITIONS  = 1;

    /** */
    public static final byte HAS_REPLICATED_CACHES  = 1 << 1;

    /** */
    public static final byte HAS_PARTITIONED_CACHES = 1 << 2;

    /** */
    public static final byte PARTIALLY_REPLICATED   = 1 << 3;

    /** */
    public static final byte DEDUPLICATED           = 1 << 4;

    /** */
    public static final byte CLIENT                 = 1 << 5;

    /** */
    @GridDirectCollection(UUID.class)
    private List<UUID> nodes;

    /** */
    @GridDirectTransient
    private List<List<UUID>> assignments;

    /** */
    @GridDirectCollection(Message.class)
    private List<UUIDCollectionMessage> assignments0;

    /** */
    private byte flags;

    /** */
    public NodesMapping() {
    }

    /** */
    public NodesMapping(List<UUID> nodes, List<List<UUID>> assignments, byte flags) {
        this.nodes = nodes;
        this.assignments = assignments;
        this.flags = flags;
    }

    /**
     * @return Lists of nodes capable to execute a query fragment for what the mapping is calculated.
     */
    public List<UUID> nodes() {
        return nodes;
    }

    /**
     * @return List of partitions (index) and nodes (items) having an appropriate partition in
     * {@link GridDhtPartitionState#OWNING} state, calculated for distributed tables, involved in query execution.
     */
    public List<List<UUID>> assignments() {
        return assignments;
    }

    /**
     * Prunes involved partitions (hence nodes, involved in query execution) on the basis of filter,
     * its distribution, query parameters and original nodes mapping.
     * @param filter Filter.
     * @return Resulting nodes mapping.
     */
    public NodesMapping prune(IgniteFilter filter) {
        return this; // TODO https://issues.apache.org/jira/browse/IGNITE-12455
    }

    /**
     * Merges this mapping with given one.
     * @param other Mapping to merge with.
     * @return Merged nodes mapping.
     * @throws LocationMappingException If involved nodes intersection is empty, hence there is no nodes capable to execute
     * being calculated fragment.
     */
    public NodesMapping mergeWith(NodesMapping other) throws LocationMappingException {
        byte flags = (byte) (this.flags | other.flags);

        // reset posiible set deduplicated flag
        flags &= ~DEDUPLICATED;

        List<UUID> nodes = intersectReplicated(this.nodes, other.nodes);

        // if there is no moving partitions both assignments are identical.
        List<List<UUID>> assignments = (flags & HAS_MOVING_PARTITIONS) == 0
            ? U.firstNotNull(this.assignments, other.assignments)
            : intersectPartitioned(this.assignments, other.assignments);

        // In case all involved replicated caches are available on
        // all nodes it's no need to check assignments against them.
        if ((flags & (PARTIALLY_REPLICATED | CLIENT)) == 0)
            return new NodesMapping(nodes, assignments, flags);

        return new NodesMapping(nodes, intersectReplicatedPartitioned(nodes, assignments), flags);
    }

    /**
     * At the calculation time the mapping is excessive, it means it consists of all possible nodes,
     * able to execute a calculated fragment. After calculation we need to choose who will actually execute
     * the query (for example we don't want to scan Replicated table on all nodes, we do the scan on one
     * of them instead).
     *
     * @return Nodes mapping, containing nodes, that actually will be in charge of query execution.
     */
    public NodesMapping deduplicate() {
        if ((flags & DEDUPLICATED) == DEDUPLICATED)
            return this;

        if (assignments == null) {
            List<List<UUID>> assignments0 = new ArrayList<>(nodes.size());

            for (UUID node : nodes)
                assignments0.add(F.asList(node));

            return new NodesMapping(nodes, assignments0, (byte)(flags | DEDUPLICATED));
        }

        HashSet<UUID> nodesSet = new HashSet<>();
        ArrayList<UUID> nodes0 = new ArrayList<>();
        List<List<UUID>> assignments0 = new ArrayList<>(assignments.size());

        for (List<UUID> partNodes : assignments) {
            UUID node = F.first(partNodes);

            assignments0.add(F.asList(node));

            if (node != null && nodesSet.add(node))
                nodes0.add(node);
        }

        return new NodesMapping(nodes0, assignments0, (byte)(flags | DEDUPLICATED));
    }

    /**
     * Returns List of partitions to scan on the given node.
     *
     * @param node Cluster node ID.
     * @return List of partitions to scan on the given node.
     */
    public int[] partitions(UUID node) {
        assert (flags & DEDUPLICATED) == DEDUPLICATED;

        GridIntList parts = new GridIntList(assignments.size());

        for (int i = 0; i < assignments.size(); i++) {
            List<UUID> assignment = assignments.get(i);
            if (Objects.equals(node, F.first(assignment)))
                parts.add(i);
        }

        return parts.array();
    }

    /**
     * @return {@code True} if some of involved partitioned tables are being rebalanced.
     */
    public boolean hasMovingPartitions() {
        return (flags & HAS_MOVING_PARTITIONS) == HAS_MOVING_PARTITIONS;
    }

    /**
     * @return {@code True} if at least one of involved tables is replicated.
     */
    public boolean hasReplicatedCaches() {
        return (flags & HAS_REPLICATED_CACHES) == HAS_REPLICATED_CACHES;
    }

    /**
     * @return {@code True} if at least one of involved tables is partitioned.
     */
    public boolean hasPartitionedCaches() {
        return (flags & HAS_PARTITIONED_CACHES) == HAS_PARTITIONED_CACHES;
    }

    /**
     * @return {@code True} if one of involved replicated tables have a node filter
     *
     * See {@link CacheConfiguration#getNodeFilter()} for more information.
     */
    public boolean partiallyReplicated() {
        return (flags & PARTIALLY_REPLICATED) == PARTIALLY_REPLICATED;
    }

    /** */
    private List<UUID> intersectReplicated(List<UUID> left, List<UUID> right) throws LocationMappingException {
        if (left == null || right == null)
            return U.firstNotNull(left, right);

        List<UUID> nodes = Commons.intersect(right, left);

        if (F.isEmpty(nodes)) // replicated caches aren't co-located on all nodes.
            throw new LocationMappingException("Failed to map fragment to location. Replicated query parts are not co-located on all nodes");

        return nodes;
    }

    /** */
    private List<List<UUID>> intersectPartitioned(List<List<UUID>> left, List<List<UUID>> right) throws LocationMappingException {
        if (left == null || right == null)
            return U.firstNotNull(left, right);

        assert left.size() == right.size();

        List<List<UUID>> res = new ArrayList<>(left.size());

        for (int i = 0; i < left.size(); i++) {
            List<UUID> partNodes = Commons.intersect(left.get(i), right.get(i));

            if (partNodes.isEmpty()) // TODO check with partition filters
                throw new LocationMappingException("Failed to map fragment to location. Partition mapping is empty [part=" + i + "]");

            res.add(partNodes);
        }

        return res;
    }

    /** */
    private List<List<UUID>> intersectReplicatedPartitioned(List<UUID> nodes, List<List<UUID>> assignments) throws LocationMappingException {
        if (nodes == null || assignments == null)
            return assignments;

        HashSet<UUID> nodesSet = new HashSet<>(nodes);
        List<List<UUID>> res = new ArrayList<>(assignments.size());

        for (int i = 0; i < assignments.size(); i++) {
            List<UUID> partNodes = Commons.intersect(nodesSet, assignments.get(i));

            if (partNodes.isEmpty()) // TODO check with partition filters
                throw new LocationMappingException("Failed to map fragment to location. Partition mapping is empty [part=" + i + "]");

            res.add(partNodes);
        }

        return res;
    }

    /** {@inheritDoc} */
    @Override public MessageType type() {
        return MessageType.NODES_MAPPING;
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
                if (!writer.writeByte("flags", flags))
                    return false;

                writer.incrementState();

            case 2:
                if (!writer.writeCollection("nodes", nodes, MessageCollectionItemType.UUID))
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
                flags = reader.readByte("flags");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 2:
                nodes = reader.readCollection("nodes", MessageCollectionItemType.UUID);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(NodesMapping.class);
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 3;
    }

    /** {@inheritDoc} */
    @Override public void prepareMarshal(Marshaller marshaller) {
        if (assignments != null && assignments0 == null)
            assignments0 = Commons.transform(assignments, this::transform);
    }

    /** {@inheritDoc} */
    @Override public void prepareUnmarshal(Marshaller marshaller, ClassLoader loader) {
        if (assignments0 != null && assignments == null)
            assignments = Commons.transform(assignments0, this::transform);
    }

    /** {@inheritDoc} */
    private List<UUID> transform(UUIDCollectionMessage message) {
        return message.uuids() instanceof List ? (List<UUID>)message.uuids() : new ArrayList<>(message.uuids());
    }

    /** {@inheritDoc} */
    private UUIDCollectionMessage transform(List<UUID> uuids) {
        return new UUIDCollectionMessage(uuids);
    }
}
