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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import org.apache.ignite.internal.GridDirectCollection;
import org.apache.ignite.internal.GridDirectTransient;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState;
import org.apache.ignite.internal.processors.query.calcite.message.MarshalableMessage;
import org.apache.ignite.internal.processors.query.calcite.message.MessageType;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.util.GridIntIterator;
import org.apache.ignite.internal.util.GridIntList;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/** */
public class ColocationGroup implements MarshalableMessage {
    /** */
    private long[] sourceIds;

    /** */
    @GridDirectCollection(UUID.class)
    private List<UUID> nodeIds;

    /** */
    @GridDirectTransient
    private List<List<UUID>> assignments;

    /**
     * Flag, indacating that assignment is formed by original cache assignment for given topology.
     * In case of {@code true} value we can skip assignment marshalling and calc assignment on remote nodes.
     */
    @GridDirectTransient
    private boolean primaryAssignment;

    /** Marshalled assignments. */
    private int[] marshalledAssignments;

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

        return new ColocationGroup(Arrays.copyOf(sourceIds, sourceIds.length), Collections.singletonList(nodeId),
                locAssignments);
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
        if (assignments != null)
            return assignments;

        if (!F.isEmpty(nodeIds))
            return nodeIds.stream().map(Collections::singletonList).collect(Collectors.toList());

        return Collections.emptyList();
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
        System.err.println("TEST | finalizeMapping, assign. cnt: " + (assignments==null ? 0 : assignments.size()));

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

        System.err.println("TEST | finalizeMapping, assign. cnt: " + (assignments==null ? 0 : assignments.size()));

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
                if (!writer.writeIntArray("marshalledAssignments", marshalledAssignments))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeCollection("nodeIds", nodeIds, MessageCollectionItemType.UUID))
                    return false;

                writer.incrementState();

            case 2:
                if (!writer.writeLongArray("sourceIds", sourceIds))
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
                marshalledAssignments = reader.readIntArray("marshalledAssignments");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                nodeIds = reader.readCollection("nodeIds", MessageCollectionItemType.UUID);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 2:
                sourceIds = reader.readLongArray("sourceIds");

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
    @Override public void prepareMarshal(GridCacheSharedContext<?, ?> ctx) {
        if (assignments != null && marshalledAssignments == null && !primaryAssignment) {
            Map<UUID, Integer> nodeIdxs = new HashMap<>();

            for (int i = 0; i < nodeIds.size(); i++)
                nodeIdxs.put(nodeIds.get(i), i);

            int bitsPerPart = Integer.SIZE - Integer.numberOfLeadingZeros(nodeIds.size());

            CompactedIntArray.Builder builder = CompactedIntArray.builder(bitsPerPart, assignments.size());

            for (List<UUID> assignment : assignments) {
                assert F.isEmpty(assignment) || assignment.size() == 1;

                if (F.isEmpty(assignment))
                    builder.add(nodeIds.size());
                else {
                    Integer nodeIdx = nodeIdxs.get(assignment.get(0));

                    builder.add(nodeIdx);
                }
            }

            marshalledAssignments = builder.build().buffer();
        }
    }

    /** {@inheritDoc} */
    @Override public void prepareUnmarshal(GridCacheSharedContext<?, ?> ctx) {
        if (marshalledAssignments != null && assignments == null) {
            int bitsPerPart = Integer.SIZE - Integer.numberOfLeadingZeros(nodeIds.size());

            CompactedIntArray compactedArr = CompactedIntArray.of(bitsPerPart, marshalledAssignments);

            assignments = new ArrayList<>(compactedArr.size());

            for (GridIntIterator iter = compactedArr.iterator(); iter.hasNext(); ) {
                int nodeIdx = iter.next();

                assignments.add(nodeIdx >= nodeIds.size() ? Collections.emptyList() :
                    Collections.singletonList(nodeIds.get(nodeIdx)));
            }
        }
    }

    /** */
    private static class CompactedIntArray {
        /** */
        protected static final int BUF_POS_MASK = Integer.SIZE - 1;

        /** */
        protected static final int BUF_POS_LOG2 = Integer.SIZE - Integer.numberOfLeadingZeros(Integer.SIZE - 1);

        /** */
        protected static final int[] BIT_MASKS = new int[Integer.SIZE];

        static {
            for (int i = 0; i < Integer.SIZE; i++)
                BIT_MASKS[i] = ~(-1 << i);
        }

        /** Buffer. */
        private final int[] buf;

        /** Bits count per each item. */
        private final int bitsPerItem;

        /** Ctor. */
        private CompactedIntArray(int bitsPerItem, int[] buf) {
            this.bitsPerItem = bitsPerItem;
            this.buf = buf;
        }

        /** */
        public int[] buffer() {
            return buf;
        }

        /** */
        public int size() {
            return buf[0];
        }

        /** */
        public GridIntIterator iterator() {
            return new Iterator();
        }

        /** */
        public static CompactedIntArray of(int bitsPerItem, int[] buf) {
            return new CompactedIntArray(bitsPerItem, buf);
        }

        /** */
        public static Builder builder(int bitsPerItem, int capacity) {
            return new Builder(bitsPerItem, capacity);
        }

        /** */
        private static class Builder {
            /** Current bit position. */
            private int bitPos = Integer.SIZE; // Skip first element.

            /** Current size. */
            private int size;

            /** Buffer. */
            protected final int[] buf;

            /** Bits count per each item. */
            protected final int bitsPerItem;

            /** Ctor. */
            public Builder(int bitsPerItem, int capacity) {
                this.bitsPerItem = bitsPerItem;
                buf = new int[(capacity * bitsPerItem + Integer.SIZE - 1) / Integer.SIZE + 1];
                buf[0] = capacity;
            }

            /** Add the next item. */
            public void add(int val) {
                assert size < buf[0];

                int bitsToWrite = bitsPerItem;
                int bitPos = this.bitPos;

                do {
                    int bitsToWriteCurBuf = Math.min(bitsToWrite, Integer.SIZE - (bitPos & BUF_POS_MASK));

                    int writeVal = (val & BIT_MASKS[bitsToWriteCurBuf]) << (bitPos & BUF_POS_MASK);

                    val >>= bitsToWriteCurBuf;

                    buf[bitPos >> BUF_POS_LOG2] |= writeVal;

                    bitPos += bitsToWriteCurBuf;

                    bitsToWrite -= bitsToWriteCurBuf;
                }
                while (bitsToWrite > 0);

                this.bitPos = bitPos;

                size++;
            }

            /** */
            public CompactedIntArray build() {
                buf[0] = size;

                return new CompactedIntArray(bitsPerItem, buf);
            }
        }

        /** */
        private class Iterator implements GridIntIterator {
            /** Current bit position. */
            private int bitPos = Integer.SIZE; // Skip first element.

            /** Current item position. */
            private int pos;

            /** Array size. */
            private final int size = buf[0];

            /** {@inheritDoc} */
            @Override public boolean hasNext() {
                return pos < size;
            }

            /** {@inheritDoc} */
            @Override public int next() {
                assert pos < size;

                int bitPos = this.bitPos;

                int bitsFirstBuf = Math.min(bitsPerItem, Integer.SIZE - (bitPos & BUF_POS_MASK));

                int val = ((buf[bitPos >> BUF_POS_LOG2] >> (bitPos & BUF_POS_MASK)) & BIT_MASKS[bitsFirstBuf]);

                bitPos += bitsFirstBuf;

                if (bitsFirstBuf < bitsPerItem) {
                    int bitsSecondBuf = bitsPerItem - bitsFirstBuf;

                    val |= ((buf[bitPos >> BUF_POS_LOG2] >> (bitPos & BUF_POS_MASK)) & BIT_MASKS[bitsSecondBuf])
                        << bitsFirstBuf;

                    bitPos += bitsSecondBuf;
                }

                this.bitPos = bitPos;

                pos++;

                return val;
            }
        }
    }
}
