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

package org.apache.ignite.internal.processors.cache.distributed.dht.preloader;

import java.io.Externalizable;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridDirectCollection;
import org.apache.ignite.internal.GridDirectMap;
import org.apache.ignite.internal.GridDirectTransient;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheGroupIdMessage;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.jetbrains.annotations.NotNull;

/**
 * Partition demand request.
 */
public class GridDhtPartitionDemandMessage extends GridCacheGroupIdMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** Update sequence. */
    private long updateSeq;

    /** Partition. */
    @GridDirectCollection(int.class)
    private Collection<Integer> parts;

    /** Partition. */
    @GridDirectMap(keyType = int.class, valueType = long.class)
    private Map<Integer, Long> partsCntrs;

    /** Topic. */
    @GridDirectTransient
    private Object topic;

    /** Serialized topic. */
    private byte[] topicBytes;

    /** Timeout. */
    private long timeout;

    /** Worker ID. */
    private int workerId = -1;

    /** Topology version. */
    private AffinityTopologyVersion topVer;

    /**
     * @param updateSeq Update sequence for this node.
     * @param topVer Topology version.
     * @param grpId Cache group ID.
     */
    GridDhtPartitionDemandMessage(long updateSeq, @NotNull AffinityTopologyVersion topVer, int grpId) {
        this.grpId = grpId;
        this.updateSeq = updateSeq;
        this.topVer = topVer;
    }

    /**
     * @param cp Message to copy from.
     * @param parts Partitions.
     */
    GridDhtPartitionDemandMessage(GridDhtPartitionDemandMessage cp, Collection<Integer> parts, Map<Integer, Long> partsCntrs) {
        grpId = cp.grpId;
        updateSeq = cp.updateSeq;
        topic = cp.topic;
        timeout = cp.timeout;
        workerId = cp.workerId;
        topVer = cp.topVer;

        // Create a copy of passed in collection since it can be modified when this message is being sent.
        this.parts = new HashSet<>(parts);
        this.partsCntrs = partsCntrs;
    }

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    public GridDhtPartitionDemandMessage() {
        // No-op.
    }

    /**
     * @param p Partition.
     */
    void addPartition(int p) {
        if (parts == null)
            parts = new HashSet<>();

        parts.add(p);
    }


    /**
     * @return Partition.
     */
    Collection<Integer> partitions() {
        return parts;
    }

    /**
     * @param updateSeq Update sequence.
     */
    void updateSequence(long updateSeq) {
        this.updateSeq = updateSeq;
    }

    /**
     * @return Update sequence.
     */
    long updateSequence() {
        return updateSeq;
    }

    /**
     * @return Reply message timeout.
     */
    long timeout() {
        return timeout;
    }

    /**
     * @param timeout Timeout.
     */
    void timeout(long timeout) {
        this.timeout = timeout;
    }

    /**
     * @return Topic.
     */
    Object topic() {
        return topic;
    }

    /**
     * @param topic Topic.
     */
    void topic(Object topic) {
        this.topic = topic;
    }

    /**
     * @return Worker ID.
     */
    int workerId() {
        return workerId;
    }

    /**
     * @param workerId Worker ID.
     */
    void workerId(int workerId) {
        this.workerId = workerId;
    }

    /**
     * @param part Partition to get counter for.
     * @return Partition counter associated with this partition or {@code null} if this information is unavailable.
     */
    Long partitionCounter(int part) {
        return partsCntrs == null ? null : partsCntrs.get(part);
    }

    /**
     * @return Topology version for which demand message is sent.
     */
    @Override public AffinityTopologyVersion topologyVersion() {
        return topVer;
    }

    /** {@inheritDoc}
     * @param ctx*/
    @Override public void prepareMarshal(GridCacheSharedContext ctx) throws IgniteCheckedException {
        super.prepareMarshal(ctx);

        if (topic != null && topicBytes == null)
            topicBytes = U.marshal(ctx, topic);
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheSharedContext ctx, ClassLoader ldr) throws IgniteCheckedException {
        super.finishUnmarshal(ctx, ldr);

        if (topicBytes != null && topic == null)
            topic = U.unmarshal(ctx, topicBytes, U.resolveClassLoader(ldr, ctx.gridConfig()));
    }

    /** {@inheritDoc} */
    @Override public boolean addDeploymentInfo() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        writer.setBuffer(buf);

        if (!super.writeTo(buf, writer))
            return false;

        if (!writer.isHeaderWritten()) {
            if (!writer.writeHeader(directType(), fieldsCount()))
                return false;

            writer.onHeaderWritten();
        }

        switch (writer.state()) {
            case 3:
                if (!writer.writeCollection("parts", parts, MessageCollectionItemType.INT))
                    return false;

                writer.incrementState();

            case 4:
                if (!writer.writeMap("partsCntrs", partsCntrs, MessageCollectionItemType.INT, MessageCollectionItemType.LONG))
                    return false;

                writer.incrementState();

            case 5:
                if (!writer.writeLong("timeout", timeout))
                    return false;

                writer.incrementState();

            case 6:
                if (!writer.writeMessage("topVer", topVer))
                    return false;

                writer.incrementState();

            case 7:
                if (!writer.writeByteArray("topicBytes", topicBytes))
                    return false;

                writer.incrementState();

            case 8:
                if (!writer.writeLong("updateSeq", updateSeq))
                    return false;

                writer.incrementState();

            case 9:
                if (!writer.writeInt("workerId", workerId))
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

        if (!super.readFrom(buf, reader))
            return false;

        switch (reader.state()) {
            case 3:
                parts = reader.readCollection("parts", MessageCollectionItemType.INT);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 4:
                partsCntrs = reader.readMap("partsCntrs", MessageCollectionItemType.INT, MessageCollectionItemType.LONG, false);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 5:
                timeout = reader.readLong("timeout");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 6:
                topVer = reader.readMessage("topVer");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 7:
                topicBytes = reader.readByteArray("topicBytes");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 8:
                updateSeq = reader.readLong("updateSeq");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 9:
                workerId = reader.readInt("workerId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(GridDhtPartitionDemandMessage.class);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 44;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 10;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDhtPartitionDemandMessage.class, this,
            "partCnt", parts != null ? parts.size() : 0,
            "super", super.toString());
    }
}
