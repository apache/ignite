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

import java.nio.ByteBuffer;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridDirectTransient;
import org.apache.ignite.internal.IgniteCodeGeneratingFail;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheGroupIdMessage;
import org.apache.ignite.internal.processors.cache.GridCachePartitionExchangeManager;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.jetbrains.annotations.NotNull;

/**
 * Partition demand request.
 */
@IgniteCodeGeneratingFail
public class GridDhtPartitionDemandMessage extends GridCacheGroupIdMessage {
    /** Cache rebalance topic. */
    private static final Object REBALANCE_TOPIC = GridCachePartitionExchangeManager.rebalanceTopic(0);

    /** Rebalance id. */
    private long rebalanceId;

    /** Partitions map. */
    @GridDirectTransient
    private IgniteDhtDemandedPartitionsMap parts;

    /** Serialized partitions map. */
    private byte[] partsBytes;

    /** Topic. */
    @GridDirectTransient
    private Object topic = REBALANCE_TOPIC;

    /** Serialized topic. */
    private byte[] topicBytes;

    /** Timeout. */
    private long timeout;

    /** Worker ID. */
    private int workerId = -1;

    /** Topology version. */
    private AffinityTopologyVersion topVer;

    /**
     * @param rebalanceId Rebalance id for this node.
     * @param topVer Topology version.
     * @param grpId Cache group ID.
     */
    GridDhtPartitionDemandMessage(long rebalanceId, @NotNull AffinityTopologyVersion topVer, int grpId) {
        this(rebalanceId, topVer, grpId, new IgniteDhtDemandedPartitionsMap());
    }

    /**
     * @param rebalanceId Rebalance id for this node.
     * @param topVer Topology version.
     * @param grpId Cache group ID.
     * @param parts Demand partiton map.
     */
    GridDhtPartitionDemandMessage(long rebalanceId, @NotNull AffinityTopologyVersion topVer, int grpId,
        IgniteDhtDemandedPartitionsMap parts) {
        this.grpId = grpId;
        this.rebalanceId = rebalanceId;
        this.topVer = topVer;
        this.parts = parts;
    }

    /**
     * Empty constructor.
     */
    public GridDhtPartitionDemandMessage() {
        // No-op.
    }

    /**
     * Creates copy of this message with new partitions map.
     *
     * @param parts New partitions map.
     * @return Copy of message with new partitions map.
     */
    public GridDhtPartitionDemandMessage withNewPartitionsMap(@NotNull IgniteDhtDemandedPartitionsMap parts) {
        GridDhtPartitionDemandMessage cp = new GridDhtPartitionDemandMessage();
        cp.grpId = grpId;
        cp.rebalanceId = rebalanceId;
        cp.topic = topic;
        cp.timeout = timeout;
        cp.workerId = workerId;
        cp.topVer = topVer;
        cp.parts = parts;
        return cp;
    }

    /**
     * @return Partition.
     */
    public IgniteDhtDemandedPartitionsMap partitions() {
        return parts;
    }

    /**
     * @param updateSeq Update sequence.
     */
    void rebalanceId(long updateSeq) {
        this.rebalanceId = updateSeq;
    }

    /**
     * @return Unique rebalance session id.
     */
    long rebalanceId() {
        return rebalanceId;
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
     * @return Topology version for which demand message is sent.
     */
    @Override public AffinityTopologyVersion topologyVersion() {
        return topVer;
    }

    /** {@inheritDoc} */
    @Override public void prepareMarshal(GridCacheSharedContext ctx) throws IgniteCheckedException {
        super.prepareMarshal(ctx);

        if (topic != null && topicBytes == null)
            topicBytes = U.marshal(ctx, topic);

        if (parts != null && partsBytes == null)
            partsBytes = U.marshal(ctx, parts);
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheSharedContext ctx, ClassLoader ldr) throws IgniteCheckedException {
        super.finishUnmarshal(ctx, ldr);

        if (topicBytes != null && topic == null)
            topic = U.unmarshal(ctx, topicBytes, U.resolveClassLoader(ldr, ctx.gridConfig()));

        if (partsBytes != null && parts == null)
            parts = U.unmarshal(ctx, partsBytes, U.resolveClassLoader(ldr, ctx.gridConfig()));
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
            if (!writer.writeHeader(directType()))
                return false;

            writer.onHeaderWritten();
        }

        switch (writer.state()) {
            case 4:
                if (!writer.writeByteArray(partsBytes))
                    return false;

                writer.incrementState();

            case 5:
                if (!writer.writeLong(timeout))
                    return false;

                writer.incrementState();

            case 6:
                if (!writer.writeAffinityTopologyVersion(topVer))
                    return false;

                writer.incrementState();

            case 7:
                if (!writer.writeByteArray(topicBytes))
                    return false;

                writer.incrementState();

            case 8:
                if (!writer.writeLong(rebalanceId))
                    return false;

                writer.incrementState();

            case 9:
                if (!writer.writeInt(workerId))
                    return false;

                writer.incrementState();

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean readFrom(ByteBuffer buf, MessageReader reader) {
        reader.setBuffer(buf);

        if (!super.readFrom(buf, reader))
            return false;

        switch (reader.state()) {
            case 4:
                partsBytes = reader.readByteArray();

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 5:
                timeout = reader.readLong();

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 6:
                topVer = reader.readAffinityTopologyVersion();

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 7:
                topicBytes = reader.readByteArray();

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 8:
                rebalanceId = reader.readLong();

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 9:
                workerId = reader.readInt();

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 45;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDhtPartitionDemandMessage.class, this,
            "partCnt", parts != null ? parts.size() : 0,
            "super", super.toString());
    }
}
