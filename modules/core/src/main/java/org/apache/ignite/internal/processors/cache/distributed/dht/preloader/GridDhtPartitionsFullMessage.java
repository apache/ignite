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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridDirectTransient;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Information about partitions of all nodes in topology.
 */
public class GridDhtPartitionsFullMessage extends GridDhtPartitionsAbstractMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    @GridToStringInclude
    @GridDirectTransient
    private Map<Integer, GridDhtPartitionFullMap> parts;

    /** */
    private byte[] partsBytes;

    /** Partitions update counters. */
    @GridToStringInclude
    @GridDirectTransient
    private Map<Integer, Map<Integer, Long>> partCntrs;

    /** Serialized partitions counters. */
    private byte[] partCntrsBytes;

    /** Topology version. */
    private AffinityTopologyVersion topVer;

    /**
     * Required by {@link Externalizable}.
     */
    public GridDhtPartitionsFullMessage() {
        // No-op.
    }

    /**
     * @param id Exchange ID.
     * @param lastVer Last version.
     * @param topVer Topology version.
     */
    public GridDhtPartitionsFullMessage(@Nullable GridDhtPartitionExchangeId id,
        @Nullable GridCacheVersion lastVer,
        @NotNull AffinityTopologyVersion topVer) {
        super(id, lastVer);

        assert id == null || topVer.equals(id.topologyVersion());

        this.topVer = topVer;
    }

    /**
     * @return Local partitions.
     */
    public Map<Integer, GridDhtPartitionFullMap> partitions() {
        return parts;
    }

    /**
     * @param cacheId Cache ID.
     * @param fullMap Full partitions map.
     */
    public void addFullPartitionsMap(int cacheId, GridDhtPartitionFullMap fullMap) {
        if (parts == null)
            parts = new HashMap<>();

        if (!parts.containsKey(cacheId))
            parts.put(cacheId, fullMap);
    }

    /**
     * @param cacheId Cache ID.
     * @param cntrMap Partition update counters.
     */
    public void addPartitionUpdateCounters(int cacheId, Map<Integer, Long> cntrMap) {
        if (partCntrs == null)
            partCntrs = new HashMap<>();

        if (!partCntrs.containsKey(cacheId))
            partCntrs.put(cacheId, cntrMap);
    }

    /**
     * @param cacheId Cache ID.
     * @return Partition update counters.
     */
    public Map<Integer, Long> partitionUpdateCounters(int cacheId) {
        if (partCntrs != null) {
            Map<Integer, Long> res = partCntrs.get(cacheId);

            return res != null ? res : Collections.<Integer, Long>emptyMap();
        }

        return Collections.emptyMap();
    }

    /** {@inheritDoc} */
    @Override public void prepareMarshal(GridCacheSharedContext ctx) throws IgniteCheckedException {
        super.prepareMarshal(ctx);

        if (parts != null && partsBytes == null)
            partsBytes = ctx.marshaller().marshal(parts);

        if (partCntrs != null && partCntrsBytes == null)
            partCntrsBytes = ctx.marshaller().marshal(partCntrs);
    }

    /**
     * @return Topology version.
     */
    @Override public AffinityTopologyVersion topologyVersion() {
        return topVer;
    }

    /**
     * @param topVer Topology version.
     */
    public void topologyVersion(AffinityTopologyVersion topVer) {
        this.topVer = topVer;
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheSharedContext ctx, ClassLoader ldr) throws IgniteCheckedException {
        super.finishUnmarshal(ctx, ldr);

        if (partsBytes != null && parts == null)
            parts = ctx.marshaller().unmarshal(partsBytes, ldr);

        if (parts == null)
            parts = new HashMap<>();

        if (partCntrsBytes != null && partCntrs == null)
            partCntrs = ctx.marshaller().unmarshal(partCntrsBytes, ldr);

        if (partCntrs == null)
            partCntrs = new HashMap<>();
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
            case 5:
                if (!writer.writeByteArray("partCntrsBytes", partCntrsBytes))
                    return false;

                writer.incrementState();

            case 6:
                if (!writer.writeByteArray("partsBytes", partsBytes))
                    return false;

                writer.incrementState();

            case 7:
                if (!writer.writeMessage("topVer", topVer))
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
            case 5:
                partCntrsBytes = reader.readByteArray("partCntrsBytes");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 6:
                partsBytes = reader.readByteArray("partsBytes");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 7:
                topVer = reader.readMessage("topVer");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(GridDhtPartitionsFullMessage.class);
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 46;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 8;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDhtPartitionsFullMessage.class, this, "partCnt", parts != null ? parts.size() : 0,
            "super", super.toString());
    }
}
