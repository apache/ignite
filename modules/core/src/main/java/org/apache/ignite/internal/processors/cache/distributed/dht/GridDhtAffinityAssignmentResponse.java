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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridDirectTransient;
import org.apache.ignite.internal.managers.discovery.DiscoCache;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheGroupIdMessage;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionFullMap;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Affinity assignment response.
 */
public class GridDhtAffinityAssignmentResponse extends GridCacheGroupIdMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private long futId;

    /** Topology version. */
    private AffinityTopologyVersion topVer;

    /** */
    @GridDirectTransient
    private List<List<UUID>> affAssignmentIds;

    /** */
    private byte[] affAssignmentIdsBytes;

    /** */
    @GridDirectTransient
    private List<List<UUID>> idealAffAssignment;

    /** Affinity assignment bytes. */
    private byte[] idealAffAssignmentBytes;

    /** */
    @GridDirectTransient
    private GridDhtPartitionFullMap partMap;

    /** */
    private byte[] partBytes;

    /**
     * Empty constructor.
     */
    public GridDhtAffinityAssignmentResponse() {
        // No-op.
    }

    /**
     * @param futId Future ID.
     * @param grpId Cache group ID.
     * @param topVer Topology version.
     * @param affAssignment Affinity assignment.
     */
    public GridDhtAffinityAssignmentResponse(
        long futId,
        int grpId,
        @NotNull AffinityTopologyVersion topVer,
        List<List<ClusterNode>> affAssignment) {
        this.futId = futId;
        this.grpId = grpId;
        this.topVer = topVer;

        affAssignmentIds = ids(affAssignment);
    }

    /**
     * @return Future ID.
     */
    public long futureId() {
        return futId;
    }

    /** {@inheritDoc} */
    @Override public boolean partitionExchangeMessage() {
        return true;
    }

    /**
     * @return Topology version.
     */
    @Override public AffinityTopologyVersion topologyVersion() {
        return topVer;
    }

    /**
     * @param discoCache Discovery data cache.
     * @return Affinity assignment.
     */
    public List<List<ClusterNode>> affinityAssignment(DiscoCache discoCache) {
        if (affAssignmentIds != null)
            return nodes(discoCache, affAssignmentIds);

        return null;
    }

    /**
     * @param discoCache Discovery data cache.
     * @return Ideal affinity assignment.
     */
    public List<List<ClusterNode>> idealAffinityAssignment(DiscoCache discoCache) {
        return nodes(discoCache, idealAffAssignment);
    }

    /**
     * @param discoCache Discovery data cache.
     * @param assignmentIds Assignment node IDs.
     * @return Assignment nodes.
     */
    private List<List<ClusterNode>> nodes(DiscoCache discoCache, List<List<UUID>> assignmentIds) {
        if (assignmentIds != null) {
            List<List<ClusterNode>> assignment = new ArrayList<>(assignmentIds.size());

            for (int i = 0; i < assignmentIds.size(); i++) {
                List<UUID> ids = assignmentIds.get(i);
                List<ClusterNode> nodes = new ArrayList<>(ids.size());

                for (int j = 0; j < ids.size(); j++) {
                    ClusterNode node = discoCache.node(ids.get(j));

                    assert node != null;

                    nodes.add(node);
                }

                assignment.add(nodes);
            }

            return assignment;
        }

        return null;
    }

    /**
     * @param idealAffAssignment Ideal affinity assignment.
     */
    public void idealAffinityAssignment(List<List<ClusterNode>> idealAffAssignment) {
        this.idealAffAssignment = ids(idealAffAssignment);
    }

    /**
     * @param partMap Partition map.
     */
    public void partitionMap(GridDhtPartitionFullMap partMap) {
        this.partMap = partMap;
    }

    /**
     * @return Partition map.
     */
    @Nullable public GridDhtPartitionFullMap partitionMap() {
        return partMap;
    }

    /**
     * @param assignments Assignment.
     * @return Assignment where cluster nodes are converted to their ids.
     */
    private List<List<UUID>> ids(List<List<ClusterNode>> assignments) {
        if (assignments != null) {
            List<List<UUID>> assignment = new ArrayList<>(assignments.size());

            for (int i = 0; i < assignments.size(); i++) {
                List<ClusterNode> nodes = assignments.get(i);
                List<UUID> ids = new ArrayList<>(nodes.size());

                for (int j = 0; j < nodes.size(); j++)
                    ids.add(nodes.get(j).id());

                assignment.add(ids);
            }

            return assignment;
        }

        return null;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 29;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 9;
    }

    /**
     * @param ctx Context.
     */
    @Override public void prepareMarshal(GridCacheSharedContext ctx) throws IgniteCheckedException {
        super.prepareMarshal(ctx);

        assert affAssignmentIds != null;

        affAssignmentIdsBytes = U.marshal(ctx, affAssignmentIds);

        if (idealAffAssignment != null && idealAffAssignmentBytes == null)
            idealAffAssignmentBytes = U.marshal(ctx, idealAffAssignment);

        if (partMap != null && partBytes == null)
            partBytes = U.zip(U.marshal(ctx.marshaller(), partMap));
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheSharedContext ctx, ClassLoader ldr) throws IgniteCheckedException {
        super.finishUnmarshal(ctx, ldr);

        assert affAssignmentIdsBytes != null;

        ldr = U.resolveClassLoader(ldr, ctx.gridConfig());

        affAssignmentIds = U.unmarshal(ctx, affAssignmentIdsBytes, ldr);

        if (idealAffAssignmentBytes != null && idealAffAssignment == null)
            idealAffAssignment = U.unmarshal(ctx, idealAffAssignmentBytes, ldr);

        if (partBytes != null && partMap == null)
            partMap = U.unmarshalZip(ctx.marshaller(), partBytes, U.resolveClassLoader(ldr, ctx.gridConfig()));
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
            case 4:
                if (!writer.writeByteArray("affAssignmentIdsBytes", affAssignmentIdsBytes))
                    return false;

                writer.incrementState();

            case 5:
                if (!writer.writeLong("futId", futId))
                    return false;

                writer.incrementState();

            case 6:
                if (!writer.writeByteArray("idealAffAssignmentBytes", idealAffAssignmentBytes))
                    return false;

                writer.incrementState();

            case 7:
                if (!writer.writeByteArray("partBytes", partBytes))
                    return false;

                writer.incrementState();

            case 8:
                if (!writer.writeAffinityTopologyVersion("topVer", topVer))
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
            case 4:
                affAssignmentIdsBytes = reader.readByteArray("affAssignmentIdsBytes");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 5:
                futId = reader.readLong("futId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 6:
                idealAffAssignmentBytes = reader.readByteArray("idealAffAssignmentBytes");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 7:
                partBytes = reader.readByteArray("partBytes");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 8:
                topVer = reader.readAffinityTopologyVersion("topVer");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(GridDhtAffinityAssignmentResponse.class);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDhtAffinityAssignmentResponse.class, this);
    }
}
