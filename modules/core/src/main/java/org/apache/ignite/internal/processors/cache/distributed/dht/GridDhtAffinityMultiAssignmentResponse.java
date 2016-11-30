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
import org.apache.ignite.internal.GridDirectCollection;
import org.apache.ignite.internal.GridDirectTransient;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManager;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheMessage;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/**
 *
 */
public class GridDhtAffinityMultiAssignmentResponse extends GridCacheMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** Topology version. */
    // TODO: remove.
    protected AffinityTopologyVersion topVer;

    /** */
    @GridDirectCollection(int.class)
    private List<Integer> cacheIds;

    /** Affinity assignment. */
    @GridDirectTransient
    // TODO remove.
    private List<List<List<ClusterNode>>> affAssignments;

    /** */
    @GridDirectTransient
    private List<List<List<UUID>>> affAssignmentIds;

    /** */
    @GridDirectCollection(byte[].class)
    private List<byte[]> affAssignmentIdsBytes;

    /** */
    @GridDirectTransient
    private List<List<List<UUID>>> idealAffAssignment;

    /** Affinity assignment bytes. */
    @GridDirectCollection(byte[].class)
    private List<byte[]> idealAffAssignmentBytes;

    /**
     * Empty constructor.
     */
    public GridDhtAffinityMultiAssignmentResponse() {
        // No-op.
    }

    /**
     * @param topVer Topology version.
     */
    public GridDhtAffinityMultiAssignmentResponse(
        AffinityTopologyVersion topVer) {
        this.topVer = topVer;
        this.cacheId = 0;

        cacheIds = new ArrayList<>();
        affAssignmentIds = new ArrayList<>();
        idealAffAssignment = new ArrayList<>();
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

    /** {@inheritDoc} */
    @Override public boolean addDeploymentInfo() {
        return false;
    }

    /**
     * @return Size of the message.
     */
    public int size() {
        return affAssignmentIds != null ? affAssignmentIds.size() : 0;
    }

    /**
     * @param idx index.
     * @param disco Discovery manager.
     * @return Affinity assignment.
     */
    public List<List<ClusterNode>> affinityAssignment(int idx, GridDiscoveryManager disco) {
        assert affAssignmentIds != null;

        if (affAssignments == null) {
            affAssignments = new ArrayList<>();
            for (List<List<UUID>> affAssignmentId : affAssignmentIds) {
                List<List<ClusterNode>> aff = nodes(disco, affAssignmentId);
                affAssignments.add(aff);

            }
        }

        return affAssignments.get(idx);
    }

    /**
     * @param idx index.
     * @param disco Discovery manager.
     * @return Ideal affinity assignment.
     */
    public List<List<ClusterNode>> idealAffinityAssignment(int idx, GridDiscoveryManager disco) {
        return nodes(disco, idealAffAssignment.get(idx));
    }

    /**
     * @param idx index.
     * @return Cache ID.
     */
    public int getCacheId(int idx) {
        return cacheIds.get(idx);
    }

    /**
     * @param cacheId Cache ID.
     * @param assignment Affinity assignment.
     * @param idealAssignment Affinity ideal assignment.
     */
    public void addResult(int cacheId, List<List<ClusterNode>> assignment, List<List<ClusterNode>> idealAssignment) {
        cacheIds.add(cacheId);
        affAssignmentIds.add(ids(assignment));
        idealAffAssignment.add(ids(idealAssignment));
    }

    /**
     * @param disco Discovery manager.
     * @param assignmentIds Assignment node IDs.
     * @return Assignment nodes.
     */
    private List<List<ClusterNode>> nodes(GridDiscoveryManager disco, List<List<UUID>> assignmentIds) {
        if (assignmentIds != null) {
            List<List<ClusterNode>> assignment = new ArrayList<>(assignmentIds.size());

            for (int i = 0; i < assignmentIds.size(); i++) {
                List<UUID> ids = assignmentIds.get(i);
                List<ClusterNode> nodes = new ArrayList<>(ids.size());

                for (int j = 0; j < ids.size(); j++) {
                    ClusterNode node = disco.node(topVer, ids.get(j));

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
     * @param assignments Assignment.
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
    @Override public void prepareMarshal(GridCacheSharedContext ctx) throws IgniteCheckedException {
        super.prepareMarshal(ctx);

        if (affAssignmentIds != null && affAssignmentIdsBytes == null) {
            affAssignmentIdsBytes = new ArrayList<>(size());
            for (List<List<UUID>> assignmentId : affAssignmentIds) {
                affAssignmentIdsBytes.add(U.marshal(ctx, assignmentId));
            }
        }

        if (idealAffAssignment != null && idealAffAssignmentBytes == null) {
            idealAffAssignmentBytes = new ArrayList<>(size());
            for (List<List<UUID>> assignmentId : idealAffAssignment) {
                idealAffAssignmentBytes.add(U.marshal(ctx, assignmentId));
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheSharedContext ctx, ClassLoader ldr) throws IgniteCheckedException {
        super.finishUnmarshal(ctx, ldr);

        ldr = U.resolveClassLoader(ldr, ctx.gridConfig());

        if (affAssignmentIdsBytes != null && affAssignmentIds == null) {
            affAssignmentIds = new ArrayList<>(affAssignmentIdsBytes.size());
            for (byte[] bytes : affAssignmentIdsBytes) {
                affAssignmentIds.add((List<List<UUID>>)U.unmarshal(ctx, bytes, ldr));
            }
        }

        if (idealAffAssignmentBytes != null && idealAffAssignment == null) {
            idealAffAssignment = new ArrayList<>(idealAffAssignmentBytes.size());
            for (byte[] bytes : idealAffAssignmentBytes) {
                idealAffAssignment.add((List<List<UUID>>)U.unmarshal(ctx, bytes, ldr));
            }
        }
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return -38;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 7;
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
                if (!writer.writeCollection("affAssignmentIdsBytes", affAssignmentIdsBytes, MessageCollectionItemType.BYTE_ARR))
                    return false;

                writer.incrementState();

            case 4:
                if (!writer.writeCollection("cacheIds", cacheIds, MessageCollectionItemType.INT))
                    return false;

                writer.incrementState();

            case 5:
                if (!writer.writeCollection("idealAffAssignmentBytes", idealAffAssignmentBytes, MessageCollectionItemType.BYTE_ARR))
                    return false;

                writer.incrementState();

            case 6:
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
            case 3:
                affAssignmentIdsBytes = reader.readCollection("affAssignmentIdsBytes", MessageCollectionItemType.BYTE_ARR);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 4:
                cacheIds = reader.readCollection("cacheIds", MessageCollectionItemType.INT);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 5:
                idealAffAssignmentBytes = reader.readCollection("idealAffAssignmentBytes", MessageCollectionItemType.BYTE_ARR);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 6:
                topVer = reader.readMessage("topVer");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(GridDhtAffinityMultiAssignmentResponse.class);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDhtAffinityMultiAssignmentResponse.class, this);
    }
}
