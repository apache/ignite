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

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.managers.discovery.DiscoCache;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheGroupIdMessage;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionFullMap;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Affinity assignment response.
 */
public class GridDhtAffinityAssignmentResponse extends GridCacheGroupIdMessage {
    /** */
    @Order(4)
    long futId;

    /** Topology version. */
    @Order(5)
    AffinityTopologyVersion topVer;

    /** */
    private List<List<UUID>> affAssignmentIds;

    /** */
    @Order(6)
    byte[] affAssignmentIdsBytes;

    /** */
    private List<List<UUID>> idealAffAssignment;

    /** Affinity assignment bytes. */
    @Order(7)
    byte[] idealAffAssignmentBytes;

    /** */
    private GridDhtPartitionFullMap partMap;

    /** */
    @Order(8)
    byte[] partBytes;

    /** Indicates that getting required affinity assignments has been failed. */
    private IgniteCheckedException affAssignmentErr;

    /** Serialized error. */
    @Order(9)
    byte[] affAssignmentErrBytes;

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

    /** */
    public void futureId(long futId) {
        this.futId = futId;
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

    /** */
    public void topologyVersion(AffinityTopologyVersion topVer) {
        this.topVer = topVer;
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

    /** */
    public byte[] affinityAssignmentIdsBytes() {
        return affAssignmentIdsBytes;
    }

    /** */
    public void affinityAssignmentIdsBytes(byte[] affAssignmentIdsBytes) {
        this.affAssignmentIdsBytes = affAssignmentIdsBytes;
    }

    /** */
    public byte[] idealAffinityAssignmentsBytes() {
        return idealAffAssignmentBytes;
    }

    /** */
    public void idealAffinityAssignmentsBytes(byte[] idealAffAssignmentBytes) {
        this.idealAffAssignmentBytes = idealAffAssignmentBytes;
    }

    /** */
    public byte[] partitionBytes() {
        return partBytes;
    }

    /** */
    public void partitionBytes(byte[] partBytes) {
        this.partBytes = partBytes;
    }

    /** */
    public byte[] affinityAssignmentsErrorBytes() {
        return affAssignmentErrBytes;
    }

    /** */
    public void affinityAssignmentsErrorBytes(byte[] affAssignmentErrBytes) {
        this.affAssignmentErrBytes = affAssignmentErrBytes;
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

        if (affAssignmentErr != null && affAssignmentErrBytes == null)
            affAssignmentErrBytes = U.marshal(ctx, affAssignmentErr);
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

        if (affAssignmentErrBytes != null && affAssignmentErr == null)
            affAssignmentErr = U.unmarshal(ctx, affAssignmentErrBytes, U.resolveClassLoader(ldr, ctx.gridConfig()));
    }

    /**
     * Error that caused failure to get affinity assignments.
     *
     * @param err Cause of failure to calculate/get affiniti assignments.
     */
    public void affinityAssignmentsError(IgniteCheckedException err) {
        affAssignmentErr = err;
    }

    /**
     * Returns error that caused failure to get affinity assignments.
     *
     * @return Error that caused failure to get affinity assignments.
     */
    public IgniteCheckedException affinityAssignmentsError() {
        return affAssignmentErr;
    }

    /** {@inheritDoc} */
    @Override public boolean addDeploymentInfo() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDhtAffinityAssignmentResponse.class, this);
    }
}
