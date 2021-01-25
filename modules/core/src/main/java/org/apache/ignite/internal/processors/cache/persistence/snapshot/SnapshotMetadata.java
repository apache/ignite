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

package org.apache.ignite.internal.processors.cache.persistence.snapshot;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.internal.processors.cache.persistence.partstate.GroupPartitionId;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Snapshot metadata file.
 */
public class SnapshotMetadata implements Serializable {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** Unique snapshot request id. */
    private UUID rqId;

    /** Snapshot name. */
    private String snpName;

    /** Consistent id of a node to which this metadata relates. */
    private String consId;

    /** The list of cache groups ids which were included into snapshot. */
    @GridToStringInclude
    private List<Integer> grpIds;

    /** The set of affected by snapshot baseline nodes. */
    @GridToStringInclude
    private Set<String> bltNodes;

    /** Map of cache group partitions from which snapshot has been taken on local node. */
    @GridToStringInclude
    private Map<Integer, Set<Integer>> parts = new HashMap<>();

    /**
     * No-arg constructor.
     */
    public SnapshotMetadata() {
        // No-arg.
    }

    /**
     * @param rqId Unique snapshot request id.
     * @param snpName Snapshot name.
     * @param consId Consistent id of a node to which this metadata relates.
     * @param grpIds The list of cache groups ids which were included into snapshot.
     * @param bltNodes The set of affected by snapshot baseline nodes.
     */
    public SnapshotMetadata(
        UUID rqId,
        String snpName,
        String consId,
        List<Integer> grpIds,
        Set<String> bltNodes,
        Set<GroupPartitionId> pairs
    ) {
        this.rqId = rqId;
        this.snpName = snpName;
        this.consId = consId;
        this.grpIds = grpIds;
        this.bltNodes = bltNodes;

        pairs.forEach(p ->
            parts.computeIfAbsent(p.getGroupId(), k -> new HashSet<>())
                .add(p.getPartitionId()));
    }

    /**
     * @return Unique snapshot request id.
     */
    public UUID requestId() {
        return rqId;
    }

    /**
     * @param rqId Unique snapshot request id.
     */
    public void requestId(UUID rqId) {
        this.rqId = rqId;
    }

    /**
     * @return Snapshot name.
     */
    public String snapshotName() {
        return snpName;
    }

    /**
     * @param snpName Snapshot name.
     */
    public void snapshotName(String snpName) {
        this.snpName = snpName;
    }

    /**
     * @return Consistent id of a node to which this metadata relates.
     */
    public String consistentId() {
        return consId;
    }

    /**
     * @param consId Consistent id of a node to which this metadata relates.
     */
    public void consistentId(String consId) {
        this.consId = consId;
    }

    /**
     * @return The list of cache groups ids which were included into snapshot.
     */
    public List<Integer> cacheGroupIds() {
        return grpIds;
    }

    /**
     * @param grpIds The list of cache groups ids which were included into snapshot.
     */
    public void cacheGroupIds(List<Integer> grpIds) {
        this.grpIds = grpIds;
    }

    /**
     * @return The set of affected by snapshot baseline nodes.
     */
    public Set<String> baselineNodes() {
        return bltNodes;
    }

    /**
     * @param bltNodes The set of affected by snapshot baseline nodes.
     */
    public void baselineNodes(Set<String> bltNodes) {
        this.bltNodes = bltNodes;
    }

    /**
     * @return Map of cache group partitions from which snapshot has been taken on local node.
     */
    public Map<Integer, Set<Integer>> partitions() {
        return parts;
    }

    /**
     * @param parts Map of cache group partitions from which snapshot has been taken on local node.
     */
    public void partitions(Map<Integer, Set<Integer>> parts) {
        this.parts = parts;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        SnapshotMetadata meta = (SnapshotMetadata)o;

        return rqId.equals(meta.rqId) &&
            snpName.equals(meta.snpName) &&
            consId.equals(meta.consId) &&
            Objects.equals(grpIds, meta.grpIds) &&
            Objects.equals(bltNodes, meta.bltNodes);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return Objects.hash(rqId, snpName, consId, grpIds, bltNodes);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(SnapshotMetadata.class, this);
    }
}
