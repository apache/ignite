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
    private final UUID rqId;

    /** Snapshot name. */
    private final String snpName;

    /** Consistent id of a node to which this metadata relates. */
    private final String consId;

    /**
     * Directory related to the current consistent node id on which partition files are stored.
     * For some of the cases, consId doesn't equal the directory name.
     */
    private final String folderName;

    /** Page size of stored snapshot data. */
    private final int pageSize;

    /** The list of cache groups ids which were included into snapshot. */
    @GridToStringInclude
    private final List<Integer> grpIds;

    /** The set of affected by snapshot baseline nodes. */
    @GridToStringInclude
    private final Set<String> bltNodes;

    /**
     * Map of cache group partitions from which snapshot has been taken on the local node. This map can be empty
     * since for instance, due to the node filter there is no cache data on node.
     */
    @GridToStringInclude
    private final Map<Integer, Set<Integer>> locParts = new HashMap<>();

    /**
     * @param rqId Unique snapshot request id.
     * @param snpName Snapshot name.
     * @param consId Consistent id of a node to which this metadata relates.
     * @param folderName Directory name which stores the data files.
     * @param pageSize Page size of stored snapshot data.
     * @param grpIds The list of cache groups ids which were included into snapshot.
     * @param bltNodes The set of affected by snapshot baseline nodes.
     */
    public SnapshotMetadata(
        UUID rqId,
        String snpName,
        String consId,
        String folderName,
        int pageSize,
        List<Integer> grpIds,
        Set<String> bltNodes,
        Set<GroupPartitionId> pairs
    ) {
        this.rqId = rqId;
        this.snpName = snpName;
        this.consId = consId;
        this.folderName = folderName;
        this.pageSize = pageSize;
        this.grpIds = grpIds;
        this.bltNodes = bltNodes;

        pairs.forEach(p ->
            locParts.computeIfAbsent(p.getGroupId(), k -> new HashSet<>())
                .add(p.getPartitionId()));
    }

    /**
     * @return Unique snapshot request id.
     */
    public UUID requestId() {
        return rqId;
    }

    /**
     * @return Snapshot name.
     */
    public String snapshotName() {
        return snpName;
    }

    /**
     * @return Consistent id of a node to which this metadata relates.
     */
    public String consistentId() {
        return consId;
    }

    /**
     * @return Directory name which stores the data files.
     */
    public String folderName() {
        return folderName;
    }

    /**
     * @return Page size of stored snapshot data.
     */
    public int pageSize() {
        return pageSize;
    }

    /**
     * @return The list of cache group IDs which were included into the snapshot globally.
     */
    public List<Integer> cacheGroupIds() {
        return grpIds;
    }

    /**
     * @return The set of affected by snapshot baseline nodes.
     */
    public Set<String> baselineNodes() {
        return bltNodes;
    }

    /**
     * @return Map of cache group partitions from which snapshot has been taken on the local node (which is actually
     * saved on the local node because some of them may be skipped due to cache node filter).
     */
    public Map<Integer, Set<Integer>> partitions() {
        return locParts;
    }

    /**
     * @param compare Snapshot metadata to compare.
     * @return {@code true} if given metadata belongs to the same snapshot.
     */
    public boolean sameSnapshot(SnapshotMetadata compare) {
        return requestId().equals(compare.requestId()) &&
            snapshotName().equals(compare.snapshotName()) &&
            pageSize() == compare.pageSize() &&
            Objects.equals(cacheGroupIds(), compare.cacheGroupIds()) &&
            Objects.equals(baselineNodes(), compare.baselineNodes());
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
