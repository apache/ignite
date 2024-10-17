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

import java.io.IOException;
import java.io.InvalidObjectException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.internal.pagemem.wal.record.delta.ClusterSnapshotRecord;
import org.apache.ignite.internal.processors.cache.persistence.partstate.GroupPartitionId;
import org.apache.ignite.internal.processors.cache.persistence.wal.WALPointer;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

/**
 * Snapshot metadata file.
 */
public class SnapshotMetadata implements Serializable {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** Unique snapshot request id. */
    private final UUID rqId;

    /** Snapshot name. */
    @GridToStringInclude
    private final String snpName;

    /** Consistent id of a node to which this metadata relates. */
    @GridToStringInclude
    private final String consId;

    /**
     * Directory related to the current consistent node id on which partition files are stored.
     * For some of the cases, consId doesn't equal the directory name.
     */
    private final String folderName;

    /**
     * If {@code true} then compress partition files.
     * This shouldn't be confused with {@link SnapshotMetadata#comprGrpIds} which represents how Ignite keeps data in memory pages
     * while {@link SnapshotMetadata#comprParts} represents how dump files are stored on disk.
     */
    private final boolean comprParts;

    /** Page size of stored snapshot data. */
    private final int pageSize;

    /** The list of cache groups ids which were included into snapshot. */
    @GridToStringInclude
    private final List<Integer> grpIds;

    /** The set of affected by snapshot baseline nodes. */
    @GridToStringInclude
    private final Set<String> bltNodes;

    /** WAL pointer to {@link ClusterSnapshotRecord} if exists. */
    private final @Nullable WALPointer snpRecPtr;

    /**
     * Map of cache group partitions from which snapshot has been taken on the local node. This map can be empty
     * since for instance, due to the node filter there is no cache data on node.
     */
    @GridToStringInclude
    private transient Map<Integer, Set<Integer>> locParts = new HashMap<>();

    /** Master key digest for encrypted caches. */
    @GridToStringInclude
    @Nullable private final byte[] masterKeyDigest;

    /** Warnings occurred at snapshot creation. */
    @GridToStringInclude
    @Nullable private List<String> warnings;

    /** Creation timestamp in milliseconds since Unix epoch. */
    private long snapshotTime;

    /** */
    private transient Set<Integer> comprGrpIds;

    /** */
    private boolean hasComprGrps;

    /** If {@code true} snapshot only primary copies of partitions. */
    private boolean onlyPrimary;

    /** If {@code true} cache group dump stored. */
    private boolean dump;

    /** Encryption key. */
    private @Nullable byte[] encKey;

    /**
     * @param rqId Unique request id.
     * @param snpName Snapshot name.
     * @param consId Consistent id of a node to which this metadata relates.
     * @param folderName Directory name which stores the data files.
     * @param comprParts If {@code true} then compress partition files.
     * @param pageSize Page size of stored snapshot data.
     * @param grpIds The list of cache groups ids which were included into snapshot.
     * @param bltNodes The set of affected by snapshot baseline nodes.
     * @param snpRecPtr WAL pointer to {@link ClusterSnapshotRecord} if exists.
     * @param masterKeyDigest Master key digest for encrypted caches.
     * @param snapshotTime of the snapshot creation.
     * @param onlyPrimary If {@code true} snapshot only primary copies of partitions.
     * @param dump If {@code true} cache group dump stored.
     * @param encKey Encryption key. For dumps, only.
     */
    public SnapshotMetadata(
        UUID rqId,
        String snpName,
        String consId,
        String folderName,
        boolean comprParts,
        int pageSize,
        List<Integer> grpIds,
        long snapshotTime,
        Collection<Integer> compGrpIds,
        Set<String> bltNodes,
        Set<GroupPartitionId> pairs,
        @Nullable WALPointer snpRecPtr,
        @Nullable byte[] masterKeyDigest,
        boolean onlyPrimary,
        boolean dump,
        @Nullable byte[] encKey
    ) {
        this.rqId = rqId;
        this.snpName = snpName;
        this.consId = consId;
        this.folderName = folderName;
        this.comprParts = comprParts;
        this.pageSize = pageSize;
        this.grpIds = grpIds;
        this.snapshotTime = snapshotTime;
        this.bltNodes = bltNodes;
        this.snpRecPtr = snpRecPtr;
        this.masterKeyDigest = masterKeyDigest;
        this.onlyPrimary = onlyPrimary;
        this.dump = dump;
        this.encKey = encKey;

        if (!F.isEmpty(compGrpIds)) {
            hasComprGrps = true;

            comprGrpIds = new HashSet<>(compGrpIds);
        }

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
     * @return {@code true} if compress partition files.
     */
    public boolean compressPartitions() {
        return comprParts;
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
        return Collections.unmodifiableMap(locParts);
    }

    /** */
    public boolean isGroupWithCompression(int grpId) {
        return hasComprGrps && comprGrpIds.contains(grpId);
    }

    /** */
    public boolean hasCompressedGroups() {
        return hasComprGrps;
    }

    /**
     * @return WAL pointer to {@link ClusterSnapshotRecord} if exists.
     */
    public @Nullable WALPointer snapshotRecordPointer() {
        return snpRecPtr;
    }

    /** @return If {@code true} snapshot only primary copies of partitions. */
    public boolean onlyPrimary() {
        return onlyPrimary;
    }

    /** @return If {@code true} then metadata describes cache dump. */
    public boolean dump() {
        return dump;
    }

    /** @return Creation timestamp in milliseconds since Unix epoch. */
    public long snapshotTime() {
        return snapshotTime;
    }

    /** Save the state of this <tt>HashMap</tt> partitions and cache groups to a stream. */
    private void writeObject(java.io.ObjectOutputStream s)
        throws java.io.IOException {
        // Write out any hidden serialization.
        s.defaultWriteObject();

        // Write out size of map.
        s.writeInt(locParts.size());

        // Write out all elements in the proper order.
        for (Map.Entry<Integer, Set<Integer>> e : locParts.entrySet()) {
            s.writeInt(e.getKey());
            s.writeInt(e.getValue().size());

            for (Integer partId : e.getValue())
                s.writeInt(partId);
        }

        if (hasComprGrps)
            U.writeCollection(s, comprGrpIds);
    }

    /** Reconstitute the <tt>HashMap</tt> instance of partitions and cache groups from a stream. */
    private void readObject(java.io.ObjectInputStream s) throws IOException, ClassNotFoundException {
        // Read in any hidden serialization.
        s.defaultReadObject();

        // Read size and verify non-negative.
        int size = s.readInt();

        if (size < 0)
            throw new InvalidObjectException("Illegal size: " + size);

        locParts = U.newHashMap(size);

        // Read in all elements in the proper order.
        for (int i = 0; i < size; i++) {
            int grpId = s.readInt();
            int total = s.readInt();

            if (total < 0)
                throw new InvalidObjectException("Illegal size: " + total);

            Set<Integer> parts = U.newHashSet(total);

            for (int k = 0; k < total; k++)
                parts.add(s.readInt());

            locParts.put(grpId, parts);
        }

        if (hasComprGrps)
            comprGrpIds = U.readSet(s);
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
            Arrays.equals(masterKeyDigest, compare.masterKeyDigest) &&
            Objects.equals(baselineNodes(), compare.baselineNodes()) &&
            onlyPrimary == compare.onlyPrimary;
    }

    /**
     * @return Master key digest for encrypted caches.
     */
    public byte[] masterKeyDigest() {
        return masterKeyDigest;
    }

    /** @return Encryption key. */
    public byte[] encryptionKey() {
        return encKey;
    }

    /**
     * @param warnings Snapshot creation warnings.
     */
    public void warnings(List<String> warnings) {
        assert this.warnings == null : "Snapshot warnings are already set. No rewriting is supposed.";

        this.warnings = warnings;
    }

    /**
     * @return Snapshot creation warnings.
     */
    public List<String> warnings() {
        return warnings;
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
            Objects.equals(bltNodes, meta.bltNodes) &&
            Arrays.equals(masterKeyDigest, meta.masterKeyDigest) &&
            Arrays.equals(encKey, meta.encKey) &&
            Objects.equals(warnings, meta.warnings) &&
            Objects.equals(hasComprGrps, meta.hasComprGrps) &&
            Objects.equals(comprGrpIds, meta.comprGrpIds) &&
            onlyPrimary == meta.onlyPrimary;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return Objects.hash(rqId, snpName, consId, grpIds, bltNodes, onlyPrimary);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(SnapshotMetadata.class, this);
    }
}
