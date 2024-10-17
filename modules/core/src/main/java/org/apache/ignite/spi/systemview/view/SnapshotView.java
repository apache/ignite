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

package org.apache.ignite.spi.systemview.view;

import java.util.Collection;
import org.apache.ignite.internal.managers.systemview.walker.Order;
import org.apache.ignite.internal.pagemem.wal.record.delta.ClusterSnapshotRecord;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.IncrementalSnapshotMetadata;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotMetadata;
import org.apache.ignite.internal.util.typedef.F;

/**
 * Snapshot representation for a {@link SystemView}.
 */
public class SnapshotView {
    /** Snapshot system view name. */
    public static final String SNAPSHOT_SYS_VIEW = "snapshot";

    /** Snapshot system view description. */
    public static final String SNAPSHOT_SYS_VIEW_DESC = "Snapshot";

    /** Snapshot name. */
    private final String name;

    /** Node consistent ID. */
    private final String consistentId;

    /** Baseline nodes affected by the snapshot. */
    private final String baselineNodes;

    /** Cache group names that were included in the snapshot. */
    private final String cacheGrps;

    /** WAL segment that contains {@link ClusterSnapshotRecord} if exists. */
    private final Long snpRecSeg;

    /** Creation timestamp in milliseconds since Unix epoch. */
    private final Long snapshotTime;

    /** Full or incremental. */
    private final SnapshotType type;

    /** Incremental snapshot index. */
    private final Integer incIdx;

    /**
     * @param meta Snapshot metadata.
     * @param cacheGrps Cache group names that were included in the snapshot.
     */
    public SnapshotView(
        SnapshotMetadata meta,
        Collection<String> cacheGrps
    ) {
        type = meta.dump() ? SnapshotType.DUMP : SnapshotType.FULL;
        name = meta.snapshotName();
        consistentId = meta.consistentId();
        baselineNodes = F.concat(meta.baselineNodes(), ",");
        snpRecSeg = meta.snapshotRecordPointer() == null ? null : meta.snapshotRecordPointer().index();
        snapshotTime = meta.snapshotTime();
        incIdx = null;

        this.cacheGrps = F.concat(cacheGrps, ",");
    }

    /**
     * @param incMeta Incremental snapshot metadata.
     */
    public SnapshotView(IncrementalSnapshotMetadata incMeta) {
        type = SnapshotType.INCREMENTAL;
        name = incMeta.snapshotName();
        consistentId = incMeta.consistentId();
        snpRecSeg = incMeta.incrementalSnapshotPointer().index();
        incIdx = incMeta.incrementIndex();
        snapshotTime = incMeta.snapshotTime();
        baselineNodes = null;
        cacheGrps = null;
    }

    /**
     * @return Snapshot name.
     */
    @Order
    public String name() {
        return name;
    }

    /**
     * @return Node consistent ID.
     */
    @Order(1)
    public String consistentId() {
        return consistentId;
    }

    /**
     * @return Baseline nodes affected by the snapshot.
     */
    @Order(2)
    public String baselineNodes() {
        return baselineNodes;
    }

    /**
     * @return Cache group names that were included in the snapshot.
     */
    @Order(3)
    public String cacheGroups() {
        return cacheGrps;
    }

    /**
     * @return WAL segment that contains {@link ClusterSnapshotRecord} if exists.
     */
    @Order(4)
    public Long snapshotRecordSegment() {
        return snpRecSeg;
    }

    /**
     * @return Incremental snapshot index, {@code null} for full snapshot.
     */
    @Order(5)
    public Integer incrementIndex() {
        return incIdx;
    }

    /**
     * @return Snapshot type.
     */
    @Order(6)
    public String type() {
        return type.name();
    }

    /**
     *  @return Creation timestamp in milliseconds since Unix epoch.
     */
    @Order(7)
    public Long snapshotTime() {
        return snapshotTime != 0 ? snapshotTime : null;
    }

    /** Snapshot types. */
    private enum SnapshotType {
        /** Full snapshot. */
        FULL,

        /** Incremental snapshot. */
        INCREMENTAL,

        /** Dump. */
        DUMP
    }
}
