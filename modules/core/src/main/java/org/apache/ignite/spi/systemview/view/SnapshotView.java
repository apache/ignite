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

import java.util.Set;
import org.apache.ignite.internal.managers.systemview.walker.Filtrable;
import org.apache.ignite.internal.managers.systemview.walker.Order;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

/**
 * Snapshot representation for a {@link SystemView}.
 */
public class SnapshotView {
    /** Snapshot name. */
    private final String name;

    /** Node consistent id. */
    private final String nodeId;

    /** Baseline node consistent id. */
    private final String baselineNodeId;

    /** Cache group name. */
    private final String cacheGrp;

    /** Partition numbers of the cache group stored in the snapshot. */
    private final String locPartitions;

    /**
     * @param name Snapshot name.
     * @param nodeId Node consistent id.
     * @param baselineNodeId Baseline node consistent id.
     * @param cacheGrp Cache group name.
     * @param locPartitions Partition numbers of the cache group stored in the snapshot.
     */
    public SnapshotView(
        String name,
        String nodeId,
        String baselineNodeId,
        String cacheGrp,
        @Nullable Set<Integer> locPartitions
    ) {
        this.name = name;
        this.nodeId = nodeId;
        this.baselineNodeId = baselineNodeId;
        this.cacheGrp = cacheGrp;
        this.locPartitions = locPartitions != null ? S.compact(locPartitions) : "[]";
    }

    /**
     * @return Snapshot name.
     */
    @Order
    @Filtrable
    public String snapshotName() {
        return name;
    }

    /**
     * @return Node consistent id.
     */
    @Order(1)
    @Filtrable
    public String nodeId() {
        return nodeId;
    }

    /**
     * @return Baseline node consistent id.
     */
    @Order(2)
    @Filtrable
    public String baselineNodeId() {
        return baselineNodeId;
    }

    /**
     * @return Cache group name.
     */
    @Order(3)
    public String cacheGroup() {
        return cacheGrp;
    }

    /**
     * @return Partition numbers of the cache group stored in the snapshot.
     */
    @Order(4)
    public String localPartitions() {
        return locPartitions;
    }
}
