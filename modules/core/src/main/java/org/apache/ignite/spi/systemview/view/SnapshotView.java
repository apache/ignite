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

import org.apache.ignite.internal.managers.systemview.walker.Order;

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

    /**
     * @param name Snapshot name.
     * @param consistentId Node consistent ID.
     * @param baselineNodes Baseline nodes affected by the snapshot.
     * @param cacheGrps Cache group names that were included in the snapshot.
     */
    public SnapshotView(
        String name,
        String consistentId,
        String baselineNodes,
        String cacheGrps
    ) {
        this.name = name;
        this.consistentId = consistentId;
        this.baselineNodes = baselineNodes;
        this.cacheGrps = cacheGrps;
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
}
