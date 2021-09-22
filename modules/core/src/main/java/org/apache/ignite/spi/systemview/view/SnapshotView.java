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

import java.util.Objects;
import org.apache.ignite.internal.managers.systemview.walker.Filtrable;
import org.apache.ignite.internal.managers.systemview.walker.Order;

/**
 * Snapshot representation for a {@link SystemView}.
 */
public class SnapshotView {
    /** Snapshot name. */
    private final String name;

    /** Node consistent id. */
    private final String nodeId;

    /** Baseline nodes affected by snapshots. */
    private final String baselineNodes;

    /** The cache groups names which were included into snapshot. */
    private final String cacheGrps;

    /**
     * @param name Snapshot name.
     * @param nodeId Node consistent id.
     * @param baselineNodes Baseline nodes affected by snapshots.
     * @param cacheGrps The cache groups names which were included into snapshot.
     */
    public SnapshotView(
        String name,
        String nodeId,
        String baselineNodes,
        String cacheGrps
    ) {
        this.name = name;
        this.nodeId = nodeId;
        this.baselineNodes = baselineNodes;
        this.cacheGrps = cacheGrps;
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
     * @return Baseline nodes affected by snapshots.
     */
    @Order(2)
    @Filtrable
    public String baselineNodes() {
        return baselineNodes;
    }

    /**
     * @return The cache groups names which were included into snapshot.
     */
    @Order(3)
    public String cacheGroups() {
        return cacheGrps;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        SnapshotView view = (SnapshotView)o;
        return name.equals(view.name)
            && nodeId.equals(view.nodeId)
            && baselineNodes.equals(view.baselineNodes)
            && cacheGrps.equals(view.cacheGrps);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return Objects.hash(name, nodeId, baselineNodes, cacheGrps);
    }
}
