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

import java.util.UUID;
import org.apache.ignite.internal.managers.systemview.walker.Filtrable;
import org.apache.ignite.internal.managers.systemview.walker.Order;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState;

/**
 * Partition state representation for a {@link SystemView}.
 */
public class PartitionStateView {
    /** Cache group id. */
    private final int cacheGrpId;

    /** Node id. */
    private final UUID nodeId;

    /** Partition id. */
    private final int partId;

    /** Partition state. */
    private final GridDhtPartitionState state;

    /** Is primary partition. */
    private final boolean primary;

    /**
     * @param cacheGrpId Cache group id.
     * @param nodeId Node id.
     * @param partId Partition id.
     * @param state Partition state.
     * @param primary Is primary partition for node.
     */
    public PartitionStateView(int cacheGrpId, UUID nodeId, int partId, GridDhtPartitionState state, boolean primary) {
        this.cacheGrpId = cacheGrpId;
        this.nodeId = nodeId;
        this.partId = partId;
        this.state = state;
        this.primary = primary;
    }

    /** @return Cache group id. */
    @Order
    @Filtrable
    public int cacheGroupId() {
        return cacheGrpId;
    }

    /** @return Node id. */
    @Order(1)
    @Filtrable
    public UUID nodeId() {
        return nodeId;
    }

    /** @return Partition id. */
    @Order(2)
    @Filtrable
    public int partitionId() {
        return partId;
    }

    /** @return Partition state. */
    @Order(3)
    public String state() {
        return state.name();
    }

    /** @return Is primary partition. */
    @Order(4)
    public boolean isPrimary() {
        return primary;
    }
}
