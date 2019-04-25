/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.distributed.dht.topology;

import org.jetbrains.annotations.Nullable;

/**
 * Partition states.
 */
public enum GridDhtPartitionState {
    /** Partition is being loaded from another node. */
    MOVING,

    /** This node is either a primary or backup owner. */
    OWNING,

    /** This node is neither primary or back up owner. */
    RENTING,

    /** Partition has been evicted from cache. */
    EVICTED,

    /** Partition state is invalid, partition should not be used. */
    LOST;

    /** Enum values. */
    private static final GridDhtPartitionState[] VALS = values();

    /**
     * @param ord Ordinal value.
     * @return Enum value.
     */
    @Nullable public static GridDhtPartitionState fromOrdinal(int ord) {
        return ord < 0 || ord >= VALS.length ? null : VALS[ord];
    }

    /**
     * @return {@code True} if state is active or owning.
     */
    public boolean active() {
        return this != EVICTED;
    }
}
