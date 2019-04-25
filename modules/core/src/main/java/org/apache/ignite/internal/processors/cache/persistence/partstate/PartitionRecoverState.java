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

package org.apache.ignite.internal.processors.cache.persistence.partstate;

/**
 * Class holds state of partition during recovery process.
 */
public class PartitionRecoverState {
    /** State id. */
    private final int stateId;

    /** Update counter. */
    private final long updateCounter;

    /**
     * @param stateId State id.
     * @param updateCounter Update counter.
     */
    public PartitionRecoverState(int stateId, long updateCounter) {
        this.stateId = stateId;
        this.updateCounter = updateCounter;
    }

    /**
     *
     */
    public int stateId() {
        return stateId;
    }

    /**
     *
     */
    public long updateCounter() {
        return updateCounter;
    }
}
