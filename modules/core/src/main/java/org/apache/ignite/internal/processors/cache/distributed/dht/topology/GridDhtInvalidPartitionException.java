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

/**
 * Exception thrown whenever entry is created for invalid partition.
 */
public class GridDhtInvalidPartitionException extends RuntimeException {
    /** */
    private static final long serialVersionUID = 0L;

    /** Partition. */
    private final int part;

    /**
     * @param part Partition.
     * @param msg Message.
     */
    public GridDhtInvalidPartitionException(int part, String msg) {
        super(msg);

        this.part = part;
    }

    /**
     * @return Partition.
     */
    public int partition() {
        return part;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return getClass() + " [part=" + part + ", msg=" + getMessage() + ']';
    }
}