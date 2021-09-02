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

package org.apache.ignite.internal.processors.cache.distributed.dht.preloader;

import java.util.UUID;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Information about supplier for specific partition.
 */
public class SupplyPartitionInfo {
    /**
     * Number of partiiton.
     */
    private int part;

    /**
     * Counter of partiotn from demander node.
     */
    private long minCntr;

    /**
     * Reservation.
     */
    private long maxReserved;

    /**
     * Node with the moust deepest history by partiton.
     */
    private UUID maxReservedNodeId;

    /**
     * @param part Partiiotn.
     * @param minCntr Minimal counter.
     * @param maxReserved Max reservation.
     * @param maxReservedNodeId Node with maximum reservation.
     */
    public SupplyPartitionInfo(int part, long minCntr, long maxReserved, UUID maxReservedNodeId) {
        this.part = part;
        this.minCntr = minCntr;
        this.maxReserved = maxReserved;
        this.maxReservedNodeId = maxReservedNodeId;
    }

    /**
     * @return Partition.
     */
    public int part() {
        return part;
    }

    /**
     * @return Minimum counter.
     */
    public long minCntr() {
        return minCntr;
    }

    /**
     * @return Max reservation.
     */
    public long maxReserved() {
        return maxReserved;
    }

    /**
     * @return Node id.
     */
    public UUID maxReservedNodeId() {
        return maxReservedNodeId;
    }

    /**
     * @return True if reserved, false otherwise.
     */
    public boolean isHistoryReserved() {
        return maxReserved != Long.MAX_VALUE && maxReservedNodeId != null;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(SupplyPartitionInfo.class, this);
    }
}
