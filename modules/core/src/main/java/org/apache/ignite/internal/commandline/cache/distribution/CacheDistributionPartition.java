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

package org.apache.ignite.internal.commandline.cache.distribution;

import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState;

/**
 * DTO for CacheDistributionTask, contains information about partition
 */
public class CacheDistributionPartition extends IgniteDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Partition identifier. */
    @Order(value = 0)
    int partId;

    /** Flag primary or backup partition. */
    @Order(value = 1)
    boolean primary;

    /** Partition status. */
    @Order(value = 2)
    GridDhtPartitionState state;

    /** Partition update counters. */
    @Order(value = 3)
    long updateCntr;

    /** Number of entries in partition. */
    @Order(value = 4)
    long size;

    /** Default constructor. */
    public CacheDistributionPartition() {
    }

    /**
     * @param partId Partition identifier.
     * @param primary Flag primary or backup partition.
     * @param state Partition status.
     * @param updateCntr Partition update counters.
     * @param size Number of entries in partition.
     */
    public CacheDistributionPartition(int partId, boolean primary,
        GridDhtPartitionState state, long updateCntr, long size) {
        this.partId = partId;
        this.primary = primary;
        this.state = state;
        this.updateCntr = updateCntr;
        this.size = size;
    }

    /** */
    public int getPartition() {
        return partId;
    }

    /** */
    public void setPartition(int partId) {
        this.partId = partId;
    }

    /** */
    public boolean isPrimary() {
        return primary;
    }

    /** */
    public void setPrimary(boolean primary) {
        this.primary = primary;
    }

    /** */
    public GridDhtPartitionState getState() {
        return state;
    }

    /** */
    public void setState(GridDhtPartitionState state) {
        this.state = state;
    }

    /** */
    public long getUpdateCounter() {
        return updateCntr;
    }

    /** */
    public void setUpdateCounter(long updateCntr) {
        this.updateCntr = updateCntr;
    }

    /** */
    public long getSize() {
        return size;
    }

    /** */
    public void setSize(long size) {
        this.size = size;
    }
}
