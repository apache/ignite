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

package org.apache.ignite.internal.pagemem.wal.record.delta;

import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.pagemem.wal.record.WalRecordCacheGroupAware;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 *
 */
public class PartitionMetaStateRecord extends WALRecord implements WalRecordCacheGroupAware {
    /** State. */
    private final byte state;

    /** Cache group ID. */
    private final int grpId;

    /** Partition id. */
    private final int partId;

    /** Update counter. */
    private final long updateCounter;

    /**
     * @param grpId Cache group ID.
     * @param state Page ID.
     */
    public PartitionMetaStateRecord(int grpId, int partId, GridDhtPartitionState state, long updateCounter) {
        this.grpId = grpId;
        this.partId = partId;
        this.state = (byte)state.ordinal();
        this.updateCounter = updateCounter;
    }

    /** {@inheritDoc} */
    @Override public RecordType type() {
        return WALRecord.RecordType.PART_META_UPDATE_STATE;
    }

    /**
     *
     */
    public byte state() {
        return state;
    }

    /** {@inheritDoc} */
    @Override public int groupId() {
        return grpId;
    }

    /**
     *
     */
    public int partitionId() {
        return partId;
    }

    /**
     *
     */
    public long updateCounter() {
        return updateCounter;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(PartitionMetaStateRecord.class, this, "super", super.toString());
    }
}
