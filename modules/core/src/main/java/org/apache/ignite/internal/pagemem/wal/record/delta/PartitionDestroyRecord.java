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
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 *
 */
public class PartitionDestroyRecord extends WALRecord implements WalRecordCacheGroupAware {
    /** */
    private int grpId;

    /** */
    private int partId;

    /**
     * @param grpId Cache group ID.
     * @param partId Partition ID.
     */
    public PartitionDestroyRecord(int grpId, int partId) {
        this.grpId = grpId;
        this.partId = partId;
    }

    /** {@inheritDoc} */
    @Override public RecordType type() {
        return RecordType.PARTITION_DESTROY;
    }

    /** {@inheritDoc} */
    @Override public int groupId() {
        return grpId;
    }

    /**
     * @param grpId Cache group ID.
     */
    public void groupId(int grpId) {
        this.grpId = grpId;
    }

    /**
     * @return Partition ID.
     */
    public int partitionId() {
        return partId;
    }

    /**
     * @param partId Partition ID.
     */
    public void partitionId(int partId) {
        this.partId = partId;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(PartitionDestroyRecord.class, this, "super", super.toString());
    }
}
