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

package org.apache.ignite.internal.pagemem.wal.record;

import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Partition clearing started record.
 * Used to repeat clearing if node was stopped without checkpoint after clearing on a rebalance.
 */
public class PartitionClearingStartRecord extends WALRecord {
    /** Partition ID. */
    private int partId;

    /** Cache group ID. */
    private int grpId;

    /** Clear version. */
    private long clearVer;

    /**
     * @param partId Partition ID.
     * @param grpId Cache group ID.
     * @param clearVer Clear version.
     */
    public PartitionClearingStartRecord(int partId, int grpId, long clearVer) {
        this.partId = partId;
        this.grpId = grpId;
        this.clearVer = clearVer;
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

    /**
     * @return Cache group ID.
     */
    public int groupId() {
        return grpId;
    }

    /**
     * @param grpId Cache group ID.
     */
    public void groupId(int grpId) {
        this.grpId = grpId;
    }

    /**
     * @return Clear version.
     */
    public long clearVersion() {
        return clearVer;
    }

    /**
     * @param clearVer Clear version.
     */
    public void clearVersion(long clearVer) {
        this.clearVer = clearVer;
    }

    /** {@inheritDoc} */
    @Override public RecordType type() {
        return RecordType.PARTITION_CLEARING_START_RECORD;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(PartitionClearingStartRecord.class, this, "super", super.toString());
    }
}
