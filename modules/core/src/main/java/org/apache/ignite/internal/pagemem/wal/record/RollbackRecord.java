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

import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Rollback record. Used to close gap in partition update sequence on tx rollback.
 */
public class RollbackRecord extends WALRecord {
    /** Cache ID. */
    @GridToStringInclude
    protected int grpId;

    /** Partition ID. */
    @GridToStringInclude
    protected int partId;

    /** Rollback start. */
    @GridToStringInclude
    protected long start;

    /** Rollback range. */
    @GridToStringInclude
    protected long range;

    /**
     * @param grpId Group id.
     * @param partId Partition id.
     * @param start Start.
     * @param range Range.
     */
    public RollbackRecord(int grpId, int partId, long start, long range) {
        this.grpId = grpId;
        this.partId = partId;
        this.start = start;
        this.range = range;
    }

    /**
     * @return Cache ID.
     */
    public int groupId() {
        return grpId;
    }

    /**
     * @return Partition ID.
     */
    public int partitionId() {
        return partId;
    }

    /**
     * @return Rollback start.
     */
    public long start() {
        return start;
    }

    /**
     * @return Rollback range.
     */
    public long range() {
        return range;
    }

    /**
     * Returns a number of overlapping update counters.
     *
     * @param from From counter (not inclusive).
     * @param to To counter (inclusive).
     */
    public long overlap(long from, long to) {
        long to0 = start + range;

        // from lies within (start, to0]
        if (start <= from && from < to0)
            return Math.min(to0 - from, to - from);

        // start lies within (from, to]
        if (from <= start && start < to)
            return Math.min(to - start, range);

        return 0;
    }



    /** {@inheritDoc} */
    @Override public RecordType type() {
        return RecordType.ROLLBACK_TX_RECORD;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(RollbackRecord.class, this);
    }
}
