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

import java.util.Set;
import java.util.UUID;
import org.apache.ignite.internal.processors.cache.consistentcut.BaselineConsistentCut;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.CacheVersionIO;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * This record is written to WAL after {@link BaselineConsistentCut} finished checking transactions and stored them
 * in a particular set - {@link #before()} or {@link #after()}.
 * <p>
 * It guarantees that the BEFORE side consists of:
 * 1. Transactions committed before {@link ConsistentCutStartRecord} and weren't included into {@link #after()}.
 * 2. Transactions committed between {@link ConsistentCutStartRecord} and {@link ConsistentCutFinishRecord}
 *    and were included into {@link #before()}.
 * <p>
 * It guarantees that the AFTER side consists of:
 * 1. Transactions committed before {@link ConsistentCutStartRecord} and were included into {@link #after()}.
 * 2. Transactions committed between {@link ConsistentCutStartRecord} and {@link ConsistentCutFinishRecord}
 *    and weren't included into {@link #before()}.
 */
public class ConsistentCutFinishRecord extends WALRecord {
    /** Consistent Cut ID. */
    @GridToStringInclude
    private final UUID cutId;

    /**
     * Set of transactions committed between {@link ConsistentCutStartRecord} and {@link ConsistentCutFinishRecord}
     * to include to the BEFORE side of Consistent Cut.
     */
    @GridToStringInclude
    private final Set<GridCacheVersion> before;

    /**
     * Set of transactions committed before {@link ConsistentCutStartRecord} to include to the AFTER side of Consistent Cut.
     */
    @GridToStringInclude
    private final Set<GridCacheVersion> after;

    /** */
    public ConsistentCutFinishRecord(UUID cutId, Set<GridCacheVersion> before, Set<GridCacheVersion> after) {
        this.cutId = cutId;
        this.before = before;
        this.after = after;
    }

    /** */
    public Set<GridCacheVersion> before() {
        return before;
    }

    /** */
    public Set<GridCacheVersion> after() {
        return after;
    }

    /** */
    public UUID cutId() {
        return cutId;
    }

    /** {@inheritDoc} */
    @Override public RecordType type() {
        return RecordType.CONSISTENT_CUT_FINISH_RECORD;
    }

    /**
     * Calculating the size of the record.
     *
     * @return Size in bytes.
     */
    public int dataSize() {
        int size = 8 + 8 + 4 + 4;  // ID, before and after tx count.

        for (GridCacheVersion v: before)
            size += CacheVersionIO.size(v, false);

        for (GridCacheVersion v: after)
            size += CacheVersionIO.size(v, false);

        return size;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ConsistentCutFinishRecord.class, this);
    }
}
