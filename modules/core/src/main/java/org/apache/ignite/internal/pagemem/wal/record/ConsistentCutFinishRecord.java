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
import org.apache.ignite.internal.processors.cache.persistence.tree.io.CacheVersionIO;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * This record is written to WAL after Consistent Cut finished on a baseline node.
 * <p>
 * During recovery node must apply:
 * 1. Transactions committed before {@link ConsistentCutStartRecord} except those contained in {@link #excluded()}.
 * 2. Transactions committed between {@link ConsistentCutStartRecord} and {@link ConsistentCutFinishRecord}
 *    and contained in {@link #included()}.
 */
public class ConsistentCutFinishRecord extends WALRecord {
    /** Consistent Cut ID. */
    @GridToStringInclude
    private final UUID id;

    /**
     * Set of transactions committed between {@link ConsistentCutStartRecord} and {@link ConsistentCutFinishRecord}
     * to include into the Consistent Cut.
     */
    @GridToStringInclude
    private final Set<GridCacheVersion> included;

    /**
     * Set of transactions committed before {@link ConsistentCutStartRecord} to exclude from the Consistent Cut.
     */
    @GridToStringInclude
    private final Set<GridCacheVersion> excluded;

    /** */
    public ConsistentCutFinishRecord(UUID id, Set<GridCacheVersion> included, Set<GridCacheVersion> excluded) {
        this.id = id;
        this.included = included;
        this.excluded = excluded;
    }

    /** */
    public Set<GridCacheVersion> included() {
        return included;
    }

    /** */
    public Set<GridCacheVersion> excluded() {
        return excluded;
    }

    /** */
    public UUID id() {
        return id;
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
        int size = 16 + 4 + 4;  // ID, included and excluded tx count.

        for (GridCacheVersion v: included)
            size += CacheVersionIO.size(v, false);

        for (GridCacheVersion v: excluded)
            size += CacheVersionIO.size(v, false);

        return size;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ConsistentCutFinishRecord.class, this);
    }
}
