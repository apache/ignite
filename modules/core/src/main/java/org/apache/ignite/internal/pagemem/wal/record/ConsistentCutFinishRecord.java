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

import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.ignite.internal.processors.cache.consistentcut.ConsistentCutFuture;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.lang.IgniteUuid;

/**
 * This record is written to WAL after it's finished to check transactions in {@link ConsistentCutFuture} and store them
 * in a particular collection - {@link #before()} or {@link #after()}.
 * <p>
 * It guarantees that the BEFORE side consists of:
 * 1. transactions physically committed before {@link ConsistentCutStartRecord} and weren't included into {@link #after()};
 * 2. transactions physically committed between {@link ConsistentCutStartRecord} and {@link ConsistentCutFinishRecord}
 *    and were included into {@link #before()}.
 * <p>
 * It guarantees that the AFTER side consists of:
 * 1. transactions physically committed before {@link ConsistentCutStartRecord} and were included into {@link #after()};
 * 2. transactions physically committed between {@link ConsistentCutStartRecord} and {@link ConsistentCutFinishRecord}
 *    and weren't included into {@link #before()}.
 */
public class ConsistentCutFinishRecord extends WALRecord {
    /** ID of Consistent Cut. */
    private final UUID cutId;

    /**
     * Set of transactions committed between {@link ConsistentCutStartRecord} and {@link ConsistentCutFinishRecord}
     * to include to the BEFORE side of Consistent Cut.
     */
    private final Set<GridCacheVersion> before;

    /**
     * Set of transactions committed before {@link ConsistentCutStartRecord} to include to the AFTER side of Consistent Cut.
     */
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

    /** {@inheritDoc} */
    @Override public String toString() {
        // Dump IgniteUuid as it more convenient for debug purposes than GridCacheVersion.
        List<IgniteUuid> txBefore = before.stream()
            .map(GridCacheVersion::asIgniteUuid)
            .collect(Collectors.toList());

        List<IgniteUuid> txAfter = after.stream()
            .map(GridCacheVersion::asIgniteUuid)
            .collect(Collectors.toList());

        return "ConsistentCutFinishRecord [before=" + txBefore + ", after=" + txAfter + "]";
    }
}
