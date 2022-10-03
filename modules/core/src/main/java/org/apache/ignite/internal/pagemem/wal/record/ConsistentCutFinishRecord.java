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

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.ignite.internal.processors.cache.consistentcut.ConsistentCut;
import org.apache.ignite.internal.processors.cache.consistentcut.ConsistentCutManager;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.lang.IgniteUuid;

/**
 * {@link ConsistentCut} splits timeline on 2 global areas - BEFORE and AFTER. It guarantees that every transaction committed
 * BEFORE also will be committed BEFORE on every other node. It means that an Ignite node can safely recover itself to this
 * point without any coordination with other nodes.
 * <p>
 * This record is written to WAL after Consistent Cut stopped analyzing transactions from {@link ConsistentCut} and
 * storing them in a particular collection - {@link #before()} or {@link #after()}.
 * <p>
 * It guarantees that the BEFORE side consist of:
 * 1. transactions physically committed before {@link ConsistentCutStartRecord} and weren't included into {@link #after()};
 * 2. transactions physically committed between {@link ConsistentCutStartRecord} and {@link ConsistentCutFinishRecord}
 *    and were included into {@link #before()}.
 * <p>
 * It guarantees that the AFTER side consist of:
 * 1. transactions physically committed before {@link ConsistentCutStartRecord} and were included into {@link #after()};
 * 2. transactions physically committed after {@link ConsistentCutStartRecord} and weren't included into {@link #before()}.
 *
 * @see ConsistentCutManager
 */
public class ConsistentCutFinishRecord extends WALRecord {
    /**
     * Set of transactions to include to the BEFORE side of Consistent Cut.
     */
    private final Set<GridCacheVersion> before;

    /**
     * Set of transactions to include to the AFTER side of Consistent Cut.
     */
    private final Set<GridCacheVersion> after;

    /** */
    public ConsistentCutFinishRecord(Set<GridCacheVersion> before, Set<GridCacheVersion> after) {
        this.before = Collections.unmodifiableSet(before);
        this.after = Collections.unmodifiableSet(after);
    }

    /** */
    public Set<GridCacheVersion> before() {
        return before;
    }

    /** */
    public Set<GridCacheVersion> after() {
        return after;
    }

    /** {@inheritDoc} */
    @Override public RecordType type() {
        return RecordType.CONSISTENT_CUT_FINISH_RECORD;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        List<IgniteUuid> incl = before.stream()
            .map(GridCacheVersion::asIgniteUuid)
            .collect(Collectors.toList());

        List<IgniteUuid> excl = after.stream()
            .map(GridCacheVersion::asIgniteUuid)
            .collect(Collectors.toList());

        return "ConsistentCutFinishRecord [before=" + incl + ", after=" + excl + "]";
    }
}
