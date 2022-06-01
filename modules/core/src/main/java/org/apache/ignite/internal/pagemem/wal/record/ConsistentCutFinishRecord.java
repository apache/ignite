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

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.ignite.internal.processors.cache.consistentcut.ConsistentCutState;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.lang.IgniteUuid;

/**
 * Consistent Cut splits timeline on 2 global areas - BEFORE and AFTER. It guarantees that every transaction committed BEFORE
 * also will be committed BEFORE on every other node. It means that an Ignite node can safely recover itself to this
 * point without any coordination with other nodes.
 *
 * This record is written to WAL in moment when Consistent Cut stops analyzing transactions from {@link ConsistentCutState#checkList()}.
 * It guarantees that every transaction included to {@link #include()} are part of the global BEFORE state, and every
 * committed transaction since {@link ConsistentCutStartRecord} and that aren't included into {@link #include()} are
 * part of the global AFTER state.
 *
 * @see ConsistentCutStartRecord
 * @see ConsistentCutState
 */
public class ConsistentCutFinishRecord extends WALRecord {
    /**
     * Set of transactions (optionally committed after this record) to include to the Consistent Cut State.
     */
    private final Set<GridCacheVersion> include;

    /** */
    public ConsistentCutFinishRecord(Set<GridCacheVersion> include) {
        this.include = new HashSet<>(include);
    }

    /** */
    public Set<GridCacheVersion> include() {
        return include;
    }

    /** {@inheritDoc} */
    @Override public RecordType type() {
        return RecordType.CONSISTENT_CUT_FINISH_RECORD;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        List<IgniteUuid> incl = include.stream()
            .map(GridCacheVersion::asIgniteUuid)
            .collect(Collectors.toList());

        return "ConsistentCutRecord [include=" + incl + "]";
    }
}
