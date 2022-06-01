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
import java.util.stream.Collectors;
import org.apache.ignite.internal.processors.cache.consistentcut.ConsistentCutState;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.lang.IgniteUuid;

/**
 * Consistent Cut splits timeline on 2 global areas - BEFORE and AFTER. It guarantees that every transaction committed BEFORE
 * also will be committed BEFORE on every other node. It means that an Ignite node can safely recover itself to this
 * point without any coordination with other nodes.
 *
 * This record is written to WAL in moment when Consistent Cut starts on a local node. All transactions committed before
 * this WAL record are part of the global area BEFORE. But it's possible then some transactions committed after this
 * record are also part of the BEFORE state. Set of such transactions is prepared in moment of taken Consistent Cut and
 * stored within {@link ConsistentCutState#checkList()}. Ignite analyzes such transactions and decided whether to include
 * them to the state or not. Information about that is written to WAL with {@link ConsistentCutFinishRecord}.
 *
 * @see ConsistentCutFinishRecord
 * @see ConsistentCutState
 */
public class ConsistentCutStartRecord extends WALRecord {
    /**
     * Consistent Cut Version. It's timestamp of start Consistent Cut on the Ignite coordinator node.
     */
    private final long ver;

    /**
     * Set of transactions (committed AFTER this record) to include to the Consistent Cut State.
     */
    private final Set<GridCacheVersion> include;

    /** */
    public ConsistentCutStartRecord(long ver, Set<GridCacheVersion> include) {
        this.ver = ver;
        this.include = include;
    }

    /** */
    public long version() {
        return ver;
    }

    /** */
    public Set<GridCacheVersion> include() {
        return include;
    }

    /** {@inheritDoc} */
    @Override public RecordType type() {
        return RecordType.CONSISTENT_CUT_START_RECORD;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        List<IgniteUuid> incl = include.stream()
            .map(GridCacheVersion::asIgniteUuid)
            .collect(Collectors.toList());

        return "ConsistentCutRecord [ver=" + version() + ";  include=" + incl + "]";
    }
}
