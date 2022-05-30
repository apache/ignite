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
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.lang.IgniteUuid;

/**
 * This WAL record contains a set of transactions to be included after checking {@link ConsistentCutStartRecord#check()}.
 */
public class ConsistentCutFinishRecord extends WALRecord {
    /**
     * Set of transactions (committed AFTER this record) to include to the Consistent Cut State.
     */
    private final Set<GridCacheVersion> include;

    /** */
    public ConsistentCutFinishRecord(Set<GridCacheVersion> include) {
        this.include = include;
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
