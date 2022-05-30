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
 * This WAL record represents moment when Consistent Cut was taken on a local node. This record splits whole WAL on
 * two areas (BEFORE and AFTER). It's guaranteed that every transaction committed BEFORE will be committed by the same
 * Consistent Cut version on every other node. It means that an Ignite node can safely recover itself to this point
 * without any coordination with other nodes.
 *
 * Some of incomplete transactions (committed AFTER) could be a part of Global Consistent State. It means that those
 * transactions could be committed BEFORE on other Ignite nodes. Set of such transactions is prepared in moment of taken
 * Consistent Cut. Ignite analyzes such transactions and decided whether to include them to the state or not.
 * Information about that is written to WAL with {@link ConsistentCutFinishRecord}.
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

    /**
     * Set of transactions (committed AFTER this record) to check whether they are part of the Consistent Cut State.
     */
    private final Set<GridCacheVersion> check;

    /** */
    public ConsistentCutStartRecord(long ver, Set<GridCacheVersion> include, Set<GridCacheVersion> check) {
        this.ver = ver;
        this.include = include;
        this.check = check;
    }

    /** */
    public long version() {
        return ver;
    }

    /** */
    public Set<GridCacheVersion> include() {
        return include;
    }

    /** */
    public Set<GridCacheVersion> check() {
        return check;
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

        List<IgniteUuid> chk = check.stream()
            .map(GridCacheVersion::asIgniteUuid)
            .collect(Collectors.toList());

        return "ConsistentCutRecord [ver=" + version() + ";  include=" + incl + "; check=" + chk + "]";
    }
}
