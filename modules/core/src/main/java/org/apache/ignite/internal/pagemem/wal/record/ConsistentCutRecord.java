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

/** */
public class ConsistentCutRecord extends WALRecord {
    /** */
    private final long consistentCutVer;

    /**
     * Exclusion flag, it marks the second CutRecord for closing ConsistentCut. It contains list of TXs to exclude
     * from the first CutRecord.
     */
    private final boolean finish;

    /**
     * List of TXs to include to ConsistentCut that should be parsed after the first CutRecord.
     */
    private final Set<GridCacheVersion> include;

    /**
     * List of TXs to include to ConsistentCut that should be parsed after the first CutRecord.
     */
    private final Set<GridCacheVersion> check;

    /** */
    public ConsistentCutRecord(
        long consistentCutVer,
        Set<GridCacheVersion> include,
        Set<GridCacheVersion> check,
        boolean finish
    ) {
        this.consistentCutVer = consistentCutVer;
        this.include = include;
        this.check = check;
        this.finish = finish;
    }

    /** */
    public long ver() {
        return consistentCutVer;
    }

    /** */
    public Set<GridCacheVersion> include() {
        return include;
    }

    /** */
    public Set<GridCacheVersion> check() {
        return check;
    }

    /** */
    public boolean finish() {
        return finish;
    }

    /** {@inheritDoc} */
    @Override public RecordType type() {
        return RecordType.CONSISTENT_CUT_RECORD;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        List<IgniteUuid> incl = include.stream()
            .map(GridCacheVersion::asIgniteUuid)
            .collect(Collectors.toList());

        List<IgniteUuid> chk = check.stream()
            .map(GridCacheVersion::asIgniteUuid)
            .collect(Collectors.toList());

        return "ConsistentCutRecord [ver=" + ver() + "; finish=" + finish + ";  include=" + incl + "; check=" + chk + "]";
    }
}
