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

import java.util.Collection;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Recover rebuild markers of index partitions.
 */
public class StartBuildIndexRecord extends TimeStampRecord {
    /** Cache group id. */
    @GridToStringInclude
    private final int grpId;

    /** Cache id. */
    @GridToStringInclude
    private final int cacheId;

    /** Partitions. */
    @GridToStringInclude
    private Collection<Integer> partIds;

    /**
     *
     * @param grpId Cache group ID.
     * @param cacheId Cache ID.
     * @param partIds Partitions.
     */
    public StartBuildIndexRecord(int grpId, int cacheId, Collection<Integer> partIds) {
        this.grpId = grpId;
        this.cacheId = cacheId;
        this.partIds = partIds;
    }

    /** {@inheritDoc} */
    @Override public RecordType type() {
        return RecordType.START_BUILD_INDEX_RECORD;
    }

    /**
     *
     * @return Cache group ID.
     */
    public int groupId() { return grpId; }

    /**
     *
     * @return Cache ID.
     */
    public int cacheId() { return cacheId; }

    /**
     *
     * @return Partitions.
     */
    public Collection<Integer> partIds() {
        return partIds;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(StartBuildIndexRecord.class, this, "super", super.toString());
    }
}
