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

import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Record
 */
public class StartBuildIndexRecord extends TimeStampRecord {
    /** Cache id. */
    @GridToStringInclude
    private final int cacheId;

    /** Cache group id. */
    @GridToStringInclude
    private final int grpId;

    public StartBuildIndexRecord(int cacheId, int grpId) {
        this.cacheId = cacheId;
        this.grpId = grpId;
    }

    /** {@inheritDoc} */
    @Override public RecordType type() { return RecordType.START_BUILD_INDEX_RECORD; }

    public int cacheId() { return cacheId; }

    public int groupId() { return grpId; }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(StartBuildIndexRecord.class, this, "super", super.toString());
    }
}