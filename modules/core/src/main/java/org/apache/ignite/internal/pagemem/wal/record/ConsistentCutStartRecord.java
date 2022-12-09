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

import java.util.UUID;
import org.apache.ignite.internal.processors.cache.consistentcut.BaselineConsistentCut;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * This record is written to WAL at the moment when {@link BaselineConsistentCut} starts on a local node.
 */
public class ConsistentCutStartRecord extends WALRecord {
    /** Consistent Cut ID. */
    @GridToStringInclude
    private final UUID cutId;

    /** */
    public ConsistentCutStartRecord(UUID cutId) {
        this.cutId = cutId;
    }

    /** */
    public UUID cutId() {
        return cutId;
    }

    /** {@inheritDoc} */
    @Override public RecordType type() {
        return RecordType.CONSISTENT_CUT_START_RECORD;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ConsistentCutStartRecord.class, this);
    }
}
