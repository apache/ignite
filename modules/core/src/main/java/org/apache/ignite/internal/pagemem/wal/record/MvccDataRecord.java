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
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Logical data record with cache operation description.
 * This record contains information about operation we want to do.
 * Contains operation type (put, remove) and (Key, Value, Version) for each {@link MvccDataEntry}
 */
public class MvccDataRecord extends DataRecord {
    /** {@inheritDoc} */
    @Override public RecordType type() {
        return RecordType.MVCC_DATA_RECORD;
    }

    /**
     * @param writeEntry Write entry.
     */
    public MvccDataRecord(MvccDataEntry writeEntry) {
        this(writeEntry, U.currentTimeMillis());
    }

    /**
     * @param writeEntries Write entries.
     */
    public MvccDataRecord(List<DataEntry> writeEntries) {
        this(writeEntries, U.currentTimeMillis());
    }

    /**
     * @param writeEntry Write entry.
     */
    public MvccDataRecord(MvccDataEntry writeEntry, long timestamp) {
        this(Collections.singletonList(writeEntry), timestamp);
    }

    /**
     * @param writeEntries Write entries.
     * @param timestamp TimeStamp.
     */
    public MvccDataRecord(List<DataEntry> writeEntries, long timestamp) {
        super(writeEntries, timestamp);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(MvccDataRecord.class, this, "super", super.toString());
    }
}
