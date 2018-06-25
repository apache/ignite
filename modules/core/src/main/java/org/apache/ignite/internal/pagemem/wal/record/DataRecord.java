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
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Logical data record with cache operation description.
 * This record contains information about operation we want to do.
 * Contains operation type (put, remove) and (Key, Value, Version) for each {@link DataEntry}
 */
public class DataRecord extends TimeStampRecord implements WalEncryptedRecord {
    /** */
    @GridToStringInclude
    private List<DataEntry> writeEntries;

    /** If {@code true} this record should be encrypted. */
    private boolean needEncryption = false;

    /** Group id that will be used for a record encryption. */
    private int grpId;

    /** {@inheritDoc} */
    @Override public RecordType type() {
        return RecordType.DATA_RECORD;
    }

    /**
     *
     */
    private DataRecord() {
        // No-op, used from builder methods.
    }

    /**
     * @param writeEntry Write entry.
     * @param needEncryption If {@code true} this record should be encrypted.
     * @param grpId Group id.
     */
    public DataRecord(DataEntry writeEntry, boolean needEncryption, int grpId) {
        this(writeEntry, U.currentTimeMillis(), needEncryption, grpId);
    }

    /**
     * @param writeEntries Write entries.
     * @param needEncryption If {@code true} this record should be encrypted.
     * @param grpId Group id.
     */
    public DataRecord(List<DataEntry> writeEntries, boolean needEncryption, int grpId) {
        this(writeEntries, U.currentTimeMillis(), needEncryption, grpId);
    }

    /**
     * @param writeEntry Write entry.
     * @param timestamp Timestamp.
     * @param needEncryption If {@code true} this record should be encrypted.
     * @param grpId Group id that will be used for a record encryption.
     */
    public DataRecord(DataEntry writeEntry, long timestamp, boolean needEncryption, int grpId) {
        this(Collections.singletonList(writeEntry), timestamp, needEncryption, grpId);
    }

    /**
     * @param writeEntries Write entries.
     * @param timestamp TimeStamp.
     * @param needEncryption If {@code true} this record should be encrypted.
     * @param grpId Group id.
     */
    public DataRecord(List<DataEntry> writeEntries, long timestamp, boolean needEncryption, int grpId) {
        super(timestamp);

        this.writeEntries = writeEntries;
        this.needEncryption = needEncryption;
        this.grpId = grpId;
    }

    /**
     * @return Collection of write entries.
     */
    public List<DataEntry> writeEntries() {
        return writeEntries == null ? Collections.<DataEntry>emptyList() : writeEntries;
    }

    /** {@inheritDoc} */
    @Override public boolean needEncryption() {
        return needEncryption;
    }

    /** {@inheritDoc} */
    @Override public int groupId() {
        return grpId;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(DataRecord.class, this, "super", super.toString());
    }
}
