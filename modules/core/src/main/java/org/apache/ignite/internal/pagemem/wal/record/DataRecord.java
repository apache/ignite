/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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
public class DataRecord extends TimeStampRecord {
    /** */
    @GridToStringInclude
    private List<DataEntry> writeEntries;

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
     */
    public DataRecord(DataEntry writeEntry) {
        this(writeEntry, U.currentTimeMillis());
    }

    /**
     * @param writeEntries Write entries.
     */
    public DataRecord(List<DataEntry> writeEntries) {
        this(writeEntries, U.currentTimeMillis());
    }

    /**
     * @param writeEntry Write entry.
     */
    public DataRecord(DataEntry writeEntry, long timestamp) {
        this(Collections.singletonList(writeEntry), timestamp);
    }

    /**
     * @param writeEntries Write entries.
     * @param timestamp TimeStamp.
     */
    public DataRecord(List<DataEntry> writeEntries, long timestamp) {
        super(timestamp);

        this.writeEntries = writeEntries;
    }

    /**
     * @param writeEntries Write entries.
     * @return {@code this} for chaining.
     */
    public DataRecord setWriteEntries(List<DataEntry> writeEntries) {
        this.writeEntries = writeEntries;

        return this;
    }

    /**
     * @return Collection of write entries.
     */
    public List<DataEntry> writeEntries() {
        return writeEntries == null ? Collections.<DataEntry>emptyList() : writeEntries;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(DataRecord.class, this, "super", super.toString());
    }
}
