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

import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxEntry;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 *
 */
public class DataRecord extends WALRecord {
    /** */
    private List<DataEntry> writeEntries;

    /** {@inheritDoc} */
    @Override public RecordType type() {
        return RecordType.DATA_RECORD;
    }

    /**
     * @param tx Transaction to build WAL record from.
     * @return WAL data record.
     */
    public static DataRecord fromTransaction(IgniteInternalTx tx) {
        Collection<IgniteTxEntry> writes = tx.writeEntries();

        List<DataEntry> entries = new ArrayList<>(writes.size());

        for (IgniteTxEntry write : writes)
            entries.add(DataEntry.fromTxEntry(write, tx));

        return new DataRecord(entries);
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
        this(Collections.singletonList(writeEntry));
    }

    /**
     * @param writeEntries Write entries.
     */
    public DataRecord(List<DataEntry> writeEntries) {
        this.writeEntries = writeEntries;
    }

    /**
     * @return Collection of write entries.
     */
    public List<DataEntry> writeEntries() {
        return writeEntries == null ? Collections.<DataEntry>emptyList() : writeEntries;
    }
}
