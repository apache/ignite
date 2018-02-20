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

package org.apache.ignite.internal.processors.cache.persistence.wal.link;

import org.apache.ignite.internal.pagemem.wal.record.WALReferenceAwareRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.DataPageInsertFragmentReferencedRecord;
import org.apache.ignite.internal.processors.cache.persistence.Storable;

/**
 * Class to store and link row to {@link WALReferenceAwareRecord} records.
 */
public class RowDataHolder {
    /** Linking row. */
    private final Storable row;

    /** Linking row size. */
    private final int rowSize;

    /**
     * Create holder with specified {@code row} and {@code rowSize}.
     *
     * @param row Row to link to {@link WALReferenceAwareRecord} records.
     * @param rowSize Size of row.
     */
    public RowDataHolder(Storable row, int rowSize) {
        this.row = row;
        this.rowSize = rowSize;
    }
    /**
     * Sets {@link Storable} row payload to given {@code record}.
     *
     * @param record WAL record.
     */
    public void linkRow(WALReferenceAwareRecord record) {
        if (record instanceof DataPageInsertFragmentReferencedRecord)
            row.link(((DataPageInsertFragmentReferencedRecord) record).lastLink());

        record.row(row);
    }

    /**
     * @return Linking row size.
     */
    public int rowSize() {
        return rowSize;
    }
}
