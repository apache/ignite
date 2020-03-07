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

import org.apache.ignite.internal.util.typedef.internal.S;

import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.SNAPSHOT_RESTORE_RECORD;

/**
 *
 */
public class SnapshotRestoreRecord extends WALRecord {
    /** Master key name. */
    private final String snpName;

    // todo do we need snapshot recovery state on phase-1?

    /**
     * @param snpName Snapshot name.
     */
    public SnapshotRestoreRecord(String snpName) {
        this.snpName = snpName;
    }

    /** @return Snapshot name. */
    public String snapshotName() {
        return snpName;
    }

    /** {@inheritDoc} */
    @Override public RecordType type() {
        return SNAPSHOT_RESTORE_RECORD;
    }

    /** @return Record data size. */
    public int dataSize() {
        // Snapshot name length.
        return 4 + snpName.length();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(SnapshotRestoreRecord.class, this);
    }
}
