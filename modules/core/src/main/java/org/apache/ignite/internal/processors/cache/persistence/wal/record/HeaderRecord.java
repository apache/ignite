/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.persistence.wal.record;

import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Header record.
 */
public class HeaderRecord extends WALRecord {
    /** Magic of regular WAL segment. */
    public static final long REGULAR_MAGIC = 0xB0D045AC_E7ED045AL;

    /** Magic of WAL segment with skipped physical records. */
    public static final long COMPACTED_MAGIC = 0x4E07AE0E_573A694EL;

    /** Serializer version */
    private final int ver;

    /**
     * @param ver Version.
     */
    public HeaderRecord(int ver) {
        this.ver = ver;
    }

    /**
     * @return Version.
     */
    public int version() {
        return ver;
    }

    /** {@inheritDoc} */
    @Override public RecordType type() {
        return RecordType.HEADER_RECORD;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(HeaderRecord.class, this, "super", super.toString());
    }
}
