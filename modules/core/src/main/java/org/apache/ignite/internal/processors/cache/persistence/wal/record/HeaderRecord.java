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

package org.apache.ignite.internal.processors.cache.persistence.wal.record;

import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Header record.
 */
public class HeaderRecord extends WALRecord {
    /** */
    public static final long MAGIC = 0xB0D045A_CE7ED045AL;

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
