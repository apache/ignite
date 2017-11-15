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

import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWALPointer;

/**
 * Special type of WAL record. Shouldn't be stored in file.
 * Contains complete binary representation of record in {@link #data} and record position in {@link #pos}.
 */
public class MarshalledRecord extends WALRecord {
    /** Type of marshalled record. */
    private WALRecord.RecordType type;

    /** Marshalled record bytes. */
    private byte[] data;

    /**
     * @param type Type of marshalled record.
     */
    public MarshalledRecord(WALRecord.RecordType type, WALPointer pos, byte[] data) {
        this.type = type;
        this.data = data;

        assert data.length == ((FileWALPointer)pos).length();

        position(pos);
    }

    /** {@inheritDoc} */
    @Override public RecordType type() {
        return type;
    }

    /**
     * @return Marshalled record bytes.
     */
    public byte[] data() {
        return data;
    }

    /**
     * @param data New marshalled record bytes.
     */
    public void data(byte[] data) {
        this.data = data;
    }
}
