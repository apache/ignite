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

import java.nio.ByteBuffer;
import org.apache.ignite.internal.pagemem.wal.WALPointer;

/**
 * Special type of WAL record. Shouldn't be stored in file.
 * Contains complete binary representation of record in {@link #buf} and record position in {@link #pos}.
 */
public class MarshalledRecord extends WALRecord {
    /** Type of marshalled record. */
    private WALRecord.RecordType type;

    /**
     * Heap buffer with marshalled record bytes.
     * Due to performance reasons accessible only by thread that performs WAL iteration and until next record is read.
     */
    private ByteBuffer buf;

    /**
     * @param type Type of marshalled record.
     * @param pos WAL pointer to record.
     * @param buf Reusable buffer with record data.
     */
    public MarshalledRecord(RecordType type, WALPointer pos, ByteBuffer buf) {
        this.type = type;
        this.buf = buf;

        position(pos);
    }

    /** {@inheritDoc} */
    @Override public RecordType type() {
        return type;
    }

    /**
     * @return Buffer with marshalled record bytes.  Due to performance reasons accessible only by thread that performs
     * WAL iteration and until next record is read.
     */
    public ByteBuffer buffer() {
        return buf;
    }
}
