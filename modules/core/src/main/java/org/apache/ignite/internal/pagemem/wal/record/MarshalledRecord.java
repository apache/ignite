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
