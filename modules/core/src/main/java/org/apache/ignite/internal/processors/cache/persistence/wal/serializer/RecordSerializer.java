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

package org.apache.ignite.internal.processors.cache.persistence.wal.serializer;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.persistence.wal.io.FileInput;

/**
 * Record serializer.
 */
public interface RecordSerializer {
    /**
     * @return serializer version
     */
    public int version();

    /**
     * Calculates record size in byte including expected wal pointer, CRC and type field
     *
     * @param record Record.
     * @return Size in bytes.
     */
    public int size(WALRecord record) throws IgniteCheckedException;

    /**
     * @param record Entry to write.
     * @param buf Buffer.
     */
    public void writeRecord(WALRecord record, ByteBuffer buf) throws IgniteCheckedException;

    /**
     * Loads record from input
     *
     * @param in Data input to read data from.
     * @param expPtr expected WAL pointer for record. Used to validate actual position against expected from the file
     * @return Read entry.
     */
    public WALRecord readRecord(FileInput in, WALPointer expPtr) throws IOException, IgniteCheckedException;

    /**
     * Flag to write (or not) wal pointer to record
     */
    public boolean writePointer();
}
