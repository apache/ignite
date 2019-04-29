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

package org.apache.ignite.internal.processors.cache.persistence.wal.serializer;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.persistence.wal.ByteBufferBackedDataInput;

/**
 * Interface to provide size, read and write operations with WAL records
 * <b>without any headers and meta information</b>.
 */
public interface RecordDataSerializer {
    /**
     * Calculates size of record data.
     *
     * @param record WAL record.
     * @return Size of record in bytes.
     * @throws IgniteCheckedException If it's unable to calculate record data size.
     */
    int size(WALRecord record) throws IgniteCheckedException;

    /**
     * Reads record data of {@code type} from buffer {@code in}.
     *
     * @param type Record type.
     * @param in Buffer to read.
     * @param size Record size (0 if unknown).
     * @return WAL record.
     * @throws IOException In case of I/O problems.
     * @throws IgniteCheckedException If it's unable to read record.
     */
    WALRecord readRecord(WALRecord.RecordType type, ByteBufferBackedDataInput in, int size) throws IOException, IgniteCheckedException;

    /**
     * Writes record data to buffer {@code buf}.
     *
     * @param record WAL record.
     * @param buf Buffer to write.
     * @throws IgniteCheckedException If it's unable to write record.
     */
    void writeRecord(WALRecord record, ByteBuffer buf) throws IgniteCheckedException;
}
