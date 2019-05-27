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

package org.apache.ignite.internal.processors.cache.persistence.wal.scanner;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWriteAheadLogManager;
import org.apache.ignite.internal.processors.cache.persistence.wal.serializer.RecordSerializer;
import org.apache.ignite.internal.processors.cache.persistence.wal.serializer.RecordV1Serializer;
import org.apache.ignite.lang.IgniteBiTuple;

/**
 * Handler to print raw pages data into file for further diagnostic.
 */
public class PrintRawToFileHandler extends PrintToFileHandler {
    /** */
    private final RecordSerializer serializer;

    /**
     * @param file Output file.
     * @param serializer Serializer for WAL records.
     */
    public PrintRawToFileHandler(File file, RecordSerializer serializer) {
        super(file, null);

        this.serializer = serializer;
    }

    /** {@inheritDoc} */
    @Override protected byte[] getBytes(IgniteBiTuple<WALPointer, WALRecord> record) {
        try {
            WALRecord walRec = record.get2();

            ByteBuffer buf = ByteBuffer.allocate(serializer.size(walRec));

            serializer.writeRecord(walRec, buf);

            return buf.array();
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /** {@inheritDoc} */
    @Override protected byte[] getHeader() {
        ByteBuffer buf = ByteBuffer.allocate(RecordV1Serializer.HEADER_RECORD_SIZE);

        buf.order(ByteOrder.nativeOrder());

        FileWriteAheadLogManager.prepareSerializerVersionBuffer(0L, serializer.version(), false, buf);

        return buf.array();
    }
}
