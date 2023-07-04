/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.persistence.wal;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Optional;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.persistence.wal.serializer.RecordSerializer;
import org.apache.ignite.internal.processors.cache.persistence.wal.serializer.RecordSerializerFactory;
import org.apache.ignite.internal.processors.cache.persistence.wal.serializer.RecordSerializerFactoryImpl;
import org.apache.ignite.lang.IgniteBiTuple;
import org.jetbrains.annotations.NotNull;

/** Byte Buffer WAL Iterator */
public class ByteBufferWalIterator extends AbstractWalRecordsIteratorApdator {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private final ByteBuffer buf;

    /** */
    private final RecordSerializer serializer;

    /** */
    private final ByteBufferBackedDataInputImpl dataInput;

    /** */
    public ByteBufferWalIterator(
        @NotNull IgniteLogger log,
        GridCacheSharedContext<?, ?> cctx,
        ByteBuffer byteBuf) throws IgniteCheckedException {
        super(log);

        buf = byteBuf;

        RecordSerializerFactory rsf = new RecordSerializerFactoryImpl(cctx,
            (t, p) -> t.purpose() == WALRecord.RecordPurpose.LOGICAL).skipPositionCheck(true);

        serializer = rsf.createSerializer(RecordSerializerFactory.LATEST_SERIALIZER_VERSION);

        dataInput = new ByteBufferBackedDataInputImpl();

        dataInput.buffer(buf);

        advance();
    }

    /** */
    private IgniteBiTuple<WALPointer, WALRecord> advanceRecord() throws IgniteCheckedException {
        if (!buf.hasRemaining())
            return null;

        IgniteBiTuple<WALPointer, WALRecord> result;

        WALPointer actualFilePtr = new WALPointer(-1, buf.position(), 0);
        try {
            WALRecord rec = serializer.readRecord(dataInput, actualFilePtr);

            actualFilePtr.length(rec.size());

            result = new IgniteBiTuple<>(actualFilePtr, rec);
        }
        catch (IOException e) {
            throw new IgniteCheckedException(e);
        }

        return result;
    }

    /** {@inheritDoc} */
    @Override protected void advance() throws IgniteCheckedException {
        do
            curRec = advanceRecord();
        while (curRec != null && curRec.get2().type() == null);
    }

    /** {@inheritDoc} */
    @Override public Optional<WALPointer> lastRead() {
        throw new UnsupportedOperationException();
    }
}
