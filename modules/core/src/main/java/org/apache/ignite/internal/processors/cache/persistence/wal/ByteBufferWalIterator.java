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

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Optional;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.persistence.wal.record.HeaderRecord;
import org.apache.ignite.internal.processors.cache.persistence.wal.serializer.RecordSerializer;
import org.apache.ignite.internal.processors.cache.persistence.wal.serializer.RecordSerializerFactory;
import org.apache.ignite.internal.processors.cache.persistence.wal.serializer.RecordSerializerFactoryImpl;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteBiTuple;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.HEADER_RECORD;
import static org.apache.ignite.internal.processors.cache.persistence.wal.serializer.RecordV1Serializer.HEADER_RECORD_SIZE;

/** Byte Buffer WAL Iterator */
public class ByteBufferWalIterator extends AbstractWalRecordsIteratorAdapter {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private final ByteBuffer buf;

    /** */
    private final RecordSerializer serializer;

    /** */
    private final ByteBufferBackedDataInputImpl dataInput;

    /** */
    private boolean headerChecked;

    /** */
    public ByteBufferWalIterator(
        @NotNull IgniteLogger log,
        @NotNull GridCacheSharedContext<?, ?> cctx,
        @NotNull ByteBuffer byteBuf,
        int ver) throws IgniteCheckedException {
        this(log, cctx, byteBuf, ver, null);
    }

    /** */
    public ByteBufferWalIterator(
        @NotNull IgniteLogger log,
        @NotNull GridCacheSharedContext<?, ?> cctx,
        @NotNull ByteBuffer byteBuf,
        int ver,
        IgniteBiPredicate<WALRecord.RecordType, WALPointer> readTypeFilter)
        throws IgniteCheckedException {
        super(log);

        buf = byteBuf;

        RecordSerializerFactory rsf = new RecordSerializerFactoryImpl(cctx, readTypeFilter);

        serializer = rsf.createSerializer(ver);

        dataInput = new ByteBufferBackedDataInputImpl();

        dataInput.buffer(buf);

        advance();
    }

    /** */
    private IgniteBiTuple<WALPointer, WALRecord> advanceRecord() throws IgniteCheckedException {
        if (!buf.hasRemaining())
            return null;

        IgniteBiTuple<WALPointer, WALRecord> result;

        try {
            if (!headerChecked) {
                IgniteBiTuple<WALPointer, WALRecord> header = tryToReadHeader();

                if (header != null)
                    return header;
            }
            WALRecord rec = serializer.readRecord(dataInput, null);

            result = new IgniteBiTuple<>(rec.position(), rec);
        }
        catch (SegmentEofException e) {
            return null;
        }
        catch (IOException e) {
            throw new IgniteCheckedException(e);
        }

        return result;
    }

    /** */
    private IgniteBiTuple<WALPointer, WALRecord> tryToReadHeader() throws IgniteCheckedException, IOException {
        headerChecked = true;

        int position = dataInput.buffer().position();

        int type = dataInput.readUnsignedByte();

        if (type == WALRecord.RecordType.STOP_ITERATION_RECORD_TYPE)
            throw new SegmentEofException("Reached logical end of the segment", null);

        WALRecord.RecordType recType = WALRecord.RecordType.fromIndex(type - 1);

        if (recType == HEADER_RECORD) {
            long idx = dataInput.readLong();

            int fileOff = dataInput.readInt();

            WALPointer walPointer = new WALPointer(idx, fileOff, HEADER_RECORD_SIZE);

            long magic = dataInput.readLong();

            if (magic != HeaderRecord.REGULAR_MAGIC && magic != HeaderRecord.COMPACTED_MAGIC)
                throw new EOFException("Magic is corrupted [actual=" + U.hexLong(magic) + ']');

            int ver = dataInput.readInt();

            HeaderRecord r = new HeaderRecord(ver);

            r.position(walPointer);

            // Read CRC.
            dataInput.readInt();

            return new IgniteBiTuple<>(walPointer, r);
        }
        else
            dataInput.buffer().position(position);
        return null;
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
