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
import org.apache.ignite.internal.processors.cache.persistence.wal.serializer.RecordSerializerFactoryImpl;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteBiTuple;

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
    public ByteBufferWalIterator(
        IgniteLogger log,
        GridCacheSharedContext<?, ?> cctx,
        ByteBuffer byteBuf,
        int ver)
        throws IgniteCheckedException {
        this(log, cctx, byteBuf, ver, null);
    }

    /** */
    public ByteBufferWalIterator(
        IgniteLogger log,
        GridCacheSharedContext<?, ?> cctx,
        ByteBuffer byteBuf,
        int ver,
        IgniteBiPredicate<WALRecord.RecordType, WALPointer> readTypeFilter)
        throws IgniteCheckedException {
        super(log);

        buf = byteBuf;

        serializer = new RecordSerializerFactoryImpl(cctx, readTypeFilter).createSerializer(ver);

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
            if (curRec == null)
                tryToReadHeader();

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
    private void tryToReadHeader() throws IOException {
        int position = dataInput.buffer().position();

        int type = dataInput.readUnsignedByte();

        WALRecord.RecordType recType = WALRecord.RecordType.fromIndex(type - 1);

        if (recType == HEADER_RECORD)
            dataInput.buffer().position(position + HEADER_RECORD_SIZE);
        else
            dataInput.buffer().position(position);
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
