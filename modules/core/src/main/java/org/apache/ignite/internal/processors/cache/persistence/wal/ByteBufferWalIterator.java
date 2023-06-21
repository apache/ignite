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

import java.nio.ByteBuffer;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.persistence.wal.serializer.RecordSerializer;
import org.apache.ignite.internal.processors.cache.persistence.wal.serializer.RecordSerializerFactory;
import org.apache.ignite.internal.processors.cache.persistence.wal.serializer.RecordSerializerFactoryImpl;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/** Byte Buffer WAL Iterator */
public class ByteBufferWalIterator extends AbstractWalRecordsIterator {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private final ByteBuffer buffer;

    /** */
    private final RecordSerializer serializer;

    /** constructor */
    public ByteBufferWalIterator(
        @NotNull IgniteLogger log,
        ByteBuffer byteBuffer) throws IgniteCheckedException {
        super(log);

        buffer = byteBuffer;

        RecordSerializerFactory rsf = new RecordSerializerFactoryImpl(null,
            (t, p) -> t.purpose() == WALRecord.RecordPurpose.LOGICAL).skipPositionCheck(true).skipIndexCheck(true);
        serializer = rsf.createSerializer(RecordSerializerFactory.LATEST_SERIALIZER_VERSION);

        advance();
    }

    /** {@inheritDoc} */
    @Override protected IgniteCheckedException validateTailReachedException(WalSegmentTailReachedException tailReachedException,
        AbstractWalSegmentHandle currWalSegment) {
        return null;
    }

    /** {@inheritDoc} */
    @Override protected AbstractWalSegmentHandle advanceSegment(
        @Nullable AbstractWalSegmentHandle segment) {
        if (segment == null) {
            ByteBufferBackedDataInput in = new ByteBufferBackedDataInputImpl().buffer(buffer);

            return new BufferSegment(in, serializer);
        }
        return null;
    }

    /** */
    private static class BufferSegment implements AbstractWalSegmentHandle {

        /** */
        private ByteBufferBackedDataInput in;

        /** */
        private RecordSerializer ser;

        /** */
        public BufferSegment(ByteBufferBackedDataInput in, RecordSerializer ser) {
            this.in = in;
            this.ser = ser;
        }

        /** */
        @Override public void close() throws IgniteCheckedException {
        }

        /** */
        @Override public long idx() {
            return -1;
        }

        /** */
        @Override public ByteBufferBackedDataInput in() {
            return in;
        }

        /** */
        @Override public RecordSerializer ser() {
            return ser;
        }
    }
}
