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
import org.apache.ignite.internal.processors.cache.persistence.wal.RecordDataSerializer;
import org.apache.ignite.internal.processors.cache.persistence.wal.record.HeaderRecord;

/**
 * Record data V2 serializer.
 */
public class RecordDataV2Serializer implements RecordDataSerializer {
    /** V1 data serializer delegate. */
    private final RecordDataV1Serializer delegateSerializer;

    /**
     * Create an instance of V2 data serializer.
     *
     * @param delegateSerializer V1 data serializer.
     */
    public RecordDataV2Serializer(RecordDataV1Serializer delegateSerializer) {
        this.delegateSerializer = delegateSerializer;
    }

    /** {@inheritDoc} */
    @Override public int size(WALRecord record) throws IgniteCheckedException {
        if (record instanceof HeaderRecord)
            throw new UnsupportedOperationException("Getting size of header records is forbidden since version 2 of serializer");

        return delegateSerializer.size(record);
    }

    /** {@inheritDoc} */
    @Override public WALRecord readRecord(WALRecord.RecordType type, ByteBufferBackedDataInput in) throws IOException, IgniteCheckedException {
        return delegateSerializer.readRecord(type, in);
    }

    /** {@inheritDoc} */
    @Override public void writeRecord(WALRecord record, ByteBuffer buf) throws IgniteCheckedException {
        if (record instanceof HeaderRecord)
            throw new UnsupportedOperationException("Writing header records is forbidden since version 2 of serializer");

        delegateSerializer.writeRecord(record, buf);
    }
}
