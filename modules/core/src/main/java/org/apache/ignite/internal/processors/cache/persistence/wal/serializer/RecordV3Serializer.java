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

import java.io.DataInput;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.wal.record.FilteredRecord;
import org.apache.ignite.internal.pagemem.wal.record.MarshalledRecord;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.persistence.wal.ByteBufferBackedDataInput;
import org.apache.ignite.internal.processors.cache.persistence.wal.SegmentEofException;
import org.apache.ignite.internal.processors.cache.persistence.wal.WALPointer;
import org.apache.ignite.internal.processors.cache.persistence.wal.WalSegmentTailReachedException;
import org.apache.ignite.internal.processors.cache.persistence.wal.io.FileInput;
import org.apache.ignite.internal.processors.cache.persistence.wal.serializer.io.RecordIO;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteBiPredicate;

import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.ENCRYPTED_RECORD;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.ENCRYPTED_RECORD_V2;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.SWITCH_SEGMENT_RECORD;
import static org.apache.ignite.internal.processors.cache.persistence.wal.serializer.RecordV1Serializer.CRC_SIZE;
import static org.apache.ignite.internal.processors.cache.persistence.wal.serializer.RecordV1Serializer.REC_TYPE_SIZE;
import static org.apache.ignite.internal.processors.cache.persistence.wal.serializer.RecordV1Serializer.readRecordType;

/**
 * Record V2 serializer.
 * Stores records in following format:
 * <ul>
 * <li>Record type from {@link RecordType#index()} incremented by 1</li>
 * <li>WAL pointer to double check consistency</li>
 * <li>Record length</li>
 * <li>Data</li>
 * <li>CRC or zero padding</li>
 * </ul>
 */
public class RecordV3Serializer extends RecordV2Serializer {
    /**
     * Create an instance of Record V3 serializer.
     *
     * @param dataSerializer V3 data serializer.
     * @param marshalledMode Marshalled mode.
     * @param skipPositionCheck Skip position check mode.
     * @param recordFilter Record type filter. {@link FilteredRecord} is deserialized instead of original record.
     */
    public RecordV3Serializer(
        RecordDataV2Serializer dataSerializer,
        boolean writePointer,
        boolean marshalledMode,
        boolean skipPositionCheck,
        IgniteBiPredicate<RecordType, WALPointer> recordFilter
    ) {
        super(dataSerializer, writePointer, marshalledMode, skipPositionCheck, recordFilter);
    }

    /** {@inheritDoc} */
    @Override public int version() {
        return 3;
    }
}
