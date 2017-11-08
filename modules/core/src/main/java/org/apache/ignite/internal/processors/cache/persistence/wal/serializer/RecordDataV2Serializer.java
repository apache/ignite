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
import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.wal.record.BaselineTopologyRecord;
import org.apache.ignite.internal.pagemem.wal.record.DataEntry;
import org.apache.ignite.internal.pagemem.wal.record.DataRecord;
import org.apache.ignite.internal.pagemem.wal.record.TxRecord;
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

    /** Serializer of {@link TxRecord} records. */
    private TxRecordSerializer txRecordSerializer;

    /** Serializer of {@link BaselineTopologyRecord} records. */
    private final BaselineTopologyRecordSerializer bltRecSerializer;

    /**
     * Create an instance of V2 data serializer.
     *
     * @param delegateSerializer V1 data serializer.
     */
    public RecordDataV2Serializer(RecordDataV1Serializer delegateSerializer) {
        this.delegateSerializer = delegateSerializer;
        this.txRecordSerializer = new TxRecordSerializer();
        this.bltRecSerializer = new BaselineTopologyRecordSerializer(delegateSerializer.cctx());
    }

    /** {@inheritDoc} */
    @Override public int size(WALRecord rec) throws IgniteCheckedException {
        if (rec instanceof HeaderRecord)
            throw new UnsupportedOperationException("Getting size of header records is forbidden since version 2 of serializer");

        switch (rec.type()) {
            case DATA_RECORD:
                return delegateSerializer.size(rec) + 8/*timestamp*/;

            case TX_RECORD:
                return txRecordSerializer.size((TxRecord)rec);

            case BASELINE_TOP_RECORD:
                return bltRecSerializer.size((BaselineTopologyRecord)rec);

            default:
                return delegateSerializer.size(rec);
        }
    }

    /** {@inheritDoc} */
    @Override public WALRecord readRecord(
        WALRecord.RecordType type,
        ByteBufferBackedDataInput in
    ) throws IOException, IgniteCheckedException {
        switch (type) {
            case DATA_RECORD:
                int entryCnt = in.readInt();
                long timeStamp = in.readLong();

                List<DataEntry> entries = new ArrayList<>(entryCnt);

                for (int i = 0; i < entryCnt; i++)
                    entries.add(delegateSerializer.readDataEntry(in));

                return new DataRecord(entries, timeStamp);

            case TX_RECORD:
                return txRecordSerializer.read(in);

            case BASELINE_TOP_RECORD:
                return bltRecSerializer.read(in);

            default:
                return delegateSerializer.readRecord(type, in);
        }
    }

    /** {@inheritDoc} */
    @Override public void writeRecord(WALRecord rec, ByteBuffer buf) throws IgniteCheckedException {
        if (rec instanceof HeaderRecord)
            throw new UnsupportedOperationException("Writing header records is forbidden since version 2 of serializer");

        switch (rec.type()) {
            case DATA_RECORD:
                DataRecord dataRec = (DataRecord)rec;

                buf.putInt(dataRec.writeEntries().size());
                buf.putLong(dataRec.timestamp());

                for (DataEntry dataEntry : dataRec.writeEntries())
                    RecordDataV1Serializer.putDataEntry(buf, dataEntry);

                break;

            case TX_RECORD:
                txRecordSerializer.write((TxRecord)rec, buf);

                break;

            case BASELINE_TOP_RECORD:
                bltRecSerializer.write((BaselineTopologyRecord)rec, buf);

            default:
                delegateSerializer.writeRecord(rec, buf);
        }
    }
}
