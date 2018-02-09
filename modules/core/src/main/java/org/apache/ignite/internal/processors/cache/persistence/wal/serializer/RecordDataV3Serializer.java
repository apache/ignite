package org.apache.ignite.internal.processors.cache.persistence.wal.serializer;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.persistence.wal.ByteBufferBackedDataInput;

public class RecordDataV3Serializer implements RecordDataSerializer {

    private final RecordDataV2Serializer delegateSerializer;

    public RecordDataV3Serializer(RecordDataV2Serializer delegateSerializer) {
        this.delegateSerializer = delegateSerializer;
    }

    @Override public int size(WALRecord record) throws IgniteCheckedException {
        switch (record.type()) {
            case DATA_PAGE_UPDATE_RECORD:

            case DATA_PAGE_INSERT_RECORD:

            case DATA_PAGE_INSERT_FRAGMENT_RECORD:


        }
        return delegateSerializer.size(record);
    }

    @Override public WALRecord readRecord(WALRecord.RecordType type, ByteBufferBackedDataInput in) throws IOException, IgniteCheckedException {
        switch (type) {
            case DATA_PAGE_UPDATE_RECORD:

            case DATA_PAGE_INSERT_RECORD:

            case DATA_PAGE_INSERT_FRAGMENT_RECORD:


        }

        return delegateSerializer.readRecord(type, in);
    }

    @Override public void writeRecord(WALRecord record, ByteBuffer buf) throws IgniteCheckedException {
        switch (record.type()) {
            case DATA_PAGE_UPDATE_RECORD:

            case DATA_PAGE_INSERT_RECORD:

            case DATA_PAGE_INSERT_FRAGMENT_RECORD:

        }

        delegateSerializer.writeRecord(record, buf);
    }
}
