package org.apache.ignite.internal.processors.cache.persistence.wal.serializer;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.persistence.wal.ByteBufferBackedDataInput;
import org.apache.ignite.internal.processors.cache.persistence.wal.RecordDataSerializer;

/**
 * Record data V2 serializer.
 */
public class RecordDataV2Serializer implements RecordDataSerializer {

    private final RecordDataV1Serializer delegateSerializer;

    public RecordDataV2Serializer(RecordDataV1Serializer delegateSerializer) {
        this.delegateSerializer = delegateSerializer;
    }

    @Override
    public int size(WALRecord record) throws IgniteCheckedException {
        //TODO: Implementation will be changed.
        return delegateSerializer.size(record);
    }

    @Override
    public WALRecord readRecord(WALRecord.RecordType type, ByteBufferBackedDataInput in) throws IOException, IgniteCheckedException {
        //TODO: Implementation will be changed.
        return delegateSerializer.readRecord(type, in);
    }

    @Override
    public void writeRecord(WALRecord record, ByteBuffer buf) throws IgniteCheckedException {
        //TODO: Implementation will be changed.
        delegateSerializer.writeRecord(record, buf);
    }
}
