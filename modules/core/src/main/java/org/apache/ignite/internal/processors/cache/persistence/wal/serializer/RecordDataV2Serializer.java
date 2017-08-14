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
        return delegateSerializer.size(record);
    }

    /** {@inheritDoc} */
    @Override public WALRecord readRecord(WALRecord.RecordType type, ByteBufferBackedDataInput in) throws IOException, IgniteCheckedException {
        return delegateSerializer.readRecord(type, in);
    }

    /** {@inheritDoc} */
    @Override public void writeRecord(WALRecord record, ByteBuffer buf) throws IgniteCheckedException {
        delegateSerializer.writeRecord(record, buf);
    }
}
