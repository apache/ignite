package org.apache.ignite.internal.processors.cache.persistence.wal.link;

import java.nio.ByteBuffer;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.pagemem.wal.record.DataEntry;
import org.apache.ignite.internal.pagemem.wal.record.DataRecord;
import org.apache.ignite.internal.pagemem.wal.record.WALReferenceAwareRecord;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.DataPageIO;

/**
 * Class to extract and link payload from {@link DataRecord} entries to {@link WALReferenceAwareRecord} records.
 */
public class DataRecordPayloadLinker {
    /** Linker base functionality. */
    private final DataRecordLinker delegateLinker = new DataRecordLinker();

    /** Linking entries. */
    private CacheDataRow entries[];

    /** WAL pointer associated with DataRecord. */
    private WALPointer pointer;

    /**
     * Initialize linker with given {@code record} and {@code pointer} associated with record.
     *
     * @param record Data record.
     * @param pointer WAL pointer associated with record.
     * @throws IgniteCheckedException If it's impossible to initialize linker.
     */
    public void init(DataRecord record, WALPointer pointer) throws IgniteCheckedException {
        delegateLinker.init(record);

        this.pointer = pointer;
        this.entries = new CacheDataRow[record.writeEntries().size()];

        for (int i = 0; i < entries.length; i++) {
            DataEntry entry = record.writeEntries().get(i);
            entries[i] = DataRecordLinker.wrap(entry);
        }
    }

    /**
     * Link {@link DataRecord} current entry {@code byte[]} payload to given {@code record}.
     *
     * @param record WAL record.
     * @throws IgniteCheckedException If it's impossible to link payload to given {@code record}.
     */
    public void linkPayload(WALReferenceAwareRecord record) throws IgniteCheckedException {
        int indexBeforeLink = delegateLinker.position().index;
        int offsetBeforeLink = delegateLinker.position().offset;

        delegateLinker.link(record);

        // Initialize byte buffer for entry payload.
        ByteBuffer payloadBuffer = ByteBuffer.allocateDirect(record.payloadSize());

        // Write data entry payload to buffer.
        DataPageIO.writeFragmentData(entries[indexBeforeLink], payloadBuffer, offsetBeforeLink, record.payloadSize());

        record.payload(payloadBuffer.array());
    }

    /**
     * @return WAL pointer associated with DataRecord.
     */
    public WALPointer pointer() {
        return pointer;
    }
}
