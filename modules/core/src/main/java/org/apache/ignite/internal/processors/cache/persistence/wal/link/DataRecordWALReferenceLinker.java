package org.apache.ignite.internal.processors.cache.persistence.wal.link;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.pagemem.wal.record.DataRecord;
import org.apache.ignite.internal.pagemem.wal.record.WALReferenceAwareRecord;

/**
 * Class to validate and link {@link DataRecord} entries {@link WALPointer} pointer to {@link WALReferenceAwareRecord} records.
 */
public class DataRecordWALReferenceLinker {
    /** Linker base functionality. */
    private final DataRecordLinker delegateLinker = new DataRecordLinker();

    /** Current WAL pointer associated with DataRecord. */
    private WALPointer pointer;

    /**
     * Initialize linker with given {@code record} and WAL {@code pointer} associated with record.
     *
     * @param record DataRecord.
     * @param pointer WAL reference.
     * @throws IgniteCheckedException If it's impossible to initialize linker.
     */
    public void init(DataRecord record, WALPointer pointer) throws IgniteCheckedException {
        delegateLinker.init(record);

        this.pointer = pointer;
    }

    /**
     * Link {@link DataRecord} WAL reference pointer to given {@code record}.
     *
     * @param record WAL record to associate with {@link DataRecord} WAL reference.
     * @throws IgniteCheckedException If it's impossible to link pointer to given {@code record}.
     */
    public void linkReference(WALReferenceAwareRecord record) throws IgniteCheckedException {
        delegateLinker.link(record);

        record.reference(pointer);
    }
}
