package org.apache.ignite.internal.pagemem.wal.record;

import org.apache.ignite.internal.pagemem.wal.WALPointer;

/**
 * Interface is needed to link WAL reference of {@link DataRecord} entries
 * to appropriate physical DataPage insert/update records.
 */
public interface WALReferenceAwareRecord {
    /**
     * Record payload size which is needed for extracting actual payload from WAL reference.
     *
     * @return Payload size in bytes.
     */
    int payloadSize();

    /**
     * Set payload extracted from {@link DataRecord} to this record.
     *
     * @param payload Payload.
     */
    void payload(byte[] payload);

    /**
     * WAL reference to appropriate {@link DataRecord}.
     *
     * @return WAL reference.
     */
    WALPointer reference();
}
