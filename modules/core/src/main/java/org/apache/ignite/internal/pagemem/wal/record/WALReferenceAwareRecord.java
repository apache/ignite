package org.apache.ignite.internal.pagemem.wal.record;

import org.apache.ignite.internal.pagemem.wal.WALPointer;

/**
 * Interface is needed to link {@link DataRecord} entries WAL reference
 * with appropriate physical DataPage insert/update records.
 */
public interface WALReferenceAwareRecord {
    /**
     * Record payload size which is needed for extracting actual payload from WAL reference.
     *
     * @return payload size in bytes.
     */
    int payloadSize();

    /**
     * Set payload to this record.
     *
     * @param payload Payload.
     */
    void payload(byte[] payload);

    /**
     * Reference to appropriate {@link DataRecord} in WAL.
     *
     * @return WAL reference.
     */
    WALPointer reference();

    /**
     * Set reference to {@link DataRecord} in WAL.
     *
     * @param reference WAL reference.
     */
    void reference(WALPointer reference);
}
