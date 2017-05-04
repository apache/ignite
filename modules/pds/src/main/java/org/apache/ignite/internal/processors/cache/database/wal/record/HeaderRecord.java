package org.apache.ignite.internal.processors.cache.database.wal.record;

import org.apache.ignite.internal.pagemem.wal.record.WALRecord;

/**
 * Header record.
 */
public class HeaderRecord extends WALRecord {
    /** */
    public static final long MAGIC = 0xB0D045A_CE7ED045AL;

    /** */
    private final int ver;

    /**
     * @param ver Version.
     */
    public HeaderRecord(int ver) {
        this.ver = ver;
    }

    /**
     * @return Version.
     */
    public int version() {
        return ver;
    }

    /** {@inheritDoc} */
    @Override public RecordType type() {
        return RecordType.HEADER_RECORD;
    }
}
