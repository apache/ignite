package org.apache.ignite.internal.processors.cache.persistence.wal.serializer;

import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.lang.IgniteBiPredicate;

public class RecordV3Serializer extends RecordV2Serializer {

    /** {@inheritDoc} */
    public RecordV3Serializer(RecordDataV3Serializer dataSerializer,
                              boolean writePointer,
                              boolean marshalledMode,
                              boolean skipPositionCheck,
                              IgniteBiPredicate<WALRecord.RecordType, WALPointer> recordFilter) {
        super(dataSerializer, writePointer, marshalledMode, skipPositionCheck, recordFilter);
    }

    /** {@inheritDoc} */
    @Override public int version() {
        return 3;
    }
}
