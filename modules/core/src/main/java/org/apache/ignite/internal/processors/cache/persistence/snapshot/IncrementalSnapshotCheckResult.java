package org.apache.ignite.internal.processors.cache.persistence.snapshot;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;
import org.apache.ignite.internal.management.cache.PartitionKeyV2;
import org.apache.ignite.internal.pagemem.wal.record.DataEntry;
import org.apache.ignite.internal.processors.cache.verify.PartitionHashRecordV2;
import org.apache.ignite.internal.processors.cache.verify.TransactionsHashRecord;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;

/** */
class IncrementalSnapshotCheckResult implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Transaction hashes collection. */
    private Map<Object, TransactionsHashRecord> txHashRes;

    /**
     * Partition hashes collection. Value is a hash of data entries {@link DataEntry} from WAL segments included
     * into the incremental snapshot.
     */
    private Map<PartitionKeyV2, PartitionHashRecordV2> partHashRes;

    /** Partially committed transactions' collection. */
    private Collection<GridCacheVersion> partiallyCommittedTxs;

    /** Occurred exceptions. */
    private Collection<Exception> exceptions;

    /** */
    public IncrementalSnapshotCheckResult() {
        // No-op.
    }

    /** */
    IncrementalSnapshotCheckResult(
        Map<Object, TransactionsHashRecord> txHashRes,
        Map<PartitionKeyV2, PartitionHashRecordV2> partHashRes,
        Collection<GridCacheVersion> partiallyCommittedTxs,
        Collection<Exception> exceptions
    ) {
        this.txHashRes = txHashRes;
        this.partHashRes = partHashRes;
        this.partiallyCommittedTxs = partiallyCommittedTxs;
        this.exceptions = exceptions;
    }

    /** */
    public Map<PartitionKeyV2, PartitionHashRecordV2> partHashRes() {
        return partHashRes;
    }

    /** */
    public Map<Object, TransactionsHashRecord> txHashRes() {
        return txHashRes;
    }

    /** */
    public Collection<GridCacheVersion> partiallyCommittedTxs() {
        return partiallyCommittedTxs;
    }

    /** */
    public Collection<Exception> exceptions() {
        return exceptions;
    }
}
