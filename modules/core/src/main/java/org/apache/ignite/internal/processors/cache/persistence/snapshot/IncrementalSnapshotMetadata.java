package org.apache.ignite.internal.processors.cache.persistence.snapshot;

import java.io.Serializable;
import java.util.Objects;
import java.util.UUID;
import org.apache.ignite.internal.processors.cache.persistence.wal.WALPointer;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Incremental snapshot metadata file.
 *
 * @see IgniteSnapshotManager#createIncrementalSnapshot(String)
 */
public class IncrementalSnapshotMetadata implements Serializable {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** Unique snapshot request id. */
    private final UUID rqId;

    /** Snapshot name. */
    private final String snpName;
    
    /** Increment index. */
    private final int incIdx;

    /** Consistent id of a node to which this metadata relates. */
    private final String consId;

    /**
     * Directory related to the current consistent node id on which partition files are stored.
     * For some of the cases, consId doesn't equal the directory name.
     */
    private final String folderName;

    /** WAL pointer to consistent cut record. */
    private final WALPointer cutPtr;

    /**
     * @param rqId Unique request id.
     * @param snpName Snapshot name.
     * @param consId Consistent id of a node to which this metadata relates.
     * @param folderName Directory name which stores the data files.
     * @param cutPtr Pointer to consistent cut record.
     */
    public IncrementalSnapshotMetadata(
        UUID rqId,
        String snpName,
        int incIdx,
        String consId,
        String folderName,
        WALPointer cutPtr
    ) {
        this.rqId = rqId;
        this.snpName = snpName;
        this.incIdx = incIdx;
        this.consId = consId;
        this.folderName = folderName;
        this.cutPtr = cutPtr;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;
        IncrementalSnapshotMetadata metadata = (IncrementalSnapshotMetadata)o;

        return incIdx == metadata.incIdx
            && Objects.equals(rqId, metadata.rqId)
            && Objects.equals(snpName, metadata.snpName)
            && Objects.equals(consId, metadata.consId)
            && Objects.equals(folderName, metadata.folderName)
            && Objects.equals(cutPtr, metadata.cutPtr);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return Objects.hash(rqId, snpName, incIdx, consId, folderName, cutPtr);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IncrementalSnapshotMetadata.class, this);
    }
}
