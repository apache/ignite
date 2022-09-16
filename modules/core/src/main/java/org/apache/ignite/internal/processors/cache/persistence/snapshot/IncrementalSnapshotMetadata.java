package org.apache.ignite.internal.processors.cache.persistence.snapshot;

import java.io.Serializable;

/** */
public class IncrementalSnapshotMetadata implements Serializable {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** */
    private final long firstSegIdx;

    /** */
    private final long lastSegIdx;

    /** */
    private final String baseSnpName;

    /** Snapshot name. */
    private final String snpName;

    /** Consistent id of a node to which this metadata relates. */
    private final String consId;

    /** */
    public IncrementalSnapshotMetadata(String baseSnpName, String snpName, String consId, long firstSegIdx, long lastSegIdx) {
        this.snpName = snpName;
        this.consId = consId;
        this.firstSegIdx = firstSegIdx;
        this.lastSegIdx = lastSegIdx;
        this.baseSnpName = baseSnpName;
    }

    /**
     * @return Snapshot name.
     */
    public String snapshotName() {
        return snpName;
    }

    /**
     * @return Consistent id of a node to which this metadata relates.
     */
    public String consistentId() {
        return consId;
    }

    /**
     * @return Snapshot name.
     */
    public String baseSnapshotName() {
        return baseSnpName;
    }

    /** */
    public long firstSegIdx() {
        return firstSegIdx;
    }

    /** */
    public long lastSegIdx() {
        return lastSegIdx;
    }
}
