/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.gui.dto;

import org.gridgain.grid.cache.*;
import org.gridgain.grid.dr.cache.receiver.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;

/**
 * Data transfer object for {@link GridDrReceiverCacheMetrics}.
 */
public class VisorDrReceiverCacheMetrics implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Total amount of cache entries received from receiver hub. */
    private long entriesReceived;

    /** Total amount of data replication conflicts resolved by using new value. */
    private long conflictNew;

    /** Total amount of data replication conflicts resolved by using old value. */
    private long conflictOld;

    /** Total amount of data replication conflicts resolved by merging values. */
    private long conflictMerge;

    /**
     * @param cache Source cache.
     * @return Create data transfer object for DR receiver cache metrics of given cache.
     */
    public static VisorDrReceiverCacheMetrics from(GridCache cache) {
        assert cache != null;

        try {
            GridDrReceiverCacheMetrics m = cache.metrics().drReceiveMetrics();

            VisorDrReceiverCacheMetrics metrics = new VisorDrReceiverCacheMetrics();

            metrics.entriesReceived(m.entriesReceived());
            metrics.conflictNew(m.conflictNew());
            metrics.conflictOld(m.conflictOld());
            metrics.conflictMerge(m.conflictMerge());

            return metrics;
        }
        catch (IllegalStateException ignored) {
            return null;
        }
    }

    /**
     * @return Total amount of cache entries received from receiver hub.
     */
    public long entriesReceived() {
        return entriesReceived;
    }

    /**
     * @param entriesReceived New total amount of cache entries received from receiver hub.
     */
    public void entriesReceived(long entriesReceived) {
        this.entriesReceived = entriesReceived;
    }

    /**
     * @return Total amount of data replication conflicts resolved by using new value.
     */
    public long conflictNew() {
        return conflictNew;
    }

    /**
     * @param conflictNew New total amount of data replication conflicts resolved by using new value.
     */
    public void conflictNew(long conflictNew) {
        this.conflictNew = conflictNew;
    }

    /**
     * @return Total amount of data replication conflicts resolved by using old value.
     */
    public long conflictOld() {
        return conflictOld;
    }

    /**
     * @param conflictOld New total amount of data replication conflicts resolved by using old value.
     */
    public void conflictOld(long conflictOld) {
        this.conflictOld = conflictOld;
    }

    /**
     * @return Total amount of data replication conflicts resolved by merging values.
     */
    public long conflictMerge() {
        return conflictMerge;
    }

    /**
     * @param conflictMerge New total amount of data replication conflicts resolved by merging values.
     */
    public void conflictMerge(long conflictMerge) {
        this.conflictMerge = conflictMerge;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorDrReceiverCacheMetrics.class, this);
    }
}
