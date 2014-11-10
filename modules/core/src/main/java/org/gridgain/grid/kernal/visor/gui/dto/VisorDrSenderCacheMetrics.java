/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.gui.dto;

import org.gridgain.grid.cache.*;
import org.gridgain.grid.dr.*;
import org.gridgain.grid.dr.cache.sender.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;
import java.util.*;

/**
 * Data transfer object for {@link GridDrSenderCacheMetrics}.
 */
public class VisorDrSenderCacheMetrics implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Total amount of batches sent for data replication. */
    private int batchesSent;

    /** Total amount of entries sent for data replication. */
    private long entriesSent;

    /** Total amount of filtered cache entries which wasn't used for data replication. */
    private long entriesFiltered;

    /** Total amount of sent batches with received acknowledgement from sender hub. */
    private int batchesAcked;

    /** Total amount of sent entries with received acknowledgement from sender hub. */
    private long entriesAcked;

    /** Total amount of batches which failed during data replication. */
    private int batchesFailed;

    /** Total amount of entries in backup queue. */
    private long backupQueueSize;

    /** Reason of data center replication status. */
    private GridDrStatus status;

    /** List of active state transfers. */
    private Collection<GridDrStateTransferDescriptor> stateTransfers;

    /** Create data transfer object for given cache. */
    public static VisorDrSenderCacheMetrics from(GridCache cache) {
        assert cache != null;

        try {
            GridDr dr = cache.gridProjection().grid().dr();

            GridDrSenderCacheMetrics m = dr.senderCacheMetrics(cache.name());

            VisorDrSenderCacheMetrics metrics = new VisorDrSenderCacheMetrics();

            metrics.batchesSent(m.batchesSent());
            metrics.entriesSent(m.entriesSent());

            metrics.batchesAcked(m.batchesAcked());
            metrics.entriesAcked(m.entriesAcked());

            metrics.batchesFailed(m.batchesFailed());
            metrics.entriesFiltered(m.entriesFiltered());
            metrics.backupQueueSize(m.backupQueueSize());
            metrics.status(m.status());
            metrics.stateTransfers(dr.senderCacheDrListStateTransfers(cache.name()));

            return metrics;
        }
        catch (IllegalStateException | IllegalArgumentException ignored) {
            return null;
        }
    }

    /**
     * @return Total amount of batches sent for data replication.
     */
    public int batchesSent() {
        return batchesSent;
    }

    /**
     * @param batchesSent New total amount of batches sent for data replication.
     */
    public void batchesSent(int batchesSent) {
        this.batchesSent = batchesSent;
    }

    /**
     * @return Total amount of entries sent for data replication.
     */
    public long entriesSent() {
        return entriesSent;
    }

    /**
     * @param entriesSent New total amount of entries sent for data replication.
     */
    public void entriesSent(long entriesSent) {
        this.entriesSent = entriesSent;
    }

    /**
     * @return Total amount of filtered cache entries which wasn't used for data replication.
     */
    public long entriesFiltered() {
        return entriesFiltered;
    }

    /**
     * @param entriesFiltered New total amount of filtered cache entries which wasn't used for data replication.
     */
    public void entriesFiltered(long entriesFiltered) {
        this.entriesFiltered = entriesFiltered;
    }

    /**
     * @return Total amount of sent batches with received acknowledgement from sender hub.
     */
    public int batchesAcked() {
        return batchesAcked;
    }

    /**
     * @param batchesAcked New total amount of sent batches with received acknowledgement from sender hub.
     */
    public void batchesAcked(int batchesAcked) {
        this.batchesAcked = batchesAcked;
    }

    /**
     * @return Total amount of sent entries with received acknowledgement from sender hub.
     */
    public long entriesAcked() {
        return entriesAcked;
    }

    /**
     * @param entriesAcked New total amount of sent entries with received acknowledgement from sender hub.
     */
    public void entriesAcked(long entriesAcked) {
        this.entriesAcked = entriesAcked;
    }

    /**
     * @return Total amount of batches which failed during data replication.
     */
    public int batchesFailed() {
        return batchesFailed;
    }

    /**
     * @param batchesFailed New total amount of batches which failed during data replication.
     */
    public void batchesFailed(int batchesFailed) {
        this.batchesFailed = batchesFailed;
    }

    /**
     * @return Total amount of entries in backup queue.
     */
    public long backupQueueSize() {
        return backupQueueSize;
    }

    /**
     * @param backupQueueSize New total amount of entries in backup queue.
     */
    public void backupQueueSize(long backupQueueSize) {
        this.backupQueueSize = backupQueueSize;
    }

    /**
     * @return Reason of data center replication status.
     */
    public GridDrStatus status() {
        return status;
    }

    /**
     * @param status New reason of data center replication status.
     */
    public void status(GridDrStatus status) {
        this.status = status;
    }

    /**
     * @return List of active state transfers.
     */
    public Collection<GridDrStateTransferDescriptor> stateTransfers() {
        return stateTransfers;
    }

    /**
     * @param stateTransfers New list of active state transfers.
     */
    public void stateTransfers(Collection<GridDrStateTransferDescriptor> stateTransfers) {
        this.stateTransfers = stateTransfers;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorDrSenderCacheMetrics.class, this);
    }
}
