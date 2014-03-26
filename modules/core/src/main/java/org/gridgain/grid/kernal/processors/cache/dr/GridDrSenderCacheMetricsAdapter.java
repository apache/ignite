/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.dr;

import org.gridgain.grid.dr.cache.sender.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jdk8.backport.*;
import org.jetbrains.annotations.*;

import java.io.*;

/**
 * Adapter for DR send data node metrics.
 */
public class GridDrSenderCacheMetricsAdapter implements GridDrSenderCacheMetrics, Externalizable {
    /** Number of sent batches. */
    private LongAdder batchesSent = new LongAdder();

    /** Number of sent entries. */
    private LongAdder entriesSent = new LongAdder();

    /** Number of sent batches with received acknowledgement from sender hub. */
    private LongAdder entriesFiltered = new LongAdder();

    /** Number of sent batches with received acknowledgement from sender hub. */
    private LongAdder batchesAcked = new LongAdder();

    /** Number of sent entries with received acknowledgement from sender hub. */
    private LongAdder entriesAcked = new LongAdder();

    /** Number of failed batches with received acknowledgement from sender hub. */
    private LongAdder batchesFailed = new LongAdder();

    /** Total amount of entries in backup queue. */
    private volatile long backupQueueSize;

    /** Reason of replication pause. */
    private volatile GridDrPauseReason pauseReason;

    /**
     * No-args constructor.
     */
    public GridDrSenderCacheMetricsAdapter() {
        // No-op.
    }

    /**
     * @param m Metrics to copy from.
     */
    GridDrSenderCacheMetricsAdapter(GridDrSenderCacheMetrics m) {
        batchesSent.add(m.batchesSent());
        entriesSent.add(m.entriesSent());
        entriesFiltered.add(m.entriesFiltered());
        batchesAcked.add(m.batchesAcked());
        entriesAcked.add(m.entriesAcked());
        batchesFailed.add(m.batchesFailed());
        backupQueueSize = m.backupQueueSize();
        pauseReason = m.pauseReason();
    }

        /** {@inheritDoc} */
    @Override public int batchesSent() {
        return batchesSent.intValue();
    }

    /** {@inheritDoc} */
    @Override public long entriesSent() {
        return entriesSent.longValue();
    }

    /** {@inheritDoc} */
    @Override public long entriesFiltered() {
        return entriesFiltered.longValue();
    }

    /** {@inheritDoc} */
    @Override public int batchesAcked() {
        return batchesAcked.intValue();
    }

    /** {@inheritDoc} */
    @Override public long entriesAcked() {
        return entriesAcked.longValue();
    }

    /** {@inheritDoc} */
    @Override public int batchesFailed() {
        return batchesFailed.intValue();
    }

    /** {@inheritDoc} */
    @Override public long backupQueueSize() {
        return backupQueueSize;
    }

    /** {@inheritDoc} */
    @Nullable @Override public GridDrPauseReason pauseReason() {
        return pauseReason;
    }

    /**
     * Callback for received successful batch acknowledgement by sender hub.
     *
     * @param entriesCnt Number of entries in batch.
     */
    public void onBatchAcked(int entriesCnt) {
        batchesAcked.increment();

        entriesAcked.add(entriesCnt);
    }

    /**
     * Callback for received batch error by sender hub.
     *
     * @param entriesCnt Number of entries in batch.
     */
    public void onBatchFailed(int entriesCnt) {
        batchesFailed.increment();
    }

    /**
     * Callback for sent batch on sender cache side.
     *
     * @param entriesCnt Number of sent entries.
     */
    public void onBatchSent(int entriesCnt) {
        batchesSent.increment();

        entriesSent.add(entriesCnt);
    }

    /**
     * Callback for filtered entries on sender cache side.
     */
    public void onEntryFiltered() {
        entriesFiltered.increment();
    }

    /**
     * Callback for backup queue size changed.
     *
     * @param backupQueueSize Size of sender cache backup queue.
     */
    public void backupQueueSize(int backupQueueSize) {
        this.backupQueueSize = backupQueueSize;
    }

    /**
     * Callback for replication pause state changed.
     *
     * @param pauseReason Pause reason or {@code null} if replication is not paused.
     */
    public void onPauseStateChanged(@Nullable GridDrPauseReason pauseReason) {
        this.pauseReason = pauseReason;
    }

    /**
     * Create a copy of given metrics object.
     *
     * @param m Metrics to copy from.
     * @return Copy of given metrics.
     */
    @Nullable public static GridDrSenderCacheMetricsAdapter copyOf(@Nullable GridDrSenderCacheMetrics m) {
        if (m == null)
            return null;

        return new GridDrSenderCacheMetricsAdapter(m);
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(batchesSent.intValue());
        out.writeLong(entriesSent.longValue());
        out.writeLong(entriesFiltered.longValue());
        out.writeInt(batchesAcked.intValue());
        out.writeLong(entriesAcked.longValue());
        out.writeInt(batchesFailed.intValue());
        out.writeLong(backupQueueSize);
        U.writeEnum(out, pauseReason);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        batchesSent.add(in.readInt());
        entriesSent.add(in.readLong());
        entriesFiltered.add(in.readLong());
        batchesAcked.add(in.readInt());
        entriesAcked.add(in.readLong());
        batchesFailed.add(in.readInt());
        backupQueueSize = in.readLong();
        pauseReason = U.readEnum(in, GridDrPauseReason.class);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDrSenderCacheMetricsAdapter.class, this);
    }
}
