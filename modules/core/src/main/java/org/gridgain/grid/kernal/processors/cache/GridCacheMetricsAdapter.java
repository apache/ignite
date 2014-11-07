/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache;

import org.gridgain.grid.cache.*;
import org.gridgain.grid.dr.cache.receiver.*;
import org.gridgain.grid.dr.cache.sender.*;
import org.gridgain.grid.kernal.processors.cache.dr.*;
import org.gridgain.grid.util.tostring.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;

/**
 * Adapter for cache metrics.
 */
public class GridCacheMetricsAdapter implements GridCacheMetrics, Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Create time. */
    private long createTime = U.currentTimeMillis();

    /** Last read time. */
    private volatile long readTime = createTime;

    /** Last update time. */
    private volatile long writeTime = createTime;

    /** Last commit time. */
    private volatile long commitTime = createTime;

    /** Last rollback time. */
    private volatile long rollbackTime = createTime;

    /** Number of reads. */
    private volatile int reads;

    /** Number of writes. */
    private volatile int writes;

    /** Number of hits. */
    private volatile int hits;

    /** Number of misses. */
    private volatile int misses;

    /** Number of transaction commits. */
    private volatile int txCommits;

    /** Number of transaction rollbacks. */
    private volatile int txRollbacks;

    /** DR send data node metrics. */
    private GridDrSenderCacheMetricsAdapter drSndMetrics;

    /** DR receive data node metrics. */
    private GridDrReceiverCacheMetricsAdapter drRcvMetrics;

    /** Cache metrics. */
    @GridToStringExclude
    private transient GridCacheMetricsAdapter delegate;

    /**
     * No-args constructor.
     */
    public GridCacheMetricsAdapter() {
        delegate = null;
    }

    /**
     * @param isDrSndCache True if data center replication configured for sending on that node.
     * @param isDrRcvCache True if data center replication configured for receiving on that node.
     */
    public GridCacheMetricsAdapter(boolean isDrSndCache, boolean isDrRcvCache) {
        this();

        if (isDrSndCache)
            drSndMetrics = new GridDrSenderCacheMetricsAdapter();

        if (isDrRcvCache)
            drRcvMetrics = new GridDrReceiverCacheMetricsAdapter();
    }

    /**
     * @param m Metrics to copy from.
     */
    public GridCacheMetricsAdapter(GridCacheMetrics m) {
        createTime = m.createTime();
        readTime = m.readTime();
        writeTime = m.writeTime();
        commitTime = m.commitTime();
        rollbackTime = m.rollbackTime();
        reads = m.reads();
        writes = m.writes();
        hits = m.hits();
        misses = m.misses();
        txCommits = m.txCommits();
        txRollbacks = m.txRollbacks();
        drSndMetrics = ((GridCacheMetricsAdapter)m).drSndMetrics;
        drRcvMetrics = ((GridCacheMetricsAdapter)m).drRcvMetrics;
    }

    /**
     * @param delegate Metrics to delegate to.
     */
    public void delegate(GridCacheMetricsAdapter delegate) {
        this.delegate = delegate;
    }

    /** {@inheritDoc} */
    @Override public long createTime() {
        return createTime;
    }

    /** {@inheritDoc} */
    @Override public long writeTime() {
        return writeTime;
    }

    /** {@inheritDoc} */
    @Override public long readTime() {
        return readTime;
    }

    /** {@inheritDoc} */
    @Override public long commitTime() {
        return commitTime;
    }

    /** {@inheritDoc} */
    @Override public long rollbackTime() {
        return rollbackTime;
    }

    /** {@inheritDoc} */
    @Override public int reads() {
        return reads;
    }

    /** {@inheritDoc} */
    @Override public int writes() {
        return writes;
    }

    /** {@inheritDoc} */
    @Override public int hits() {
        return hits;
    }

    /** {@inheritDoc} */
    @Override public int misses() {
        return misses;
    }

    /** {@inheritDoc} */
    @Override public int txCommits() {
        return txCommits;
    }

    /** {@inheritDoc} */
    @Override public int txRollbacks() {
        return txRollbacks;
    }

    /** {@inheritDoc} */
    @Override public GridDrSenderCacheMetrics drSendMetrics() {
        if (drSndMetrics == null)
            throw new IllegalStateException("Data center replication is not configured.");

        return drSndMetrics;
    }

    /**
     * @return DR sender cache metrics adapter.
     */
    @Nullable GridDrSenderCacheMetricsAdapter drSendMetrics0() {
        return drSndMetrics;
    }

    /** {@inheritDoc} */
    @Override public GridDrReceiverCacheMetrics drReceiveMetrics() {
        if (drRcvMetrics == null)
            throw new IllegalStateException("Data center replication is not configured.");

        return drRcvMetrics;
    }

    /**
     * @return DR receiver cache metrics adapter.
     */
    @Nullable GridDrReceiverCacheMetricsAdapter drReceiveMetrics0() {
        return drRcvMetrics;
    }

    /**
     * Cache read callback.
     * @param isHit Hit or miss flag.
     */
    public void onRead(boolean isHit) {
        readTime = U.currentTimeMillis();

        reads++;

        if (isHit)
            hits++;
        else
            misses++;

        if (delegate != null)
            delegate.onRead(isHit);
    }

    /**
     * Cache write callback.
     */
    public void onWrite() {
        writeTime = U.currentTimeMillis();

        writes++;

        if (delegate != null)
            delegate.onWrite();
    }

    /**
     * Transaction commit callback.
     */
    public void onTxCommit() {
        commitTime = U.currentTimeMillis();

        txCommits++;

        if (delegate != null)
            delegate.onTxCommit();
    }

    /**
     * Transaction rollback callback.
     */
    public void onTxRollback() {
        rollbackTime = U.currentTimeMillis();

        txRollbacks++;

        if (delegate != null)
            delegate.onTxRollback();
    }

    /**
     * Callback for received acknowledgement by sender hub.
     *
     * @param entriesCnt Number of entries in batch.
     */
    public void onSenderCacheBatchAcknowledged(int entriesCnt) {
        if (drSndMetrics != null) // TODO 9341
            drSndMetrics.onBatchAcked(entriesCnt);

        if (delegate != null)
            delegate.onSenderCacheBatchAcknowledged(entriesCnt);
    }

    /**
     * Callback for received batch error by sender hub.
     *
     * @param entriesCnt Number of entries in batch.
     */
    public void onSenderCacheBatchFailed(int entriesCnt) {
        if (drSndMetrics != null) // TODO 9341
            drSndMetrics.onBatchFailed(entriesCnt);

        if (delegate != null)
            delegate.onSenderCacheBatchFailed(entriesCnt);
    }

    /**
     * Callback for sent batch on sender cache side.
     *
     * @param entriesCnt Number of sent entries.
     */
    public void onSenderCacheBatchSent(int entriesCnt) {
        if (drSndMetrics != null)  // TODO 9341
            drSndMetrics.onBatchSent(entriesCnt);

        if (delegate != null)
            delegate.onSenderCacheBatchSent(entriesCnt);
    }

    /**
     * Callback for filtered entries on sender cache side.
     */
    public void onSenderCacheEntryFiltered() {
        if (drSndMetrics != null) // TODO 9341
            drSndMetrics.onEntryFiltered();

        if (delegate != null)
            delegate.onSenderCacheEntryFiltered();
    }

    /**
     * Callback for replication pause state changed.
     *
     * @param pauseReason Pause reason or {@code null} if replication is not paused.
     * @param errMsg Error message.
     */
    public void onPauseStateChanged(@Nullable GridDrPauseReason pauseReason, @Nullable String errMsg) {
        if (drSndMetrics != null) // TODO 9341
            drSndMetrics.onPauseStateChanged(pauseReason, errMsg);

        if (delegate != null)
            delegate.onPauseStateChanged(pauseReason, errMsg);
    }

    /**
     * Callback for conflict resolver on receiver cache side.
     *
     * @param usedNew New conflict status flag.
     * @param usedOld Old conflict status flag.
     * @param usedMerge Merge conflict status flag.
     */
    public void onReceiveCacheConflictResolved(boolean usedNew, boolean usedOld, boolean usedMerge) {
        if (drRcvMetrics != null)
            drRcvMetrics.onReceiveCacheConflictResolved(usedNew, usedOld, usedMerge);

        if (delegate != null)
            delegate.onReceiveCacheConflictResolved(usedNew, usedOld, usedMerge);
    }

    /**
     * Callback for received entries from receiver hub.
     *
     * @param entriesCnt Number of received entries.
     */
    public void onReceiveCacheEntriesReceived(int entriesCnt) {
        if (drRcvMetrics != null)
            drRcvMetrics.onReceiveCacheEntriesReceived(entriesCnt);

        if (delegate != null)
            delegate.onReceiveCacheEntriesReceived(entriesCnt);
    }

    /**
     * Create a copy of given metrics object.
     *
     * @param m Metrics to copy from.
     * @return Copy of given metrics.
     */
    @Nullable public static GridCacheMetricsAdapter copyOf(@Nullable GridCacheMetrics m) {
        if (m == null)
            return null;

        return new GridCacheMetricsAdapter(m);
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeLong(createTime);
        out.writeLong(readTime);
        out.writeLong(writeTime);
        out.writeLong(commitTime);
        out.writeLong(rollbackTime);

        out.writeInt(reads);
        out.writeInt(writes);
        out.writeInt(hits);
        out.writeInt(misses);
        out.writeInt(txCommits);
        out.writeInt(txRollbacks);

        out.writeObject(drSndMetrics);
        out.writeObject(drRcvMetrics);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        createTime = in.readLong();
        readTime = in.readLong();
        writeTime = in.readLong();
        commitTime = in.readLong();
        rollbackTime = in.readLong();

        reads = in.readInt();
        writes = in.readInt();
        hits = in.readInt();
        misses = in.readInt();
        txCommits = in.readInt();
        txRollbacks = in.readInt();

        drSndMetrics = (GridDrSenderCacheMetricsAdapter)in.readObject();
        drRcvMetrics = (GridDrReceiverCacheMetricsAdapter)in.readObject();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheMetricsAdapter.class, this);
    }
}
