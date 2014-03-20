/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache;

import org.gridgain.grid.dr.cache.receiver.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jdk8.backport.*;
import org.jetbrains.annotations.*;

import java.io.*;

/**
 * Adapter for DR receive data node metrics.
 */
class GridCacheDrReceiverMetricsAdapter implements GridDrReceiverCacheMetrics, Externalizable {
    /** Total amount of received cache entries. */
    private LongAdder entriesReceived = new LongAdder();

    /** Total amount of conflicts resolved by using new value. */
    private LongAdder conflictNew = new LongAdder();

    /** Total amount of conflicts resolved by using old value. */
    private LongAdder conflictOld = new LongAdder();

    /** Total amount of conflicts resolved by merging values. */
    private LongAdder conflictMerge = new LongAdder();

    /**
     * No-args constructor.
     */
    public GridCacheDrReceiverMetricsAdapter() {
        // No-op.
    }

    /**
     * @param m Metrics to copy from.
     */
    GridCacheDrReceiverMetricsAdapter(GridDrReceiverCacheMetrics m) {
        entriesReceived.add(m.entriesReceived());
        conflictNew.add(m.conflictNew());
        conflictOld.add(m.conflictOld());
        conflictMerge.add(m.conflictMerge());
    }

    /** {@inheritDoc} */
    @Override public long entriesReceived() {
        return entriesReceived.longValue();
    }

    /** {@inheritDoc} */
    @Override public long conflictNew() {
        return conflictNew.longValue();
    }

    /** {@inheritDoc} */
    @Override public long conflictOld() {
        return conflictOld.longValue();
    }

    /** {@inheritDoc} */
    @Override public long conflictMerge() {
        return conflictMerge.longValue();
    }

    /**
     * Callback for conflict resolver on receiver cache side.
     *
     * @param usedNew New conflict status flag.
     * @param usedOld Old conflict status flag.
     * @param usedMerge Merge conflict status flag.
     */
    public void onReceiveCacheConflictResolved(boolean usedNew, boolean usedOld, boolean usedMerge) {
        if (usedNew)
            conflictNew.increment();
        else if (usedOld)
            conflictOld.increment();
        else if (usedMerge)
            conflictMerge.increment();
    }

    /**
     * Callback for received entries from receiver hub.
     *
     * @param entriesCnt Number of received entries.
     */
    public void onReceiveCacheEntriesReceived(int entriesCnt) {
        entriesReceived.add(entriesCnt);
    }

    /**
     * Create a copy of given metrics object.
     *
     * @param m Metrics to copy from.
     * @return Copy of given metrics.
     */
    @Nullable public static GridCacheDrReceiverMetricsAdapter copyOf(@Nullable GridDrReceiverCacheMetrics m) {
        if (m == null)
            return null;

        return new GridCacheDrReceiverMetricsAdapter(m);
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeLong(entriesReceived.longValue());
        out.writeLong(conflictNew.longValue());
        out.writeLong(conflictOld.longValue());
        out.writeLong(conflictMerge.longValue());
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        entriesReceived.add(in.readInt());
        conflictNew.add(in.readInt());
        conflictOld.add(in.readLong());
        conflictMerge.add(in.readInt());
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheDrReceiverMetricsAdapter.class, this);
    }
}
