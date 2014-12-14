/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.cache;

import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;

/**
 * Tx metrics adapter.
 */
public class IgniteTxMetricsAdapter implements IgniteTxMetrics, Externalizable {
    /** Number of transaction commits. */
    private volatile int txCommits;

    /** Number of transaction rollbacks. */
    private volatile int txRollbacks;

    /** Last commit time. */
    private volatile long commitTime;

    /** Last rollback time. */
    private volatile long rollbackTime;

    /**
     *
     */
    public IgniteTxMetricsAdapter() {

    }

    /**
     * @param m Transaction metrics to copy.
     */
    public IgniteTxMetricsAdapter(IgniteTxMetrics m) {
        commitTime = m.commitTime();
        rollbackTime = m.rollbackTime();
        txCommits = m.txCommits();
        txRollbacks = m.txRollbacks();
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
    @Override public int txCommits() {
        return txCommits;
    }

    /** {@inheritDoc} */
    @Override public int txRollbacks() {
        return txRollbacks;
    }

    /**
     * Transaction commit callback.
     */
    public void onTxCommit() {
        commitTime = U.currentTimeMillis();

        txCommits++;
    }

    /**
     * Transaction rollback callback.
     */
    public void onTxRollback() {
        rollbackTime = U.currentTimeMillis();

        txRollbacks++;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeLong(commitTime);
        out.writeLong(rollbackTime);
        out.writeInt(txCommits);
        out.writeInt(txRollbacks);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        commitTime = in.readLong();
        rollbackTime = in.readLong();
        txCommits = in.readInt();
        txRollbacks = in.readInt();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IgniteTxMetricsAdapter.class, this);
    }
}
