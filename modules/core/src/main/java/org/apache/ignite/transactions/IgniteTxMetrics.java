/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.transactions;

import java.io.*;

/**
 * Transaction metrics, shared across all caches.
 */
public interface IgniteTxMetrics extends Serializable {
    /**
     * Gets last time transaction was committed.
     *
     * @return Last commit time.
     */
    public long commitTime();

    /**
     * Gets last time transaction was rollback.
     *
     * @return Last rollback time.
     */
    public long rollbackTime();

    /**
     * Gets total number of transaction commits.
     *
     * @return Number of transaction commits.
     */
    public int txCommits();

    /**
     * Gets total number of transaction rollbacks.
     *
     * @return Number of transaction rollbacks.
     */
    public int txRollbacks();
}
