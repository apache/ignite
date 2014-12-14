/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite;

import org.apache.ignite.configuration.*;
import org.gridgain.grid.cache.*;
import org.jetbrains.annotations.*;

/**
 * Transactions facade.
 */
public interface IgniteTransactions {
    /**
     * Starts transaction with default isolation, concurrency, timeout, and invalidation policy.
     * All defaults are set in {@link GridCacheConfiguration} at startup.
     *
     * @return New transaction
     * @throws IllegalStateException If transaction is already started by this thread.
     * @throws UnsupportedOperationException If cache is {@link GridCacheAtomicityMode#ATOMIC}.
     */
    public GridCacheTx txStart() throws IllegalStateException;

    /**
     * Starts new transaction with the specified concurrency and isolation.
     *
     * @param concurrency Concurrency.
     * @param isolation Isolation.
     * @return New transaction.
     * @throws IllegalStateException If transaction is already started by this thread.
     * @throws UnsupportedOperationException If cache is {@link GridCacheAtomicityMode#ATOMIC}.
     */
    public GridCacheTx txStart(GridCacheTxConcurrency concurrency, GridCacheTxIsolation isolation);

    /**
     * Starts transaction with specified isolation, concurrency, timeout, invalidation flag,
     * and number of participating entries.
     *
     * @param concurrency Concurrency.
     * @param isolation Isolation.
     * @param timeout Timeout.
     * @param txSize Number of entries participating in transaction (may be approximate).
     * @return New transaction.
     * @throws IllegalStateException If transaction is already started by this thread.
     * @throws UnsupportedOperationException If cache is {@link GridCacheAtomicityMode#ATOMIC}.
     */
    public GridCacheTx txStart(GridCacheTxConcurrency concurrency, GridCacheTxIsolation isolation, long timeout,
        int txSize);

    /**
     * Starts {@code affinity-group-locked} transaction based on affinity key. In this mode only affinity key
     * is locked and all other entries in transaction are written without locking. However,
     * all keys in such transaction must have the same affinity key. Node on which transaction
     * is started must be primary for the given affinity key (an exception is thrown otherwise).
     * <p>
     * Since only affinity key is locked, and no individual keys, it is user's responsibility to make sure
     * there are no other concurrent explicit updates directly on individual keys participating in the
     * transaction. All updates to the keys involved should always go through {@code affinity-group-locked}
     * transaction, otherwise cache may be left in inconsistent state.
     * <p>
     * If cache sanity check is enabled ({@link IgniteConfiguration#isCacheSanityCheckEnabled()}),
     * the following checks are performed:
     * <ul>
     *     <li>
     *         An exception will be thrown if affinity key differs from one specified on transaction start.
     *     </li>
     *     <li>
     *         An exception is thrown if entry participating in transaction is externally locked at commit.
     *     </li>
     * </ul>
     *
     * @param affinityKey Affinity key for all entries updated by transaction. This node
     *      must be primary for this key.
     * @param timeout Timeout ({@code 0} for default).
     * @param txSize Number of entries participating in transaction (may be approximate), {@code 0} for default.
     * @param concurrency Transaction concurrency control.
     * @param isolation Cache transaction isolation level.
     * @return Started transaction.
     * @throws IllegalStateException If transaction is already started by this thread.
     * @throws IgniteCheckedException If local node is not primary for any of provided keys.
     * @throws UnsupportedOperationException If cache is {@link GridCacheAtomicityMode#ATOMIC}.
     */
    public GridCacheTx txStartAffinity(String cacheName, Object affinityKey, GridCacheTxConcurrency concurrency,
        GridCacheTxIsolation isolation, long timeout, int txSize) throws IllegalStateException, IgniteCheckedException;

    /**
     * Starts {@code partition-group-locked} transaction based on partition ID. In this mode the whole partition
     * is locked and all other entries in transaction are written without locking. However,
     * all keys in such transaction must belong to the same partition. Node on which transaction
     * is started must be primary for the given partition (an exception is thrown otherwise).
     * <p>
     * Since only partition is locked, and no individual keys, it is user's responsibility to make sure
     * there are no other concurrent explicit updates directly on individual keys participating in the
     * transaction. All updates to the keys involved should always go through {@code partition-group-locked}
     * transaction, otherwise, cache may be left in inconsistent state.
     * <p>
     * If cache sanity check is enabled ({@link IgniteConfiguration#isCacheSanityCheckEnabled()}),
     * the following checks are performed:
     * <ul>
     *     <li>
     *         An exception will be thrown if key partition differs from one specified on transaction start.
     *     </li>
     *     <li>
     *         An exception is thrown if entry participating in transaction is externally locked at commit.
     *     </li>
     * </ul>
     *
     * @param partId Partition id for which transaction is started. This node
     *      must be primary for this partition.
     * @param timeout Timeout ({@code 0} for default).
     * @param txSize Number of entries participating in transaction (may be approximate), {@code 0} for default.
     * @param concurrency Transaction concurrency control.
     * @param isolation Cache transaction isolation level.
     * @return Started transaction.
     * @throws IllegalStateException If transaction is already started by this thread.
     * @throws IgniteCheckedException If local node is not primary for any of provided keys.
     * @throws UnsupportedOperationException If cache is {@link GridCacheAtomicityMode#ATOMIC}.
     */
    public GridCacheTx txStartPartition(String cacheName, int partId, GridCacheTxConcurrency concurrency,
        GridCacheTxIsolation isolation, long timeout, int txSize) throws IllegalStateException, IgniteCheckedException;

    /**
     * Gets transaction started by this thread or {@code null} if this thread does
     * not have a transaction.
     *
     * @return Transaction started by this thread or {@code null} if this thread
     *      does not have a transaction.
     */
    @Nullable public GridCacheTx tx();

    /**
     * @return Transaction metrics.
     */
    public IgniteTxMetrics metrics();

    /**
     * Resets transaction metrics.
     */
    public void resetMetrics();
}
