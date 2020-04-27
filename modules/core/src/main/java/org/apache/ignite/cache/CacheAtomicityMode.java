/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.cache;

import javax.cache.processor.EntryProcessor;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.lang.IgniteExperimental;
import org.apache.ignite.transactions.Transaction;
import org.jetbrains.annotations.Nullable;

/**
 * Cache atomicity mode controls whether cache should maintain fully transactional semantics
 * or more light-weight atomic behavior. It is recommended that {@link #ATOMIC} mode is
 * used whenever transactions and explicit locking are not needed. Note that in {@link #ATOMIC}
 * mode cache will still maintain full data consistency across all cache nodes.
 * <p>
 * Cache atomicity may be set via {@link org.apache.ignite.configuration.CacheConfiguration#getAtomicityMode()}
 * configuration property.
 */
public enum CacheAtomicityMode {
    /**
     * Enables fully {@code ACID}-compliant transactional cache behavior for the key-value API.
     * <p>
     * <b>Note!</b> In this mode, transactional consistency is guaranteed for key-value API operations only.
     * To enable ACID capabilities for SQL transactions, use the {@code TRANSACTIONAL_SNAPSHOT} mode.
     * <p>
     * <b>Note!</b> This atomicity mode is not compatible with the other modes within the same transaction.
     * if a transaction is executed over multiple caches, all caches must have the same atomicity mode,
     * either {@code TRANSACTIONAL_SNAPSHOT} or {@code TRANSACTIONAL}.
     * <p>
     * See {@link Transaction} for more information about transactions.
     */
    TRANSACTIONAL,

    /**
     * Specifies atomic-only cache behaviour. In this mode distributed transactions and distributed
     * locking are not supported. Disabling transactions and locking allows to achieve much higher
     * performance and throughput ratios.
     * <p>
     * In addition to transactions and locking, one of the main differences in {@code ATOMIC} mode
     * is that bulk writes, such as {@code putAll(...)}, {@code removeAll(...)}, and {@code transformAll(...)}
     * methods, become simple batch operations which can partially fail. In case of partial
     * failure {@link CachePartialUpdateException} will be thrown
     * which will contain a list of keys for which the update failed. It is recommended that bulk writes are used
     * whenever multiple keys need to be inserted or updated in cache, as they reduce number of network trips and
     * provide better performance.
     * <p>
     * Note that even without locking and transactions, {@code ATOMIC} mode makes best effort to provide
     * full consistency guarantees across all cache nodes. However, in following scenarios (but not limited to)
     * full consistency is not possible and {@link #TRANSACTIONAL} mode should be used or custom defined recovery
     * logic should be applied to restore data consistency:
     * <ul>
     *     <li>
     *         Node that originated update has left together with at least one primary node
     *         for this update operation, and left primary node has not finished update propagation
     *         to all nodes holding backup partitions. This way backup copies may differ. And also if
     *         persistent store is configured it may come to an inconsistent state as well.
     *     </li>
     *     <li>
     *         If update originating node is alive then update is retried by default and for operations
     *         {@code put(...)}, {@code putAll(...)}, {@code remove(K, V)} and {@code removeAll(Set<K>)}
     *         all copies of partitions will come to a consistent state.
     *     </li>
     *     <li>
     *         If {@link EntryProcessor} is used and processor is not idempotent then failure of primary node
     *         may result in applying the same processor on next chosen primary which may have already been
     *         updated within current operation. If processor is not idempotent it is recommended to disable
     *         automatic retries and manually restore consistency between key-value copies in case of update failure.
     *     </li>
     *     <li>
     *         For operations {@code putIfAbsent(K, V)}, {@code replace(K, V, V)} and {@code remove(K, V)} return
     *         value on primary node crash may be incorrect because of the automatic retries. It is recommended
     *         to disable retries with {@link IgniteCache#withNoRetries()} and manually restore primary-backup
     *         consistency in case of update failure.
     *     </li>
     * </ul>
     * <p>
     * Also note that all data modifications in {@code ATOMIC} mode are guaranteed to be atomic
     * and consistent with writes to the underlying persistent store, if one is configured.
     * <p>
     * Note! Consistency behavior of atomic cache will be improved in future releases.
     *
     * @see IgniteCache#withNoRetries()
     */
    ATOMIC,

    /**
     * <b>This is an experimental feature. Transactional SQL is currently in a beta status.</b>
     * <p>
     * Specifies fully {@code ACID}-compliant transactional cache behavior for both key-value API and SQL transactions.
     * <p>
     * This atomicity mode enables multiversion concurrency control (MVCC) for the cache. In MVCC-enabled caches,
     * when a transaction updates a row, it creates a new version of that row instead of overwriting it.
     * Other users continue to see the old version of the row until the transaction is committed.
     * In this way, readers and writers do not conflict with each other and always work with a consistent dataset.
     * The old version of data is cleaned up when it's no longer accessed by anyone.
     * <p>
     * With this mode enabled, one node is elected as an MVCC coordinator. This node tracks all in-flight transactions
     * and queries executed in the cluster. Each transaction or query executed over the cache with
     * {@code TRANSACTIONAL_SNAPSHOT} mode works with a current snapshot of data generated for this transaction or query
     * by the coordinator. This snapshot ensures that the transaction works with a consistent database state
     * during its execution period.
     * <p>
     * <b>Note!</b> This atomicity mode is not compatible with the other modes within the same transaction.
     * If a transaction is executed over multiple caches, all caches must have the same atomicity mode,
     * either {@code TRANSACTIONAL_SNAPSHOT} or {@code TRANSACTIONAL}.
     * <p>
     * See {@link Transaction} for more information about transactions.
     */
    @IgniteExperimental
    TRANSACTIONAL_SNAPSHOT;

    /** Enumerated values. */
    private static final CacheAtomicityMode[] VALS = values();

    /**
     * Efficiently gets enumerated value from its ordinal.
     *
     * @param ord Ordinal value.
     * @return Enumerated value or {@code null} if ordinal out of range.
     */
    @Nullable public static CacheAtomicityMode fromOrdinal(int ord) {
        return ord >= 0 && ord < VALS.length ? VALS[ord] : null;
    }
}
