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
     * Specified fully {@code ACID}-compliant transactional cache behavior. See
     * {@link Transaction} for more information about transactions.
     * <p>
     * This mode is currently the default cache atomicity mode. However, cache
     * atomicity mode will be changed to {@link #ATOMIC} starting from version {@code 5.2},
     * so it is recommended that desired atomicity mode is explicitly configured
     * instead of relying on default value.
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
     * failure {@link org.apache.ignite.internal.processors.cache.CachePartialUpdateCheckedException} will be thrown which will contain a list of keys
     * for which the update failed. It is recommended that bulk writes are used whenever multiple keys
     * need to be inserted or updated in cache, as they reduce number of network trips and provide
     * better performance.
     * <p>
     * Note that even without locking and transactions, {@code ATOMIC} mode still provides
     * full consistency guarantees across all cache nodes.
     * <p>
     * Also note that all data modifications in {@code ATOMIC} mode are guaranteed to be atomic
     * and consistent with writes to the underlying persistent store, if one is configured.
     * <p>
     * This mode is currently implemented for {@link CacheMode#PARTITIONED} caches only.
     */
    ATOMIC;

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