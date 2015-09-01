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
 * Mode indicating how Ignite should wait for write replies from other nodes. Default
 * value is {@link #FULL_ASYNC}}, which means that Ignite will not wait for responses from
 * participating nodes. This means that by default remote nodes may get their state updated slightly after
 * any of the cache write methods complete, or after {@link Transaction#commit()} method completes.
 * <p>
 * Note that regardless of write synchronization mode, cache data will always remain fully
 * consistent across all participating nodes.
 * <p>
 * Write synchronization mode may be configured via {@link org.apache.ignite.configuration.CacheConfiguration#getWriteSynchronizationMode()}
 * configuration property.
 */
public enum CacheWriteSynchronizationMode {
    /**
     * Flag indicating that Ignite should wait for write or commit replies from all nodes.
     * This behavior guarantees that whenever any of the atomic or transactional writes
     * complete, all other participating nodes which cache the written data have been updated.
     */
    FULL_SYNC,

    /**
     * Flag indicating that Ignite will not wait for write or commit responses from participating nodes,
     * which means that remote nodes may get their state updated a bit after any of the cache write methods
     * complete, or after {@link Transaction#commit()} method completes.
     */
    FULL_ASYNC,

    /**
     * This flag only makes sense for {@link CacheMode#PARTITIONED} mode. When enabled, Ignite
     * will wait for write or commit to complete on {@code primary} node, but will not wait for
     * backups to be updated.
     */
    PRIMARY_SYNC;

    /** Enumerated values. */
    private static final CacheWriteSynchronizationMode[] VALS = values();

    /**
     * Efficiently gets enumerated value from its ordinal.
     *
     * @param ord Ordinal value.
     * @return Enumerated value or {@code null} if ordinal out of range.
     */
    @Nullable public static CacheWriteSynchronizationMode fromOrdinal(int ord) {
        return ord >= 0 && ord < VALS.length ? VALS[ord] : null;
    }
}