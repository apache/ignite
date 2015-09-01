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

import org.jetbrains.annotations.Nullable;

/**
 * Cache write ordering mode. This enumeration is taken into account only in {@link CacheAtomicityMode#ATOMIC}
 * atomicity mode. Write ordering mode determines which node assigns the write version, sender or the primary node.
 * <p>
 * For example, {@link #CLOCK} mode assigns write versions on a sender node which generally leads to better
 * performance in {@link CacheWriteSynchronizationMode#FULL_SYNC} synchronization mode, since in this case
 * sender can send write requests to primary and backups at the same time. Otherwise, if ordering mode is
 * {@link #PRIMARY}, it would only send request to primary node, which in turn will assign write version
 * and forward it to backups.
 * <p>
 * {@link #CLOCK} mode will be automatically configured only with {@link CacheWriteSynchronizationMode#FULL_SYNC}
 * write synchronization mode, as for other synchronization modes it does not render better performance.
 */
public enum CacheAtomicWriteOrderMode {
    /**
     * In this mode, write versions are assigned on a sender node which generally leads to better
     * performance in {@link CacheWriteSynchronizationMode#FULL_SYNC} synchronization mode, since in this case
     * sender can send write requests to primary and backups at the same time.
     * <p>
     * This mode will be automatically configured only with {@link CacheWriteSynchronizationMode#FULL_SYNC}
     * write synchronization mode, as for other synchronization modes it does not render better performance.
     */
    CLOCK,

    /**
     * Cache version is assigned only on primary node. This means that sender will only send write request
     * to primary node, which in turn will assign write version and forward it to backups.
     */
    PRIMARY;

    /** Enumerated values. */
    private static final CacheAtomicWriteOrderMode[] VALS = values();

    /**
     * Efficiently gets enumerated value from its ordinal.
     *
     * @param ord Ordinal value.
     * @return Enumerated value or {@code null} if ordinal out of range.
     */
    @Nullable public static CacheAtomicWriteOrderMode fromOrdinal(byte ord) {
        return ord >= 0 && ord < VALS.length ? VALS[ord] : null;
    }
}