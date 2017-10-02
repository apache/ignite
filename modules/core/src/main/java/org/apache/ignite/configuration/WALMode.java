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

package org.apache.ignite.configuration;

import org.apache.ignite.Ignite;
import org.jetbrains.annotations.Nullable;

/**
 * WAL Mode. This enum defines crash recovery guarantees when Ignite persistence is enabled.
 */
public enum WALMode {
    /**
     * Default mode: full-sync disk writes. These writes survive power loss scenarios. When a control is returned
     * from the trnasaction commit operation, the changes are guaranteed to be forced on disk according to the
     * transaction write synchronization mode.
     */
    DEFAULT,

    /**
     * Log only mode: flushes application buffers. These writes survive process crash. When a control is returned
     * from the transaction commit operation, the changes are guaranteed to be forced to the OS buffer cache.
     */
    LOG_ONLY,

    /**
     * Background mode. Does not force application buffer flush. Last updates may be lost in case of a process crash.
     */
    BACKGROUND,

    /**
     * WAL is disabled. Data is guaranteed to be persisted on disk only in case of graceful cluster shutdown using
     * {@link Ignite#active(boolean)} method. If an Ignite node is forefully terminated in NONE mode, it is likely
     * that data stored on disk is corrupted and work directory will need to be cleared for a successfull node restart.
     */
    NONE;

    /**
     * Enumerated values.
     */
    private static final WALMode[] VALS = values();

    /**
     * Efficiently gets enumerated value from its ordinal.
     *
     * @param ord Ordinal value.
     * @return Enumerated value or {@code null} if ordinal out of range.
     */
    @Nullable public static WALMode fromOrdinal(int ord) {
        return ord >= 0 && ord < VALS.length ? VALS[ord] : null;
    }
}
