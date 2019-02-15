/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.configuration;

import org.apache.ignite.Ignite;
import org.jetbrains.annotations.Nullable;

/**
 * WAL Mode. This enum defines crash recovery guarantees when Ignite persistence is enabled.
 */
public enum WALMode {
    /**
     * FSYNC mode: full-sync disk writes. These writes survive power loss scenarios. When a control is returned
     * from the transaction commit operation, the changes are guaranteed to be persisted to disk according to the
     * transaction write synchronization mode.
     */
    FSYNC,

    /**
     * Log only mode: flushes application buffers. These writes survive process crash. When a control is returned
     * from the transaction commit operation, the changes are guaranteed to be forced to the OS buffer cache.
     * It's up to the OS to decide when to flush its caches to disk.
     */
    LOG_ONLY,

    /**
     * Background mode. Does not force application's buffer flush. Last updates may be lost in case of a process crash.
     */
    BACKGROUND,

    /**
     * WAL is disabled. Data is guaranteed to be persisted on disk only in case of graceful cluster shutdown using
     * {@link Ignite#active(boolean)} method. If an Ignite node is terminated in NONE mode abruptly, it is likely
     * that the data stored on disk is corrupted and work directory will need to be cleared for a node restart.
     */
    NONE,

    /**
     * Default mode: full-sync disk writes. These writes survive power loss scenarios. When a control is returned
     * from the transaction commit operation, the changes are guaranteed to be persisted to disk according to the
     * transaction write synchronization mode.
     * @deprecated This mode is no longer default and left here only for API compatibility. It is equivalent to the
     * {@code FSYNC} mode.
     */
    @Deprecated DEFAULT;

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
