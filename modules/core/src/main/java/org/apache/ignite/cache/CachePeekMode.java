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

package org.apache.ignite.cache;

import org.apache.ignite.IgniteCache;
import org.jetbrains.annotations.Nullable;

/**
 * Enumeration of all supported cache peek modes. Peek modes can be passed
 * into {@link IgniteCache#localPeek(Object, CachePeekMode...)},
 * {@link IgniteCache#localEntries(CachePeekMode...)},
 * {@link IgniteCache#localSize(CachePeekMode...)} and
 * {@link IgniteCache#size(CachePeekMode...)} methods.
 * <p>
 * The following modes are supported:
 * <ul>
 * <li>{@link #ALL}</li>
 * <li>{@link #NEAR}</li>
 * <li>{@link #PRIMARY}</li>
 * <li>{@link #BACKUP}</li>
 * <li>{@link #ONHEAP}</li>
 * <li>{@link #OFFHEAP}</li>
 * </ul>
 */
public enum CachePeekMode {
    /** Peeks into all available cache storages. */
    ALL,

    /**
     * Peek into near cache only (don't peek into partitioned cache).
     * In case of {@link CacheMode#LOCAL} cache, behaves as {@link #ALL} mode.
     */
    NEAR,

    /**
     * Peek value from primary copy of partitioned cache only (skip near cache).
     * In case of {@link CacheMode#LOCAL} cache, behaves as {@link #ALL} mode.
     */
    PRIMARY,

    /**
     * Peek value from backup copies of partitioned cache only (skip near cache).
     * In case of {@link CacheMode#LOCAL} cache, behaves as {@link #ALL} mode.
     */
    BACKUP,

    /** Peeks value from the on-heap storage only. */
    ONHEAP,

    /** Peeks value from the off-heap storage only, without loading off-heap value into cache. */
    OFFHEAP;

    /** Enumerated values. */
    private static final CachePeekMode[] VALS = values();

    /**
     * Efficiently gets enumerated value from its ordinal.
     *
     * @param ord Ordinal value.
     * @return Enumerated value or {@code null} if ordinal out of range.
     */
    @Nullable public static CachePeekMode fromOrdinal(byte ord) {
        return ord >= 0 && ord < VALS.length ? VALS[ord] : null;
    }
}