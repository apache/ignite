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

import org.apache.ignite.cache.affinity.AffinityFunction;
import org.jetbrains.annotations.Nullable;

/**
 * Cache rebalance mode. When rebalancing is enabled (i.e. has value other than {@link #NONE}), distributed caches
 * will attempt to rebalance all necessary values from other grid nodes. This enumeration is used to configure
 * rebalancing via {@link org.apache.ignite.configuration.CacheConfiguration#getRebalanceMode()} configuration property. If not configured
 * explicitly, then {@link org.apache.ignite.configuration.CacheConfiguration#DFLT_REBALANCE_MODE} is used.
 * <p>
 * Replicated caches will try to load the full set of cache entries from other nodes (or as defined by
 * pluggable {@link AffinityFunction}), while partitioned caches will only load the entries for which
 * current node is primary or back up.
 * <p>
 * Note that rebalance mode only makes sense for {@link CacheMode#REPLICATED} and {@link CacheMode#PARTITIONED}
 * caches. Caches with {@link CacheMode#LOCAL} mode are local by definition and therefore cannot rebalance
 * any values from neighboring nodes.
 */
public enum CacheRebalanceMode {
    /**
     * Synchronous rebalance mode. Distributed caches will not start until all necessary data
     * is loaded from other available grid nodes.
     */
    SYNC,

    /**
     * Asynchronous rebalance mode. Distributed caches will start immediately and will load all necessary
     * data from other available grid nodes in the background.
     */
    ASYNC,

    /**
     * In this mode no rebalancing will take place which means that caches will be either loaded on
     * demand from persistent store whenever data is accessed, or will be populated explicitly.
     */
    NONE;

    /** Enumerated values. */
    private static final CacheRebalanceMode[] VALS = values();

    /**
     * Efficiently gets enumerated value from its ordinal.
     *
     * @param ord Ordinal value.
     * @return Enumerated value or {@code null} if ordinal out of range.
     */
    @Nullable public static CacheRebalanceMode fromOrdinal(int ord) {
        return ord >= 0 && ord < VALS.length ? VALS[ord] : null;
    }
}