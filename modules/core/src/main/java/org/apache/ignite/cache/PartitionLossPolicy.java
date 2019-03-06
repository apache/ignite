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

import java.util.Collection;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.jetbrains.annotations.Nullable;

/**
 * Partition loss policy. Defines how cache will behave in a case when one or more partitions are lost
 * because of a node(s) failure.
 * <p>
 * All <code>*_SAFE</code> policies prevent a user from interaction with partial data in lost partitions until
 * {@link Ignite#resetLostPartitions(Collection)} method is called. <code>*_ALL</code> policies allow working with
 * partial data in lost partitions.
 * <p>
 * <code>READ_ONLY_*</code> and <code>READ_WRITE_*</code> policies do not automatically change partition state
 * and thus do not change rebalancing assignments for such partitions.
 *
 * @see Ignite#resetLostPartitions(Collection)
 * @see IgniteCache#lostPartitions()
 */
public enum PartitionLossPolicy {
    /**
     * All writes to the cache will be failed with an exception, reads will only be allowed for keys in
     * non-lost partitions. Reads from lost partitions will be failed with an exception.
     */
    READ_ONLY_SAFE,

    /**
     * All writes to the cache will be failed with an exception. All reads will proceed as if all partitions
     * were in a consistent state. The result of reading from a lost partition is undefined and may be different
     * on different nodes in the cluster.
     */
    READ_ONLY_ALL,

    /**
     * All reads and writes will be allowed for keys in valid partitions. All reads and writes for keys
     * in lost partitions will be failed with an exception.
     */
    READ_WRITE_SAFE,

    /**
     * All reads and writes will proceed as if all partitions were in a consistent state. The result of reading
     * from a lost partition is undefined and may be different on different nodes in the cluster.
     */
    READ_WRITE_ALL,

    /**
     * If partition is lost, reset it's state and do not clear intermediate data. The result of reading from
     * a previously lost and not cleared partition is undefined and may be different on different nodes in the
     * cluster.
     */
    IGNORE;

    /** Enumerated values. */
    private static final PartitionLossPolicy[] VALS = values();

    /**
     * Efficiently gets enumerated value from its ordinal.
     *
     * @param ord Ordinal value.
     * @return Enumerated value or {@code null} if ordinal out of range.
     */
    @Nullable public static PartitionLossPolicy fromOrdinal(byte ord) {
        return ord >= 0 && ord < VALS.length ? VALS[ord] : null;
    }
}
