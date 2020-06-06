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

import java.util.Collection;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.jetbrains.annotations.Nullable;

/**
 * Partition loss policy. Defines how a cache will behave in a case when one or more partitions are lost
 * because of a node(s) failure.
 * <p>
 * A partition is considered <em>lost</em> if all owning nodes had left a topology.
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
     *
     * @deprecated {@link #READ_ONLY_SAFE} is used instead.
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
     *
     * @deprecated {@link #READ_WRITE_SAFE} is used instead.
     */
    READ_WRITE_ALL,

    /**
     * If a partition was lost silently ignore it and allow any operations with a partition.
     * Partition loss events are not fired if using this mode.
     * For pure in-memory caches the policy will work only when baseline auto adjust is enabled with zero timeout.
     * If persistence is enabled, the policy is always ignored. READ_WRITE_SAFE is used instead.
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
