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

package org.apache.ignite.internal.processors.query.h2.dml;

import java.util.List;
import org.apache.ignite.internal.processors.query.h2.affinity.PartitionNode;

/**
 * Additional information about distributed update plan.
 */
public final class DmlDistributedPlanInfo {
    /** Whether update involves only replicated caches. */
    private final boolean replicatedOnly;

    /** Identifiers of caches involved in update (used for cluster nodes mapping). */
    private final List<Integer> cacheIds;

    /** Derived partitions tree. */
    private final PartitionNode partTree;

    /**
     * Constructor.
     * @param replicatedOnly Whether all caches are replicated.
     * @param cacheIds List of cache identifiers.
     * @param partTree PartitionNode tree to calculate derived partition
     *      (reference to PartitionNode#apply(java.lang.Object...)).
     */
    public DmlDistributedPlanInfo(boolean replicatedOnly, List<Integer> cacheIds, PartitionNode partTree) {
        this.replicatedOnly = replicatedOnly;
        this.cacheIds = cacheIds;
        this.partTree = partTree;
    }

    /**
     * @return {@code true} in case all involved caches are replicated.
     */
    public boolean isReplicatedOnly() {
        return replicatedOnly;
    }

    /**
     * @return cache identifiers.
     */
    public List<Integer> getCacheIds() {
        return cacheIds;
    }

    /**
     * @return PartitionNode tree to calculate derived partitions.
     */
    public PartitionNode partitionTree() {
        return partTree;
    }
}
