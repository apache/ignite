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

package org.apache.ignite.internal.sql.optimizer.affinity;

import java.util.Collection;
import javax.cache.CacheException;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Partition extraction result.
 */
public class PartitionResult {
    /** Tree. */
    @GridToStringInclude
    private final PartitionNode tree;

    /** Affinity function. */
    private final PartitionTableAffinityDescriptor aff;

    /** Affinity topology version. Used within Jdbc thin partition awareness. */
    private final AffinityTopologyVersion topVer;

    /** Cache name. Used within Jdbc thin partition awareness. */
    private final String cacheName;

    /** Partitions count. Used within Jdbc thin partition awareness. */
    private final int partsCnt;

    /**
     * Constructor.
     *
     * @param tree Tree.
     * @param aff Affinity function.
     */
    public PartitionResult(PartitionNode tree, PartitionTableAffinityDescriptor aff, AffinityTopologyVersion topVer) {
        this.tree = tree;
        this.aff = aff;
        this.topVer = topVer;
        cacheName = tree != null ? tree.cacheName() : null;
        partsCnt = aff != null ? aff.parts() : 0;
    }

    /**
     * Constructor.
     *
     * @param tree Tree.
     * @param aff Affinity function.
     */

    /**
     * Constructor.
     *
     * @param tree Tree.
     * @param topVer Affinity topology version.
     * @param cacheName Cache name.
     * @param partsCnt Partitions count.
     */
    public PartitionResult(PartitionNode tree, AffinityTopologyVersion topVer, String cacheName, int partsCnt) {
        this.tree = tree;
        aff = null;
        this.topVer = topVer;
        this.cacheName = cacheName;
        this.partsCnt = partsCnt;
    }

    /**
     * Tree.
     */
    public PartitionNode tree() {
        return tree;
    }

    /**
     * @return Affinity function.
     */
    public PartitionTableAffinityDescriptor affinity() {
        return aff;
    }

    /**
     * Calculate partitions for the query.
     *
     * @param explicitParts Explicit partitions provided in SqlFieldsQuery.partitions property.
     * @param derivedParts Derived partitions found during partition pruning.
     * @param args Arguments.
     * @return Calculated partitions or {@code null} if failed to calculate and there should be a broadcast.
     */
    @SuppressWarnings("ZeroLengthArrayAllocation")
    public static int[] calculatePartitions(int[] explicitParts, PartitionResult derivedParts, Object[] args) {
        if (!F.isEmpty(explicitParts))
            return explicitParts;
        else if (derivedParts != null) {
            try {
                Collection<Integer> realParts = derivedParts.tree().apply(null, args);

                if (realParts == null)
                    return null;
                else if (realParts.isEmpty())
                    return IgniteUtils.EMPTY_INTS;
                else {
                    int[] realParts0 = new int[realParts.size()];

                    int i = 0;

                    for (Integer realPart : realParts)
                        realParts0[i++] = realPart;

                    return realParts0;
                }
            }
            catch (IgniteCheckedException e) {
                throw new CacheException("Failed to calculate derived partitions for query.", e);
            }
        }

        return null;
    }

    /**
     * @return Affinity topology version. This method is intended to be used within the Jdbc thin client.
     */
    public AffinityTopologyVersion topologyVersion() {
        return topVer;
    }

    /**
     * @return Cache name. This method is intended to be used within the Jdbc thin client.
     */
    public String cacheName() {
        return cacheName;
    }

    /**
     * @return Partitions count. This method is intended to be used within the Jdbc thin client.
     */
    public int partitionsCount() {
        return partsCnt;
    }

    /**
     * @return True if applicable to jdbc thin client side partition awareness:
     *   1. Rendezvous affinity function without map filters was used;
     *   2. Partition result tree neither PartitoinAllNode nor PartitionNoneNode;
     */
    public boolean isClientPartitionAwarenessApplicable() {
        return aff != null && aff.isClientPartitionAwarenessApplicable() &&
            !(tree instanceof PartitionNoneNode) && !(tree instanceof PartitionAllNode);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(PartitionResult.class, this);
    }
}
