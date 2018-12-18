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

package org.apache.ignite.internal.processors.query.h2.affinity.join;

import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.internal.util.typedef.internal.S;

import java.io.Serializable;

/**
 * Affinity function descriptor. Used to compare affinity functions of two tables.
 */
public class PartitionJoinAffinityDescriptor implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Cache mode. */
    private final CacheMode cacheMode;

    /** Affinity function type. */
    private final PartitionAffinityFunctionType affFunc;

    /** Number of partitions. */
    private final int parts;

    /** Whether node filter is set. */
    private final boolean hasNodeFilter;

    /**
     * Constructor.
     *
     * @param cacheMode Cache mode.
     * @param affFunc Affinity function type.
     * @param parts Number of partitions.
     * @param hasNodeFilter Whether node filter is set.
     */
    public PartitionJoinAffinityDescriptor(
        CacheMode cacheMode,
        PartitionAffinityFunctionType affFunc,
        int parts,
        boolean hasNodeFilter
    ) {
        this.cacheMode = cacheMode;
        this.affFunc = affFunc;
        this.parts = parts;
        this.hasNodeFilter = hasNodeFilter;
    }

    /**
     * Check is provided descriptor is compatible with this instance (i.e. can be used in the same co-location group).
     *
     * @param other Other descriptor.
     * @return {@code True} if compatible.
     */
    public boolean isCompatible(PartitionJoinAffinityDescriptor other) {
        // REPLICATED caches has special treatment during parititon pruning, so exclude them.
        if (cacheMode == CacheMode.PARTITIONED) {
            // Rendezvous affinity function is deterministic and doesn't depend on previous cluster view changes.
            // In future other user affinity functions would be applicable as well if explicityl marked deterministic.
            if (affFunc == PartitionAffinityFunctionType.RENDEZVOUS) {
                // We cannot be sure that two caches are co-located if custom node filter is present.
                // Nota that technically we may try to compare two filters. However, this adds unnecessary complexity
                // and potential deserialization issues when SQL is called from client nodes or thin clients.
                if (!hasNodeFilter) {
                    return
                        other.cacheMode == CacheMode.PARTITIONED &&
                        other.affFunc == PartitionAffinityFunctionType.RENDEZVOUS &&
                        !other.hasNodeFilter &&
                        other.parts == parts;
                }
            }
        }

        return false;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(PartitionJoinAffinityDescriptor.class, this);
    }
}
