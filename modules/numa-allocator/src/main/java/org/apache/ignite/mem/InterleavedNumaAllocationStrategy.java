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

package org.apache.ignite.mem;

import java.io.Serializable;
import java.util.Arrays;
import org.apache.ignite.internal.mem.NumaAllocUtil;
import org.apache.ignite.internal.util.tostring.GridToStringBuilder;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.A;

/**
 * Interleaved NUMA allocation strategy.
 * <p>
 * Use {@link InterleavedNumaAllocationStrategy#InterleavedNumaAllocationStrategy()} to allocate memory interleaved
 * on all available NUMA nodes. Memory will be allocated using {@code void *numa_alloc_interleaved(size_t)}
 * of {@code libnuma}.
 * <p>
 * Use {@link InterleavedNumaAllocationStrategy#InterleavedNumaAllocationStrategy(int[])} to allocate memory interleaved
 * on specified nodes.
 * {@code void *numa_alloc_interleaved_subset(size_t, struct bitmask*)} of {@code lubnuma}.
 */
public class InterleavedNumaAllocationStrategy implements NumaAllocationStrategy, Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    @GridToStringInclude
    private final int[] nodes;

    /** */
    public InterleavedNumaAllocationStrategy() {
        this(null);
    }

    /**
     * @param nodes Array of NUMA nodes to allocate on.
     */
    public InterleavedNumaAllocationStrategy(int[] nodes) {
        if (nodes != null && nodes.length > 0) {
            this.nodes = Arrays.copyOf(nodes, nodes.length);

            Arrays.sort(this.nodes);
            A.ensure(this.nodes[0] >= 0, "NUMA node number must be positive, passed instead "
                + Arrays.toString(this.nodes));
            A.ensure(this.nodes[this.nodes.length - 1] < NumaAllocUtil.NUMA_NODES_CNT,
                "NUMA node number must be less than NUMA_NODES_CNT=" + NumaAllocUtil.NUMA_NODES_CNT +
                    ", passed instead " + Arrays.toString(this.nodes));
        }
        else
            this.nodes = null;
    }

    /** {@inheritDoc}*/
    @Override public long allocateMemory(long size) {
        return NumaAllocUtil.allocateInterleaved(size, nodes);
    }

    /** {@inheritDoc}*/
    @Override public String toString() {
        return GridToStringBuilder.toString(InterleavedNumaAllocationStrategy.class, this);
    }
}
