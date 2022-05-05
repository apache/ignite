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
import org.apache.ignite.internal.mem.NumaAllocUtil;
import org.apache.ignite.internal.util.tostring.GridToStringBuilder;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.A;

/**
 * Simple NUMA allocation strategy.
 * <p>
 * Use {@link SimpleNumaAllocationStrategy#SimpleNumaAllocationStrategy(int)} to allocate memory on specific NUMA node
 * with number equals to {@code node}. Memory will be allocated using {@code void *numa_alloc_onnode(size_t, int)}
 * of {@code libnuma}.
 * <p>
 * Use {@link SimpleNumaAllocationStrategy#SimpleNumaAllocationStrategy()} to allocate memory using default NUMA
 * memory policy of current thread. Memory will be allocated using {@code void *numa_alloc(size_t)} of {@code lubnuma}.
 * Memory policy could be set by running application with {@code numactl}
 */
public class SimpleNumaAllocationStrategy implements NumaAllocationStrategy, Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private static final int NODE_NOT_SET = Integer.MAX_VALUE;

    /** */
    @GridToStringInclude
    private final int node;

    /** */
    public SimpleNumaAllocationStrategy() {
        node = NODE_NOT_SET;
    }

    /**
     * @param node Numa node to allocate on.
     */
    public SimpleNumaAllocationStrategy(int node) {
        A.ensure(node >= 0, "NUMA node number must be positive, passed instead " + node);
        A.ensure(node < NumaAllocUtil.NUMA_NODES_CNT,
            "NUMA node number must be less than NUMA_NODES_CNT=" + NumaAllocUtil.NUMA_NODES_CNT +
                ", passed instead " + node);

        this.node = node;
    }

    /** {@inheritDoc}*/
    @Override public long allocateMemory(long size) {
        if (node == NODE_NOT_SET)
            return NumaAllocUtil.allocate(size);

        return NumaAllocUtil.allocateOnNode(size, node);
    }

    /** {@inheritDoc}*/
    @Override public String toString() {
        return GridToStringBuilder.toString(SimpleNumaAllocationStrategy.class, this);
    }
}
