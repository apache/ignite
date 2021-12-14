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

/**
 * NUMA aware memory allocator. Uses {@code libnuma} under the hood. Only Linux distros with {@code libnuma >= 2.0.x}
 * are supported.
 * <p>
 * Allocation strategy can be defined by setting {@code allocStrategy} to
 * {@link NumaAllocator#NumaAllocator(NumaAllocationStrategy)}.
 */
public class NumaAllocator implements MemoryAllocator, Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private final NumaAllocationStrategy allocStrategy;

    /**
     * @param allocStrategy Allocation strategy.
     */
    public NumaAllocator(NumaAllocationStrategy allocStrategy) {
        this.allocStrategy = allocStrategy;
    }

    /** {@inheritDoc}*/
    @Override public long allocateMemory(long size) {
        return allocStrategy.allocateMemory(size);
    }

    /** {@inheritDoc}*/
    @Override public void freeMemory(long addr) {
        NumaAllocUtil.free(addr);
    }
}
