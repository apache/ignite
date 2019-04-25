/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.platform.memory;

import static org.apache.ignite.internal.processors.platform.memory.PlatformMemoryUtils.reallocateUnpooled;
import static org.apache.ignite.internal.processors.platform.memory.PlatformMemoryUtils.releaseUnpooled;

/**
 * Interop un-pooled memory chunk.
 */
public class PlatformUnpooledMemory extends PlatformAbstractMemory {
    /**
     * Constructor.
     *
     * @param memPtr Cross-platform memory pointer.
     */
    public PlatformUnpooledMemory(long memPtr) {
        super(memPtr);
    }

    /** {@inheritDoc} */
    @Override public void reallocate(int cap) {
        // Try doubling capacity to avoid excessive allocations.
        int doubledCap = PlatformMemoryUtils.capacity(memPtr) << 1;

        if (doubledCap > cap)
            cap = doubledCap;

        reallocateUnpooled(memPtr, cap);
    }

    /** {@inheritDoc} */
    @Override public void close() {
        releaseUnpooled(memPtr);
    }
}
