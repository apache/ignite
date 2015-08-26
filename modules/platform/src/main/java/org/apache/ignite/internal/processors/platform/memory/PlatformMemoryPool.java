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

package org.apache.ignite.internal.processors.platform.memory;

import static org.apache.ignite.internal.processors.platform.memory.PlatformMemoryUtils.*;

/**
 * Memory pool associated with a thread.
 */
public class PlatformMemoryPool {
    /** base pointer. */
    private final long poolPtr;

    /** First pooled memory chunk. */
    private PlatformPooledMemory mem1;

    /** Second pooled memory chunk. */
    private PlatformPooledMemory mem2;

    /** Third pooled memory chunk. */
    private PlatformPooledMemory mem3;

    /**
     * Constructor.
     */
    public PlatformMemoryPool() {
        poolPtr = allocatePool();

        sun.misc.Cleaner.create(this, new CleanerRunnable(poolPtr));
    }

    /**
     * Allocate memory chunk, optionally pooling it.
     *
     * @param cap Minimum capacity.
     * @return Memory chunk.
     */
    public PlatformMemory allocate(int cap) {
        long memPtr = allocatePooled(poolPtr, cap);

        // memPtr == 0 means that we failed to acquire thread-local memory chunk, so fallback to unpooled memory.
        return memPtr != 0 ? get(memPtr) : new PlatformUnpooledMemory(allocateUnpooled(cap));
    }

    /**
     * Re-allocate existing pool memory chunk.
     *
     * @param memPtr Memory pointer.
     * @param cap Minimum capacity.
     */
    void reallocate(long memPtr, int cap) {
        reallocatePooled(memPtr, cap);
    }

    /**
     * Release pooled memory chunk.
     *
     * @param memPtr Memory pointer.
     */
    void release(long memPtr) {
        releasePooled(memPtr);
    }

    /**
     * Get pooled memory chunk.
     *
     * @param memPtr Memory pointer.
     * @return Memory chunk.
     */
    public PlatformMemory get(long memPtr) {
        long delta = memPtr - poolPtr;

        if (delta == POOL_HDR_OFF_MEM_1) {
            if (mem1 == null)
                mem1 = new PlatformPooledMemory(this, memPtr);

            return mem1;
        }
        else if (delta == POOL_HDR_OFF_MEM_2) {
            if (mem2 == null)
                mem2 = new PlatformPooledMemory(this, memPtr);

            return mem2;
        }
        else {
            assert delta == POOL_HDR_OFF_MEM_3;

            if (mem3 == null)
                mem3 = new PlatformPooledMemory(this, memPtr);

            return mem3;
        }
    }

    /**
     * Cleaner runnable.
     */
    private static class CleanerRunnable implements Runnable {
        /** Pointer. */
        private final long poolPtr;

        /**
         * Constructor.
         *
         * @param poolPtr Pointer.
         */
        private CleanerRunnable(long poolPtr) {
            assert poolPtr != 0;

            this.poolPtr = poolPtr;
        }

        /** {@inheritDoc} */
        @Override public void run() {
            PlatformMemoryUtils.releasePool(poolPtr);
        }
    }
}
