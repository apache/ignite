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

package org.apache.ignite.internal.binary.streams;

/**
 * Thread-local memory allocator.
 */
public final class BinaryMemoryAllocator {
    /** Memory allocator instance. */
    public static final BinaryMemoryAllocator INSTANCE = new BinaryMemoryAllocator();

    /** Holders. */
    private static final ThreadLocal<BinaryMemoryAllocatorChunk> holders = new ThreadLocal<>();

    /**
     * Ensures singleton.
     */
    private BinaryMemoryAllocator() {
        // No-op.
    }

    public BinaryMemoryAllocatorChunk chunk() {
        BinaryMemoryAllocatorChunk holder = holders.get();

        if (holder == null)
            holders.set(holder = new BinaryMemoryAllocatorChunk());

        return holder;
    }

    /**
     * Checks whether a thread-local array is acquired or not.
     * The function is used by Unit tests.
     *
     * @return {@code true} if acquired {@code false} otherwise.
     */
    public boolean isAcquired() {
        BinaryMemoryAllocatorChunk holder = holders.get();

        return holder != null && holder.isAcquired();
    }
}
