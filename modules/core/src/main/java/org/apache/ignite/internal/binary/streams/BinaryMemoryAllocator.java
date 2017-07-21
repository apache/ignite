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
