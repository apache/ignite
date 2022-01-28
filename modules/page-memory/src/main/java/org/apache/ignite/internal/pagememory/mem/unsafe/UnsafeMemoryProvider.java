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

package org.apache.ignite.internal.pagememory.mem.unsafe;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.ignite.internal.pagememory.mem.DirectMemoryProvider;
import org.apache.ignite.internal.pagememory.mem.DirectMemoryRegion;
import org.apache.ignite.internal.pagememory.mem.MemoryAllocator;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.lang.IgniteLogger;
import org.jetbrains.annotations.Nullable;

/**
 * Memory provider implementation based on unsafe memory access.
 *
 * <p>Supports memory reuse semantics.
 */
public class UnsafeMemoryProvider implements DirectMemoryProvider {
    /** Logger.*/
    private static final IgniteLogger LOG = IgniteLogger.forClass(UnsafeMemoryProvider.class);

    /** Array with memory regions sizes. */
    private long[] sizes;

    /** List of allocated memory regions. */
    private List<DirectMemoryRegion> regions;

    /** Flag shows if current memory provider have been already initialized. */
    private boolean isInit;

    /** Number of used data regions in {@link #regions} list. */
    private int used = 0;

    /** Memory allocator. */
    private final MemoryAllocator allocator;

    /**
     * Public constructor.
     *
     * @param allocator Memory allocator. If {@code null}, default {@link UnsafeMemoryAllocator} will be used.
     */
    public UnsafeMemoryProvider(@Nullable MemoryAllocator allocator) {
        this.allocator = allocator == null ? new UnsafeMemoryAllocator() : allocator;
    }

    /** {@inheritDoc} */
    @Override public void initialize(long[] sizes) {
        if (isInit) {
            return;
        }

        this.sizes = sizes;

        regions = new ArrayList<>();

        isInit = true;
    }

    /** {@inheritDoc} */
    @Override public void shutdown(boolean deallocate) {
        if (!deallocate) {
            used = 0;

            return;
        }

        if (regions != null) {
            for (Iterator<DirectMemoryRegion> it = regions.iterator(); it.hasNext(); ) {
                DirectMemoryRegion chunk = it.next();

                allocator.freeMemory(chunk.address());

                // Safety.
                it.remove();
            }
        }
    }

    /** {@inheritDoc} */
    @Override public DirectMemoryRegion nextRegion() {
        if (used == sizes.length) {
            return null;
        }

        if (used < regions.size()) {
            return regions.get(used++);
        }

        long chunkSize = sizes[regions.size()];

        long ptr;

        try {
            ptr = allocator.allocateMemory(chunkSize);
        } catch (IllegalArgumentException e) {
            String msg = "Failed to allocate next memory chunk: " + IgniteUtils.readableSize(chunkSize, true)
                    + ". Check if chunkSize is too large and 32-bit JVM is used.";

            if (regions.isEmpty()) {
                throw new IgniteInternalException(msg, e);
            }

            LOG.error(msg);

            return null;
        }

        if (ptr <= 0) {
            LOG.error("Failed to allocate next memory chunk: " + IgniteUtils.readableSize(chunkSize, true));

            return null;
        }

        DirectMemoryRegion region = new UnsafeChunk(ptr, chunkSize);

        regions.add(region);

        used++;

        return region;
    }
}
