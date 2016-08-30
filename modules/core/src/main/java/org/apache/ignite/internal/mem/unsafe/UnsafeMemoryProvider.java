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

package org.apache.ignite.internal.mem.unsafe;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.mem.DirectMemory;
import org.apache.ignite.internal.mem.DirectMemoryRegion;
import org.apache.ignite.internal.mem.DirectMemoryProvider;
import org.apache.ignite.internal.mem.UnsafeChunk;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.lifecycle.LifecycleAware;

/**
 *
 */
public class UnsafeMemoryProvider implements DirectMemoryProvider, LifecycleAware {
    /** */
    private final long[] sizes;

    /** */
    private List<DirectMemoryRegion> regions;

    /**
     * @param sizes Sizes of segments.
     */
    public UnsafeMemoryProvider(long[] sizes) {
        this.sizes = sizes;
    }

    /** {@inheritDoc} */
    @Override public DirectMemory memory() {
        return new DirectMemory(false, regions);
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteException {
        regions = new ArrayList<>();

        long allocated = 0;

        for (long size : sizes) {
            long ptr = GridUnsafe.allocateMemory(size);

            if (ptr <= 0) {
                for (DirectMemoryRegion region : regions)
                    GridUnsafe.freeMemory(region.address());

                throw new IgniteException("Failed to allocate memory [allocated=" + allocated +
                    ", requested=" + size + ']');
            }

            DirectMemoryRegion chunk = new UnsafeChunk(ptr, size);

            regions.add(chunk);

            allocated += size;
        }
    }

    /** {@inheritDoc} */
    @Override public void stop() throws IgniteException {
        for (Iterator<DirectMemoryRegion> it = regions.iterator(); it.hasNext(); ) {
            DirectMemoryRegion chunk = it.next();

            GridUnsafe.freeMemory(chunk.address());

            // Safety.
            it.remove();
        }
    }
}
