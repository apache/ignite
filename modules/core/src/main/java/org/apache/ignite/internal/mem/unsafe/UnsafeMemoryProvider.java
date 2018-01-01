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
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.mem.DirectMemoryProvider;
import org.apache.ignite.internal.mem.DirectMemoryRegion;
import org.apache.ignite.internal.mem.UnsafeChunk;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 *
 */
public class UnsafeMemoryProvider implements DirectMemoryProvider {
    /** */
    private long[] sizes;

    /** */
    private List<DirectMemoryRegion> regions;

    /** */
    private IgniteLogger log;

    /**
     * @param log Ignite logger to use.
     */
    public UnsafeMemoryProvider(IgniteLogger log) {
        this.log = log;
    }

    /** {@inheritDoc} */
    @Override public void initialize(long[] sizes) {
        this.sizes = sizes;

        regions = new ArrayList<>();
    }

    /** {@inheritDoc} */
    @Override public void shutdown() {
        for (Iterator<DirectMemoryRegion> it = regions.iterator(); it.hasNext(); ) {
            DirectMemoryRegion chunk = it.next();

            GridUnsafe.freeMemory(chunk.address());

            // Safety.
            it.remove();
        }
    }

    /** {@inheritDoc} */
    @Override public DirectMemoryRegion nextRegion() {
        if (regions.size() == sizes.length)
            return null;

        long chunkSize = sizes[regions.size()];

        long ptr;

        try {
            ptr = GridUnsafe.allocateMemory(chunkSize);
        }
        catch (IllegalArgumentException e) {
            String msg = "Failed to allocate next memory chunk: " + U.readableSize(chunkSize, true) +
                ". Check if chunkSize is too large and 32-bit JVM is used.";

            if (regions.size() == 0)
                throw new IgniteException(msg, e);

            U.error(log, msg);

            return null;
        }

        if (ptr <= 0) {
            U.error(log, "Failed to allocate next memory chunk: " + U.readableSize(chunkSize, true));

            return null;
        }

        DirectMemoryRegion region = new UnsafeChunk(ptr, chunkSize);

        regions.add(region);

        return region;
    }
}
