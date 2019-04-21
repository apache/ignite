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

import com.sun.jna.NativeLong;
import com.sun.jna.Pointer;
import com.sun.jna.ptr.PointerByReference;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.internal.mem.DirectMemoryProvider;
import org.apache.ignite.internal.mem.DirectMemoryRegion;
import org.apache.ignite.internal.mem.UnsafeChunk;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.internal.util.Mem;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_OFF_HEAP_MEM_ADVICE;

/**
 * Memory provider implementation based on unsafe memory access.
 * <p>
 * Supports memory reuse semantics.
 */
public class UnsafeMemoryProvider implements DirectMemoryProvider {
    /** */
    private long[] sizes;

    /** */
    private List<DirectMemoryRegion> regions;

    /** */
    private IgniteLogger log;

    /** Flag shows if current memory provider have been already initialized. */
    private boolean isInit;

    /** */
    private int used = 0;

    /**
     * @param log Ignite logger to use.
     */
    public UnsafeMemoryProvider(IgniteLogger log) {
        this.log = log;
    }

    /** {@inheritDoc} */
    @Override public void initialize(long[] sizes) {
        if (isInit)
            return;

        this.sizes = sizes;

        regions = new ArrayList<>();

        isInit = true;
    }

    /** {@inheritDoc} */
    @Override public void shutdown(boolean deallocate) {
        if (regions != null) {
            for (Iterator<DirectMemoryRegion> it = regions.iterator(); it.hasNext(); ) {
                DirectMemoryRegion chunk = it.next();

                if (deallocate) {
                    GridUnsafe.freeMemory(chunk.address());

                    // Safety.
                    it.remove();
                }
            }

            if (!deallocate)
                used = 0;
        }
    }

    /** {@inheritDoc} */
    @Override public DirectMemoryRegion nextRegion() {
        if (used == sizes.length)
            return null;

        if (used < regions.size())
            return regions.get(used++);

        long chunkSize = sizes[regions.size()];

        long ptr = allocate(chunkSize);

        DirectMemoryRegion region = new UnsafeChunk(ptr, chunkSize);

        regions.add(region);

        used++;

        return region;
    }

    /**
     *
     * @param chunkSize Chunk size.
     * @return Offheap pointer to memory.
     */
    private long allocate(long chunkSize) {
        long ptr;

        if (U.isLinux() || U.isMacOs()) {
            boolean advice = IgniteSystemProperties.getBoolean(IGNITE_OFF_HEAP_MEM_ADVICE, false);

            PointerByReference refToPtr = new PointerByReference();

            try {
                int pageSize = Mem.getpagesize();

                if (pageSize < 0)
                    pageSize = GridUnsafe.pageSize();

                if (pageSize < 0){
                    U.warn(log,"Can not get native page size, use 4k as default.");

                    pageSize = 4096;
                }

                int ptrRes = Mem.posix_memalign(refToPtr, new NativeLong(pageSize), new NativeLong(chunkSize));

                if (ptrRes != 0) {
                    U.error(log, "Failed to allocate next memory chunk: "
                        + U.readableSize(chunkSize, true)
                        + ", res:" + ptrRes);

                    return -1;
                }

                ptr = Pointer.nativeValue(refToPtr.getValue());

                if (advice) {
                    int adviceRes = Mem.posix_madvise(ptr, chunkSize, Mem.POSIX_MADV_RANDOM);

                    if (adviceRes < 0)
                        U.error(log, "Failed to advice memory chunk: "
                            + U.readableSize(chunkSize, true)
                            + ", res:" + adviceRes);
                }
            }
            catch (IllegalArgumentException e) {
                String msg = "Failed to allocate next memory chunk: " + U.readableSize(chunkSize, true) +
                    ". Check if chunkSize is too large and 32-bit JVM is used.";

                if (regions.isEmpty())
                    throw new IgniteException(msg, e);

                U.error(log, msg);

                return -1;
            }

        }
        else {
            try {
                ptr = GridUnsafe.allocateMemory(chunkSize);
            }
            catch (IllegalArgumentException e) {
                String msg = "Failed to allocate next memory chunk: " + U.readableSize(chunkSize, true) +
                    ". Check if chunkSize is too large and 32-bit JVM is used.";

                if (regions.isEmpty())
                    throw new IgniteException(msg, e);

                U.error(log, msg);

                return -1;
            }
        }

        if (ptr <= 0) {
            U.error(log, "Failed to allocate next memory chunk: " + U.readableSize(chunkSize, true));

            return -1;
        }

        return ptr;
    }
}
