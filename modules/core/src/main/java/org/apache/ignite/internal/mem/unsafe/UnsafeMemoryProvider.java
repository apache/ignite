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
import java.util.List;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.mem.DirectMemory;
import org.apache.ignite.internal.mem.DirectMemoryFragment;
import org.apache.ignite.internal.mem.DirectMemoryProvider;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lifecycle.LifecycleAware;

/**
 *
 */
public class UnsafeMemoryProvider implements DirectMemoryProvider, LifecycleAware {
    /** */
    private final long limit;

    /** */
    private final long chunkSize;

    /** */
    private List<DirectMemoryFragment> chunks;

    /**
     * @param limit Memory limit.
     * @param chunkSize Chunk size.
     */
    public UnsafeMemoryProvider(long limit, long chunkSize) {
        this.limit = limit;
        this.chunkSize = chunkSize;
    }

    /** {@inheritDoc} */
    @Override public DirectMemory memory() {
        return new DirectMemory(false, chunks);
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteException {
        chunks = new ArrayList<>();

        long allocated = 0;

        while (allocated < limit) {
            long size = Math.min(chunkSize, limit - allocated);

            long ptr = GridUnsafe.allocateMemory(size);

            if (ptr <= 0) {
                for (DirectMemoryFragment chunk : chunks) {
                    UnsafeChunk uc = (UnsafeChunk)chunk;

                    GridUnsafe.freeMemory(uc.ptr);
                }

                throw new IgniteException("Failed to allocate memory [allocated=" + allocated +
                    ", requested=" + limit + ']');
            }

            DirectMemoryFragment chunk = new UnsafeChunk(ptr, size);

            chunks.add(chunk);

            allocated += size;
        }
    }

    /** {@inheritDoc} */
    @Override public void stop() throws IgniteException {
        for (DirectMemoryFragment chunk : chunks) {
            UnsafeChunk uc = (UnsafeChunk)chunk;

            GridUnsafe.freeMemory(uc.ptr);
        }
    }

    /**
     *
     */
    private static class UnsafeChunk implements DirectMemoryFragment {
        /** */
        private long ptr;

        /** */
        private long len;

        /**
         * @param ptr Pointer to the memory start.
         * @param len Memory length.
         */
        private UnsafeChunk(long ptr, long len) {
            this.ptr = ptr;
            this.len = len;
        }

        /** {@inheritDoc} */
        @Override public long address() {
            return ptr;
        }

        /** {@inheritDoc} */
        @Override public long size() {
            return len;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(UnsafeChunk.class, this);
        }
    }
}
