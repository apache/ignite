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

package org.apache.ignite.internal.mem;

import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 *
 */
public class UnsafeChunk implements DirectMemoryRegion {
    /** */
    private long ptr;

    /** */
    private long len;

    /**
     * @param ptr Pointer to the memory start.
     * @param len Memory length.
     */
    public UnsafeChunk(long ptr, long len) {
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
    @Override public DirectMemoryRegion slice(long offset) {
        if (offset < 0 || offset >= len)
            throw new IllegalArgumentException("Failed to create a memory region slice [ptr=" + U.hexLong(ptr) +
                ", len=" + len + ", offset=" + offset + ']');

        return new UnsafeChunk(ptr + offset, len - offset);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(UnsafeChunk.class, this);
    }
}
