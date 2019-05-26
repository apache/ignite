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

package org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.stack;

import org.apache.ignite.internal.util.GridUnsafe;

import static java.util.Arrays.copyOf;
import static org.apache.ignite.internal.pagemem.PageIdUtils.pageId;

/**
 * Page lock stack build in on offheap.
 */
public class OffHeapLockStack extends LockStack {
    /** */
    private final int stackSize;
    /** */
    private final long ptr;

    /**
     *
     */
    public OffHeapLockStack(String name, int size) {
        super(name, size);

        this.stackSize = capacity * 8 * 2;

        this.ptr = allocate(stackSize);
    }

    /** {@inheritDoc} */
    @Override public int capacity() {
        return capacity;
    }

    /** {@inheritDoc} */
    @Override protected long getByIndex(int idx) {
        return GridUnsafe.getLong(ptr + offset(idx));
    }

    /** {@inheritDoc} */
    @Override protected void setByIndex(int idx, long val) {
        GridUnsafe.putLong(ptr + offset(idx), val);
    }

    /** */
    private long offset(long headIdx) {
        return headIdx * 8;
    }

    /** */
    private long allocate(int size) {
        long ptr = GridUnsafe.allocateMemory(size);

        GridUnsafe.setMemory(ptr, size, (byte)0);

        return ptr;
    }

    /** {@inheritDoc} */
    @Override protected void free() {
        GridUnsafe.freeMemory(ptr);
    }

    /** {@inheritDoc} */
    @Override public PageLockStackSnapshot snapshot() {
        long[] stack = new long[capacity * 2];

        GridUnsafe.copyMemory(null, ptr, stack, GridUnsafe.LONG_ARR_OFF, stackSize);

        return new PageLockStackSnapshot(
            name,
            System.currentTimeMillis(),
            headIdx,
            stack,
            nextOp,
            nextOpStructureId,
            nextOpPageId
        );
    }
}
