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

import static java.util.Arrays.copyOf;

/**
 *  Page lock stack build in on offheap.
 */
public class HeapArrayLockStack extends LockStack {
    /** */
    private final long[] pageIdLocksStack;

    /**
     * @param name Page lock tracker name.
     * @param capacity Capacity.
     */
    public HeapArrayLockStack(String name, int capacity) {
        super(name, capacity);

        this.pageIdLocksStack = new long[capacity];
    }

    /** {@inheritDoc} */
    @Override protected long getByIndex(int idx) {
        return pageIdLocksStack[idx];
    }

    /** {@inheritDoc} */
    @Override protected void setByIndex(int idx, long val) {
        pageIdLocksStack[idx] = val;
    }

    /** {@inheritDoc} */
    @Override protected void free() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public PageLockStackSnapshot snapshot() {
        long[] stack = pageIdLocksStack.clone();

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
