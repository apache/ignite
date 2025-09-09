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

package org.apache.ignite.internal.processors.query.calcite.exec.tracker;

import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.IgniteException;

/**
 * Memory allocation tracker for queries.
 * Each query can have more than one fragment and each fragment can be executed on it's own thread, implementation
 * must be thread safe.
 */
public class QueryMemoryTracker implements MemoryTracker {
    /** Parent (global) memory tracker. */
    private final MemoryTracker parent;

    /** Memory quota for each query. */
    private final long quota;

    /** Currently allocated. */
    private final AtomicLong allocated = new AtomicLong();

    /** Factory method. */
    public static MemoryTracker create(MemoryTracker parent, long quota) {
        return quota > 0 || parent != NoOpMemoryTracker.INSTANCE ?
            new QueryMemoryTracker(parent, quota) : NoOpMemoryTracker.INSTANCE;
    }

    /** */
    QueryMemoryTracker(MemoryTracker parent, long quota) {
        this.parent = parent;
        this.quota = quota;
    }

    /** {@inheritDoc} */
    @Override public void onMemoryAllocated(long size) {
        try {
            if (allocated.addAndGet(size) > quota && quota > 0)
                throw new IgniteException("Query quota exceeded [quota=" + quota + ']');

            parent.onMemoryAllocated(size);
        }
        catch (Exception e) {
            // Undo changes in case of quota exceeded.
            release(size);

            throw e;
        }
    }

    /** {@inheritDoc} */
    @Override public void onMemoryReleased(long size) {
        long released = release(size);

        if (released > 0)
            parent.onMemoryReleased(released);
    }

    /** Release size, but no more than currently allocated. */
    private long release(long size) {
        long wasAllocated;
        long released;

        do {
            wasAllocated = allocated.get();

            released = Math.min(size, wasAllocated);
        }
        while (!allocated.compareAndSet(wasAllocated, wasAllocated - released));

        return released;
    }

    /** {@inheritDoc} */
    @Override public void reset() {
        long wasAllocated = allocated.getAndSet(0);

        if (wasAllocated > 0)
            parent.onMemoryReleased(wasAllocated);
    }

    /** {@inheritDoc} */
    @Override public long allocated() {
        return allocated.get();
    }
}
