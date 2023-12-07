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
import org.apache.ignite.internal.util.typedef.internal.A;

/**
 * Memory allocation tracker for all queries.
 */
public class GlobalMemoryTracker implements MemoryTracker {
    /** Global memory quota. */
    private final long quota;

    /** Currently allocated. */
    private final AtomicLong allocated = new AtomicLong();

    /** */
    public GlobalMemoryTracker(long quota) {
        A.ensure(quota > 0, "quota > 0");
        this.quota = quota;
    }

    /** {@inheritDoc} */
    @Override public void onMemoryAllocated(long size) {
        if (allocated.addAndGet(size) > quota) {
            allocated.addAndGet(-size);

            throw new IgniteException("Global memory quota for SQL queries exceeded [quota=" + quota + ']');
        }
    }

    /** {@inheritDoc} */
    @Override public void onMemoryReleased(long size) {
        allocated.addAndGet(-size);
    }

    /** {@inheritDoc} */
    @Override public void reset() {
        allocated.set(0);
    }

    /** {@inheritDoc} */
    @Override public long allocated() {
        return allocated.get();
    }
}
