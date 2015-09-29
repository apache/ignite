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

package org.apache.ignite.internal.processors.platform.datastructures;

import org.apache.ignite.IgniteAtomicLong;
import org.apache.ignite.internal.processors.datastructures.GridCacheAtomicLongImpl;
import org.apache.ignite.internal.processors.platform.PlatformAbstractTarget;
import org.apache.ignite.internal.processors.platform.PlatformContext;

/**
 * Platform atomic long wrapper.
 */
public class PlatformAtomicLong extends PlatformAbstractTarget {
    /** */
    private final GridCacheAtomicLongImpl atomicLong;

    /**
     * Ctor.
     * @param ctx Context.
     * @param atomicLong AtomicLong to wrap.
     */
    public PlatformAtomicLong(PlatformContext ctx, GridCacheAtomicLongImpl atomicLong) {
        super(ctx);

        assert atomicLong != null;

        this.atomicLong = atomicLong;
    }

    /**
     * Reads the value.
     *
     * @return Current atomic long value.
     */
    public long read() {
        return atomicLong.get();
    }

    /**
     * Increments the value.
     *
     * @return Current atomic long value.
     */
    public long increment() {
        return atomicLong.incrementAndGet();
    }

    /**
     * Adds a value.
     *
     * @return Current atomic long value.
     */
    public long add(long val) {
        return atomicLong.addAndGet(val);
    }

    /**
     * Decrements the value.
     *
     * @return Current atomic long value.
     */
    public long decrement() {
        return atomicLong.decrementAndGet();
    }

    /**
     * Reads the value.
     *
     * @return Current atomic long value.
     */
    public long exchange(long val) {
        return atomicLong.getAndSet(val);
    }

    /**
     * Compares two values for equality and, if they are equal, replaces the first value.
     *
     * @return Original atomic long value.
     */
    public long compareExchange(long val, long cmp) {
        return atomicLong.compareAndSetAndGet(cmp, val);
    }

    /**
     * Gets status of atomic.
     *
     * @return {@code true} if atomic was removed from cache, {@code false} in other case.
     */
    public boolean isRemoved() {
        return atomicLong.removed();
    }

    /**
     * Removes this atomic long.
     */
    public void close() {
        atomicLong.close();
    }
}
