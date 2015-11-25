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
    public long get() {
        return atomicLong.get();
    }

    /**
     * Increments the value.
     *
     * @return Current atomic long value.
     */
    public long incrementAndGet() {
        return atomicLong.incrementAndGet();
    }

    /**
     * Increments the value.
     *
     * @return Original atomic long value.
     */
    public long getAndIncrement() {
        return atomicLong.getAndIncrement();
    }

    /**
     * Adds a value.
     *
     * @return Current atomic long value.
     */
    public long addAndGet(long val) {
        return atomicLong.addAndGet(val);
    }

    /**
     * Adds a value.
     *
     * @return Original atomic long value.
     */
    public long getAndAdd(long val) {
        return atomicLong.getAndAdd(val);
    }

    /**
     * Decrements the value.
     *
     * @return Current atomic long value.
     */
    public long decrementAndGet() {
        return atomicLong.decrementAndGet();
    }

    /**
     * Decrements the value.
     *
     * @return Original atomic long value.
     */
    public long getAndDecrement() {
        return atomicLong.getAndDecrement();
    }

    /**
     * Gets current value of atomic long and sets new value
     *
     * @return Original atomic long value.
     */
    public long getAndSet(long val) {
        return atomicLong.getAndSet(val);
    }

    /**
     * Compares two values for equality and, if they are equal, replaces the first value.
     *
     * @return Original atomic long value.
     */
    public long compareAndSetAndGet(long expVal, long newVal) {
        return atomicLong.compareAndSetAndGet(expVal, newVal);
    }

    /**
     * Compares two values for equality and, if they are equal, replaces the first value.
     *
     * @return Original atomic long value.
     */
    public boolean compareAndSet(long cmp, long val) {
        return atomicLong.compareAndSet(cmp, val);
    }

    /**
     * Gets status of atomic.
     *
     * @return {@code true} if atomic was removed from cache, {@code false} in other case.
     */
    public boolean isClosed() {
        return atomicLong.removed();
    }

    /**
     * Removes this atomic long.
     */
    public void close() {
        atomicLong.close();
    }
}
