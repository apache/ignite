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

package org.apache.ignite.spi.metric.counter;

import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import org.apache.ignite.internal.processors.metric.AbstractMetric;
import org.apache.ignite.spi.metric.LongMetric;
import org.jetbrains.annotations.Nullable;

/**
 * Long counter implementation.
 */
public class LongCounter extends AbstractMetric implements LongMetric, Counter {
    /** Field updater. */
    private static final AtomicLongFieldUpdater<LongCounter> updater =
        AtomicLongFieldUpdater.newUpdater(LongCounter.class, "val");

    /** Field value. */
    private volatile long val;

    /**
     * @param name Name.
     * @param descr Description.
     */
    public LongCounter(String name, @Nullable String descr) {
        super(name, descr);
    }

    /**
     * Adds x to the counter.
     *
     * @param x Value to be added.
     */
    public void add(long x) {
        updater.getAndAdd(this, x);
    }

    /** Adds 1 to the counter. */
    public void increment() {
        add(1);
    }

    /** Adds -1 to the counter. */
    public void decrement() {
        add(-1);
    }

    /** {@inheritDoc} */
    @Override public void reset() {
        updater.set(this, 0);
    }

    /** {@inheritDoc} */
    @Override public long value() {
        return val;
    }

    /**
     * Atomically sets the value to the given updated value
     * if the current value {@code ==} the expected value.
     *
     * @param expect The expected value.
     * @param update The new value.
     * @return {@code true} if successful. False return indicates that
     * the actual value was not equal to the expected value.
     */
    public boolean compareAndSet(long expect, long update) {
        return updater.compareAndSet(this, expect, update);
    }
}
