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

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import org.apache.ignite.internal.processors.metrics.AbstractMetric;
import org.apache.ignite.spi.metric.IntMetric;

/**
 * Int counter.
 */
public class IntCounter extends AbstractMetric implements IntMetric, Counter {
    /** Field updater. */
    private static AtomicIntegerFieldUpdater<IntCounter> updater =
        AtomicIntegerFieldUpdater.newUpdater(IntCounter.class, "value");

    /**
     * Value.
     */
    private volatile int value;

    /**
     * @param name Name.
     * @param description Description.
     */
    public IntCounter(String name, String description) {
        super(name, description);
    }

    /**
     * Adds x to the counter.
     *
     * @param x Value to be added.
     */
    public void add(int x) {
        updater.addAndGet(this, x);
    }

    /** {@inheritDoc} */
    @Override public void reset() {
        updater.set(this, 0);
    }

    /** {@inheritDoc} */
    @Override public int value() {
        return value;
    }
}
