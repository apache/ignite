/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.metric.impl;

import java.util.concurrent.atomic.DoubleAccumulator;
import org.apache.ignite.internal.processors.metric.AbstractMetric;
import org.apache.ignite.spi.metric.DoubleMetric;
import org.jetbrains.annotations.Nullable;

/**
 * Double metric.
 */
public class DoubleMetricImpl extends AbstractMetric implements DoubleMetric {
    /** Value. */
    private volatile DoubleAccumulator val;

    /**
     * @param name Name.
     * @param desc Description.
     */
    public DoubleMetricImpl(String name, @Nullable String desc) {
        super(name, desc);

        this.val = new DoubleAccumulator(Double::sum, 0d);
    }

    /**
     * Adds x to the metric.
     *
     * @param x Value to be added.
     */
    public void add(double x) {
        val.accumulate(x);
    }

    /**
     * Sets value.
     *
     * @param val Value.
     */
    public void value(double val) {
        this.val = new DoubleAccumulator(Double::sum, val);
    }

    /** {@inheritDoc} */
    @Override public void reset() {
        val.reset();
    }

    /** {@inheritDoc} */
    @Override public double value() {
        return val.doubleValue();
    }
}
