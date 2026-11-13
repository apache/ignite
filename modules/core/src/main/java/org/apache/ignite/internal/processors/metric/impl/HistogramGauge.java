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

package org.apache.ignite.internal.processors.metric.impl;

import java.util.function.Consumer;
import java.util.function.LongConsumer;
import org.apache.ignite.internal.processors.metric.AbstractMetric;
import org.apache.ignite.internal.processors.metric.ConfigurableHistogramMetric;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.jetbrains.annotations.Nullable;

/**
 * Histogram of the values supplied at the time of reading: unlike {@link HistogramMetricImpl}, it does not accumulate
 * measurements.
 */
public class HistogramGauge extends AbstractMetric implements ConfigurableHistogramMetric {
    /** Source of the values to distribute over the bounds intervals: passes them to the given consumer. */
    private final Consumer<LongConsumer> src;

    /** */
    private volatile long[] bounds;

    /**
     * @param name Name.
     * @param desc Description.
     * @param bounds Bounds.
     * @param src Source of the values to distribute over the bounds intervals: passes them to the given consumer.
     */
    public HistogramGauge(String name, @Nullable String desc, long[] bounds, Consumer<LongConsumer> src) {
        super(name, desc);

        this.bounds = bounds;
        this.src = src;
    }

    /** {@inheritDoc} */
    @Override public long[] value() {
        long[] bounds = this.bounds;

        long[] res = new long[bounds.length + 1];

        src.accept(val -> {
            int idx = 0;

            while (idx < bounds.length && val > bounds[idx])
                idx++;

            res[idx]++;
        });

        return res;
    }

    /** {@inheritDoc} */
    @Override public long[] bounds() {
        return bounds;
    }

    /** {@inheritDoc} */
    @Override public void bounds(long[] bounds) {
        checkBounds(bounds);

        this.bounds = bounds;
    }

    /** {@inheritDoc} */
    @Override public void checkBounds(long[] bounds) {
        A.notEmpty(bounds, "bounds");
        A.ensure(F.isSorted(bounds), "bounds should be sorted");
    }

    /** {@inheritDoc} */
    @Override public Class<long[]> type() {
        return long[].class;
    }
}
