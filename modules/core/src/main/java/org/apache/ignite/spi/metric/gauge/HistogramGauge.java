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

package org.apache.ignite.spi.metric.gauge;

import java.util.concurrent.atomic.AtomicLongArray;
import org.apache.ignite.internal.processors.metrics.AbstractMetric;
import org.apache.ignite.spi.metric.ObjectMetric;
import org.jetbrains.annotations.Nullable;

/**
 * Histogram gauge that will calculate counts of measurements that gets into each bounds interval.
 * Note, that {@link #value()} will return array length of {@code bounds.length + 1}.
 * Last element will contains count of measurements bigger then most right value of bounds.
 */
public class HistogramGauge extends AbstractMetric implements ObjectMetric<long[]>, Gauge {
    /**
     * Holder of measurements.
     */
    private volatile HistogramHolder holder;

    /**
     * @param name Name.
     * @param description Description.
     * @param bounds Bounds.
     */
    public HistogramGauge(String name, @Nullable String description, long[] bounds) {
        super(name, description);

        holder = new HistogramHolder(bounds);
    }

    /**
     * Sets value.
     *
     * @param x Value.
     */
    public void value(long x) {
        assert x >= 0;

        HistogramHolder h = holder;

        //Expect arrays of few elements.
        for (int i = 0; i < h.bounds.length; i++) {
            if (x <= h.bounds[i]) {
                h.measurements.incrementAndGet(i);

                return;
            }
        }

        h.measurements.incrementAndGet(h.bounds.length);
    }

    /**
     * Resets histogram state with the specified bounds.
     *
     * @param bounds Bounds.
     */
    public void reset(long[] bounds) {
        holder = new HistogramHolder(bounds);
    }

    /** {@inheritDoc} */
    @Override public void reset() {
        reset(holder.bounds);
    }

    /** {@inheritDoc} */
    @Override public long[] value() {
        HistogramHolder h = holder;

        long[] res = new long[h.measurements.length()];

        for (int i = 0; i < h.measurements.length(); i++) {
            res[i] = h.measurements.get(i);
        }

        return res;
    }

    /** {@inheritDoc} */
    @Override public Class<long[]> type() {
        return long[].class;
    }

    /**
     * Histogram holder.
     */
    private static class HistogramHolder {
        /**
         * Count of measurement for each bound.
         */
        public AtomicLongArray measurements;

        /**
         * Bounds of measurements.
         */
        public long[] bounds;

        public HistogramHolder(long[] bounds) {
            this.bounds = bounds;

            this.measurements = new AtomicLongArray(bounds.length + 1);
        }
    }
}
