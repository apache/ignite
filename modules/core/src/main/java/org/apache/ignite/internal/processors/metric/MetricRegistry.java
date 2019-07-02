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

package org.apache.ignite.internal.processors.metric;

import java.util.function.BooleanSupplier;
import java.util.function.DoubleSupplier;
import java.util.function.IntSupplier;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import org.apache.ignite.internal.processors.metric.impl.DoubleMetricImpl;
import org.apache.ignite.internal.processors.metric.impl.HitRateMetric;
import org.apache.ignite.internal.processors.metric.impl.IntMetricImpl;
import org.apache.ignite.internal.processors.metric.impl.LongAdderMetricImpl;
import org.apache.ignite.internal.processors.metric.impl.LongMetricImpl;
import org.apache.ignite.internal.processors.metric.impl.BooleanMetricImpl;
import org.apache.ignite.internal.processors.metric.impl.HistogramMetric;
import org.apache.ignite.internal.processors.metric.impl.ObjectMetricImpl;
import org.apache.ignite.spi.metric.BooleanMetric;
import org.apache.ignite.spi.metric.IntMetric;
import org.apache.ignite.spi.metric.LongMetric;
import org.apache.ignite.spi.metric.Metric;
import org.apache.ignite.spi.metric.ReadOnlyMetricRegistry;
import org.jetbrains.annotations.Nullable;

/**
 * Metrics registry.
 * Provide methods to register required metrics, gauges for Ignite internals.
 * Provide the way to obtain all registered metrics for exporters.
 */
public interface MetricRegistry extends ReadOnlyMetricRegistry {
    /**
     * Register existing metrics in this group with the specified name.
     *
     * @param metric Metric.
     */
    public void register(Metric metric);

    /**
     * Removes metrics with the {@code name} from registry.
     *
     * @param name Metric name.
     */
    public void remove(String name);

    /**
     * Resets state of this metric set.
     */
    public default void reset() {
        getMetrics().forEach(Metric::reset);
    }

    /**
     * Registers {@link BooleanMetric} which value will be queried from the specified supplier.
     *
     * @param name Name.
     * @param supplier Supplier.
     * @param description Description.
     */
    public void register(String name, BooleanSupplier supplier, @Nullable String description);

    /**
     * Registers {@link BooleanMetric} which value will be queried from the specified supplier.
     *
     * @param name Name.
     * @param supplier Supplier.
     * @param description Description.
     */
    public void register(String name, DoubleSupplier supplier, @Nullable String description);

    /**
     * Registers {@link IntMetric} which value will be queried from the specified supplier.
     *
     * @param name Name.
     * @param supplier Supplier.
     * @param description Description.
     */
    public void register(String name, IntSupplier supplier, @Nullable String description);

    /**
     * Registers {@link LongMetric} which value will be queried from the specified supplier.
     *
     * @param name Name.
     * @param supplier Supplier.
     * @param description Description.
     */
    public void register(String name, LongSupplier supplier, @Nullable String description);

    /**
     * Registers {@code ObjectMetric} which value will be queried from the specified supplier.
     *
     * @param name Name.
     * @param supplier Supplier.
     * @param type Type.
     * @param description Description.
     */
    public <T> void register(String name, Supplier<T> supplier, Class<T> type, @Nullable String description);

    /**
     * Creates and register named metric.
     * Returned instance are thread safe.
     *
     * @param name Name.
     * @param description Description.
     * @return Metric
     */
    public DoubleMetricImpl doubleMetric(String name, @Nullable String description);

    /**
     * Creates and register named metric.
     * Returned instance are thread safe.
     *
     * @param name Name.
     * @param description Description.
     * @return Metric.
     */
    public IntMetricImpl intMetric(String name, @Nullable String description);

    /**
     * Creates and register named metric.
     * Returned instance are thread safe.
     *
     * @param name Name.
     * @param description Description.
     * @return Metric.
     */
    public LongMetricImpl metric(String name, @Nullable String description);

    /**
     * Creates and register named metric.
     * Returned instance are thread safe.
     *
     * @param name Name.
     * @param description Description.
     * @return Metric
     */
    public LongAdderMetricImpl longAdderMetric(String name, @Nullable String description);

    /**
     * Creates and register hit rate metric.
     *
     * It will accumulates approximate hit rate statistics.
     * Calculates number of hits in last rateTimeInterval milliseconds.
     *
     * @param rateTimeInterval Rate time interval.
     * @param size Array size for underlying calculations.
     * @return Metric.
     * @see HitRateMetric
     */
    public HitRateMetric hitRateMetric(String name, @Nullable String description, long rateTimeInterval, int size);

    /**
     * Creates and register named gauge.
     * Returned instance are thread safe.
     *
     * @param name Name.
     * @param description Description.
     * @return Gauge.
     */
    public BooleanMetricImpl booleanMetric(String name, @Nullable String description);

    /**
     * Creates and register named gauge.
     * Returned instance are thread safe.
     *
     * @param name Name.
     * @param type Type.
     * @param description Description.
     * @return Gauge.
     */
    public <T> ObjectMetricImpl<T> objectMetric(String name, Class<T> type, @Nullable String description);

    /**
     * Creates and registre named histogram gauge.
     *
     * @param name Name
     * @param bounds Bounds of measurements.
     * @param description Description.
     * @return HistogramGauge.
     */
    public HistogramMetric histogram(String name, long[] bounds, @Nullable String description);
}
