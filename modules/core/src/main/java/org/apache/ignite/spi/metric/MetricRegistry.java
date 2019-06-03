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

package org.apache.ignite.spi.metric;

import java.util.Collection;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.DoubleSupplier;
import java.util.function.IntSupplier;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import org.apache.ignite.spi.metric.counter.DoubleCounter;
import org.apache.ignite.spi.metric.counter.HitRateCounter;
import org.apache.ignite.spi.metric.counter.IntCounter;
import org.apache.ignite.spi.metric.counter.LongCounter;
import org.apache.ignite.spi.metric.gauge.BooleanGauge;
import org.apache.ignite.spi.metric.gauge.DoubleGauge;
import org.apache.ignite.spi.metric.gauge.HistogramGauge;
import org.apache.ignite.spi.metric.gauge.IntGauge;
import org.apache.ignite.spi.metric.gauge.LongGauge;
import org.apache.ignite.spi.metric.gauge.ObjectGauge;
import org.jetbrains.annotations.Nullable;

/**
 * Groups of the metrics.
 * Logically groups metrics representing state of some system entity.
 */
public interface MetricRegistry {
    /**
     * @param prefix prefix for all metrics.
     * @return Proxy implementation that will search and create only metrics with specified prefix.
     */
    public MetricRegistry withPrefix(String prefix);

    /**
     * Prefixes combined using dot notation {@code ["io", "stat"] -> "io.stat"}
     *
     * @param prefix prefixes for all metrics.
     * @return Proxy implementation that will search and create only metrics with specified prefixes.
     */
    public MetricRegistry withPrefix(String... prefixes);

    /**
     * @return Metrics stored in this group.
     */
    public Collection<Metric> getMetrics();

    /**
     * Adds listener of metrics sets creation events.
     *
     * @param lsnr Listener.
     */
    public void addMetricCreationListener(Consumer<Metric> lsnr);

    /**
     * @param name Name of the metric
     * @return Metric with specified name if exists. Null otherwise.
     */
    @Nullable public Metric findMetric(String name);

    /**
     * Register existing metrics in this group with the specified name.
     *
     * @param metric Metric.
     */
    public void register(Metric metric);

    /**
     * Resets state of this metric set.
     */
    public default void reset() {
        getMetrics().forEach(Metric::reset);
    }

    /**
     * Register {@link BooleanMetric} that value will be queried from specified supplier.
     *
     * @param name Name.
     * @param supplier Supplier.
     * @param description Description.
     */
    public void register(String name, BooleanSupplier supplier, @Nullable String description);

    /**
     * Register {@link BooleanMetric} that value will be queried from specified supplier.
     *
     * @param name Name.
     * @param supplier Supplier.
     * @param description Description.
     */
    public void register(String name, DoubleSupplier supplier, @Nullable String description);

    /**
     * Register {@link IntMetric} that value will be queried from specified supplier.
     *
     * @param name Name.
     * @param supplier Supplier.
     * @param description Description.
     */
    public void register(String name, IntSupplier supplier, @Nullable String description);

    /**
     * Register {@link LongMetric} that value will be queried from specified supplier.
     *
     * @param name Name.
     * @param supplier Supplier.
     * @param description Description.
     */
    public void register(String name, LongSupplier supplier, @Nullable String description);

    /**
     * Register {@code ObjectMetric} that value will be queried from specified supplier.
     *
     * @param name Name.
     * @param supplier Supplier.
     * @param type Type.
     * @param description Description.
     */
    public <T> void register(String name, Supplier<T> supplier, Class<T> type, @Nullable String description);

    /**
     * Creates and register named counter.
     * Returned instance are thread safe.
     *
     * @param name Name.
     * @param description Description.
     * @return Counter.
     */
    public DoubleCounter doubleCounter(String name, @Nullable String description);

    /**
     * Creates and register named counter.
     * Returned instance are thread safe.
     *
     * @param name Name.
     * @param description Description.
     * @return Counter.
     */
    public IntCounter intCounter(String name, @Nullable String description);

    /**
     * Creates and register named counter.
     * Returned instance are thread safe.
     *
     * @param name Name.
     * @param description Description.
     * @return Counter.
     */
    public LongCounter counter(String name, @Nullable String description);

    /**
     * Creates and register named counter.
     * Returned instance are thread safe.
     *
     * @param name Name.
     * @param description Description.
     * @param updateDelegate Delegate. Will be called each time created counter value changed.
     * @return Counter.
     * @see #counter(String)
     */
    public LongCounter counter(String name, LongConsumer updateDelegate, @Nullable String description);

    /**
     * Creates and register hit rate counter.
     *
     * It will accumulates approximate hit rate statistics.
     * Calculates number of hits in last rateTimeInterval milliseconds.
     *
     * @param rateTimeInterval Rate time interval.
     * @param size Array size for underlying calculations.
     * @return Counter.
     * @see HitRateCounter
     */
    public HitRateCounter hitRateCounter(String name, @Nullable String description, long rateTimeInterval, int size);

    /**
     * Creates and register named gauge.
     * Returned instance are thread safe.
     *
     * @param name Name.
     * @param description Description.
     * @return Gauge.
     */
    public BooleanGauge booleanGauge(String name, @Nullable String description);

    /**
     * Creates and register named gauge.
     * Returned instance are thread safe.
     *
     * @param name Name.
     * @param description Description.
     * @return Gauge.
     */
    public DoubleGauge doubleGauge(String name, @Nullable String description);

    /**
     * Creates and register named gauge.
     * Returned instance are thread safe.
     *
     * @param name Name.
     * @param description Description.
     * @return Gauge.
     */
    public IntGauge intGauge(String name, @Nullable String description);

    /**
     * Creates and register named gauge.
     * Returned instance are thread safe.
     *
     * @param name Name.
     * @param description Description.
     * @return Gauge.
     */
    public LongGauge gauge(String name, @Nullable String description);

    /**
     * Creates and register named gauge.
     * Returned instance are thread safe.
     *
     * @param name Name.
     * @param type Type.
     * @param description Description.
     * @return Gauge.
     */
    public <T> ObjectGauge<T> objectGauge(String name, Class<T> type, @Nullable String description);

    /**
     * Creates and registre named histogram gauge.
     *
     * @param name Name
     * @param bounds Bounds of measurements.
     * @param description Description.
     * @return HistogramGauge.
     */
    public HistogramGauge histogram(String name, long[] bounds, @Nullable String description);
}
