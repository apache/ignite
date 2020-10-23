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

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.BooleanSupplier;
import java.util.function.DoubleSupplier;
import java.util.function.IntSupplier;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.processors.metric.impl.AtomicLongMetric;
import org.apache.ignite.internal.processors.metric.impl.BooleanGauge;
import org.apache.ignite.internal.processors.metric.impl.BooleanMetricImpl;
import org.apache.ignite.internal.processors.metric.impl.DoubleGauge;
import org.apache.ignite.internal.processors.metric.impl.DoubleMetricImpl;
import org.apache.ignite.internal.processors.metric.impl.HistogramMetricImpl;
import org.apache.ignite.internal.processors.metric.impl.HitRateMetric;
import org.apache.ignite.internal.processors.metric.impl.IntGauge;
import org.apache.ignite.internal.processors.metric.impl.IntMetricImpl;
import org.apache.ignite.internal.processors.metric.impl.LongAdderMetric;
import org.apache.ignite.internal.processors.metric.impl.LongAdderWithDelegateMetric;
import org.apache.ignite.internal.processors.metric.impl.LongGauge;
import org.apache.ignite.internal.processors.metric.impl.ObjectGauge;
import org.apache.ignite.internal.processors.metric.impl.ObjectMetricImpl;
import org.apache.ignite.spi.metric.HistogramMetric;
import org.apache.ignite.spi.metric.Metric;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.metricName;
import static org.apache.ignite.internal.util.lang.GridFunc.nonThrowableSupplier;

public class MetricRegistryBuilder {
    /** Registry name. */
    private String name;

    /** Logger. */
    private IgniteLogger log;

    /** Registered metrics. */
    private Map<String, Metric> metrics = new LinkedHashMap<>();

    private MetricRegistryBuilder(String name, IgniteLogger log) {
        this.name = name;
        this.log = log;
    }

    public static MetricRegistryBuilder newInstance(String name, IgniteLogger log) {
        return new MetricRegistryBuilder(name, log);
    }

    public MetricRegistry build() {
        if (metrics == null)
            throw new IllegalStateException("Builder can't be used twiced.");

        MetricRegistry reg = new MetricRegistry(name, metrics);

        metrics = null;

        return reg;
    }

    /**
     * Returns registry name.
     *
     * @return Registry name.
     */
    public String name() {
        return name;
    }

    /**
     * Adds existing metric with the specified name.
     *
     * @param metric Metric.
     * @throws IllegalStateException If metric with given name is already added.
     * @deprecated Wrong method. Breaks contract.
     */
    @Deprecated
    public void addMetric(Metric metric) {
        addMetric(metric.name(), metric);
    }

    /**
     * Creates and adds metric with "non-primitive" value.
     *
     * @param name Metric name.
     * @param type Type of metric value.
     * @param desc Description.
     * @return {@link ObjectMetricImpl} instance.
     * @throws IllegalStateException If metric with given name is already added.
     */
    public <T> ObjectMetricImpl<T> objectMetric(String name, Class<T> type, @Nullable String desc) {
        return addMetric(name, new ObjectMetricImpl<>(metricName(this.name, name), desc, type));
    }

    /**
     * Creates and adds {@link BooleanGauge} which value will be obtained from the specified value provider.
     *
     * @param name Metric name.
     * @param supplier Value provider.
     * @param desc Description.
     */
    public BooleanGauge register(String name, BooleanSupplier supplier, @Nullable String desc) {
        return addMetric(name, new BooleanGauge(metricName(this.name, name), desc, nonThrowableSupplier(supplier, log)));
    }

    /**
     * Creates and adds {@link DoubleGauge} which value will be obtained from the specified value provider.
     *
     * @param name Metric name.
     * @param supplier Value provider.
     * @param desc Description.
     */
    public DoubleGauge register(String name, DoubleSupplier supplier, @Nullable String desc) {
        return addMetric(name, new DoubleGauge(metricName(this.name, name), desc, nonThrowableSupplier(supplier, log)));
    }

    /**
     * Creates and adds {@link IntGauge} which value will be obtained from the specified value provider.
     *
     * @param name Metric name.
     * @param supplier Value provider.
     * @param desc Description.
     */
    public IntGauge register(String name, IntSupplier supplier, @Nullable String desc) {
        return addMetric(name, new IntGauge(metricName(this.name, name), desc, nonThrowableSupplier(supplier, log)));
    }

    /**
     * Creates and adds {@link LongGauge} which value will be obtained from the specified value provider.
     *
     * @param name Metric name.
     * @param supplier Value provider.
     * @param desc Description.
     */
    public LongGauge register(String name, LongSupplier supplier, @Nullable String desc) {
        return addMetric(name, new LongGauge(metricName(this.name, name), desc, nonThrowableSupplier(supplier, log)));
    }

    /**
     * Creates and adds {@link ObjectGauge} which value will be obtained from the specified value provider.
     *
     * @param name Metric name.
     * @param supplier Value provider.
     * @param type VAlue type.
     * @param desc Description.
     */
    public <T> ObjectGauge<T> register(String name, Supplier<T> supplier, Class<T> type, @Nullable String desc) {
        return addMetric(name, new ObjectGauge<>(metricName(this.name, name), desc,
                nonThrowableSupplier(supplier, log), type));
    }

    /**
     * Creates and adds double metric.
     *
     * @param name Metric name.
     * @param desc Description.
     * @return {@link DoubleMetricImpl} instance.
     */
    public DoubleMetricImpl doubleMetric(String name, @Nullable String desc) {
        return addMetric(name, new DoubleMetricImpl(metricName(this.name, name), desc));
    }

    /**
     * Creates and adds integer metric.
     *
     * @param name Metric name.
     * @param desc Description.
     * @return {@link IntMetricImpl} instance.
     */
    public IntMetricImpl intMetric(String name, @Nullable String desc) {
        return addMetric(name, new IntMetricImpl(metricName(this.name, name), desc));
    }

    /**
     * Creates and adds long metric.
     *
     * @param name Metric name.
     * @param desc Description.
     * @return {@link AtomicLongMetric} instance.
     */
    public AtomicLongMetric longMetric(String name, @Nullable String desc) {
        return addMetric(name, new AtomicLongMetric(metricName(this.name, name), desc));
    }

    /**
     * Creates and adds long metric.
     *
     * @param name Metric name.
     * @param desc Description.
     * @return {@link LongAdderMetric} instance.
     */
    public LongAdderMetric longAdderMetric(String name, @Nullable String desc) {
        return addMetric(name, new LongAdderMetric(metricName(this.name, name), desc));
    }

    //TODO: why we have this method?
    /**
     * Creates and register named metric.
     * Returned instance are thread safe.
     *
     * @param name Name.
     * @param delegate Delegate to which all updates from new metric will be delegated to.
     * @param desc Description.
     * @return {@link LongAdderWithDelegateMetric}.
     */
    public LongAdderMetric longAdderMetric(String name, LongConsumer delegate, @Nullable String desc) {
        return addMetric(name, new LongAdderWithDelegateMetric(metricName(this.name, name), delegate, desc));
    }

    //TODO: remove {@code size}
    /**
     * Creates and adds hit rate metric.
     *
     * @param rateTimeInterval Rate time interval.
     * @param size Array size for underlying calculations.
     * @return {@link HitRateMetric} instance.
     * @see HitRateMetric
     */
    public HitRateMetric hitRateMetric(String name, @Nullable String desc, long rateTimeInterval, int size) {
        return addMetric(name, new HitRateMetric(metricName(this.name, name), desc, rateTimeInterval, size));
    }

    /**
     * Creates and adds boolean metric.
     *
     * @param name Metric name.
     * @param desc Description.
     * @return {@link BooleanMetricImpl} instance.
     */
    public BooleanMetricImpl booleanMetric(String name, @Nullable String desc) {
        return addMetric(name, new BooleanMetricImpl(metricName(this.name, name), desc));
    }

    /**
     * Creates and adds histogram metric.
     *
     * @param name Metric name
     * @param bounds Bounds of measurements.
     * @param desc Description.
     * @return {@link HistogramMetric} instance.
     */
    public HistogramMetricImpl histogram(String name, long[] bounds, @Nullable String desc) {
        return addMetric(name, new HistogramMetricImpl(metricName(this.name, name), desc, bounds));
    }

    /**
     * Adds new metric if not exists already.
     *
     * @param name Metric name.
     * @param metric Metric
     * @param <T> Type of metric.
     * @return Registered metric.
     * @throws IllegalStateException If metric with given name is already added.
     */
    public <T extends Metric> T addMetric(String name, T metric) {
        T old = (T)metrics.putIfAbsent(name, metric);

        if (old != null) {
            throw new IllegalStateException("Metric with given name is already registered [name=" + name +
                    ", metric=" + metric + ']');
        }

        return metric;
    }
}
