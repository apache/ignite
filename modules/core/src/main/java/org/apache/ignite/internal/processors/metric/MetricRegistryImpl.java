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

import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BooleanSupplier;
import java.util.function.DoubleSupplier;
import java.util.function.Function;
import java.util.function.IntSupplier;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import org.apache.ignite.IgniteException;
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
import org.apache.ignite.metric.MetricRegistry;
import org.apache.ignite.spi.metric.Metric;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.metric.impl.HitRateMetric.DFLT_SIZE;
import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.customMetric;
import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.fromFullName;
import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.metricName;
import static org.apache.ignite.internal.util.lang.GridFunc.nonThrowableSupplier;

/**
 * Metric registry.
 */
public class MetricRegistryImpl implements MetricRegistry {
    /** Registry name. */
    private String regName;

    /** Logger. */
    private IgniteLogger log;

    /** Registered metrics. */
    private final ConcurrentHashMap<String, Metric> metrics = new ConcurrentHashMap<>();

    /** HitRate config provider. */
    private final Function<String, Long> hitRateCfgProvider;

    /** Histogram config provider. */
    private final Function<String, long[]> histogramCfgProvider;

    /**
     * @param regName Registry name.
     * @param hitRateCfgProvider HitRate config provider.
     * @param histogramCfgProvider Histogram config provider.
     * @param log Logger.
     */
    public MetricRegistryImpl(String regName, Function<String, Long> hitRateCfgProvider,
        Function<String, long[]> histogramCfgProvider, IgniteLogger log) {
        this.regName = regName;
        this.log = log;
        this.hitRateCfgProvider = hitRateCfgProvider;
        this.histogramCfgProvider = histogramCfgProvider;
    }

    /** {@inheritDoc} */
    @Nullable @Override public <M extends Metric> M findMetric(String name) {
        return (M)metrics.get(name);
    }

    /** Resets state of this metric registry. */
    public void reset() {
        for (Metric m : metrics.values())
            m.reset();
    }

    /**
     * Creates and register named gauge.
     * Returned instance are thread safe.
     *
     * @param name Name.
     * @param type Type.
     * @param desc Description.
     * @return {@link ObjectMetricImpl}
     */
    public <T> ObjectMetricImpl<T> objectMetric(String name, Class<T> type, @Nullable String desc) {
        return addMetric(name, new ObjectMetricImpl<>(metricName(regName, name), desc, type));
    }

    /** {@inheritDoc} */
    @NotNull @Override public Iterator<Metric> iterator() {
        return metrics.values().iterator();
    }

    /**
     * Register existing metrics in this group with the specified name. Note that the name of the metric must
     * start with the name of the current registry it is registered into.
     *
     * @param metric Metric.
     */
    public void register(Metric metric) {
        assert fromFullName(metric.name()).get1().equals(regName);

        addMetric(fromFullName(metric.name()).get2(), metric);
    }

    /** {@inheritDoc} */
    @Override public void remove(String name) {
        metrics.remove(name);
    }

    /** {@inheritDoc} */
    @Override public void register(String name, BooleanSupplier supplier, @Nullable String desc) {
        addMetric(name, new BooleanGauge(metricName(regName, name), desc, nonThrowableSupplier(supplier, log)));
    }

    /** {@inheritDoc} */
    @Override public void register(String name, DoubleSupplier supplier, @Nullable String desc) {
        addMetric(name, new DoubleGauge(metricName(regName, name), desc, nonThrowableSupplier(supplier, log)));
    }

    /** {@inheritDoc} */
    @Override public void register(String name, IntSupplier supplier, @Nullable String desc) {
        addMetric(name, new IntGauge(metricName(regName, name), desc, nonThrowableSupplier(supplier, log)));
    }

    /** {@inheritDoc} */
    @Override public void register(String name, LongSupplier supplier, @Nullable String desc) {
        addMetric(name, new LongGauge(metricName(regName, name), desc, nonThrowableSupplier(supplier, log)));
    }

    /** {@inheritDoc} */
    @Override public <T> void register(String name, Supplier<T> supplier, Class<T> type, @Nullable String desc) {
        addMetric(name, new ObjectGauge<>(metricName(regName, name), desc, nonThrowableSupplier(supplier, log), type));
    }

    /**
     * Creates and register named metric.
     * Returned instance are thread safe.
     *
     * @param name Name.
     * @param desc Description.
     * @return {@link DoubleMetricImpl}.
     */
    public DoubleMetricImpl doubleMetric(String name, @Nullable String desc) {
        return addMetric(name, new DoubleMetricImpl(metricName(regName, name), desc));
    }

    /**
     * Creates and register named metric.
     * Returned instance are thread safe.
     *
     * @param name Name.
     * @param desc Description.
     * @return {@link IntMetricImpl}.
     */
    public IntMetricImpl intMetric(String name, @Nullable String desc) {
        return addMetric(name, new IntMetricImpl(metricName(regName, name), desc));
    }

    /** {@inheritDoc} */
    @Override public AtomicLongMetric longMetric(String name, @Nullable String desc) {
        return addMetric(name, new AtomicLongMetric(metricName(regName, name), desc));
    }

    /**
     * Creates and register named metric.
     * Returned instance are thread safe.
     *
     * @param name Name.
     * @param desc Description.
     * @return {@link LongAdderMetric}.
     */
    public LongAdderMetric longAdderMetric(String name, @Nullable String desc) {
        return addMetric(name, new LongAdderMetric(metricName(regName, name), desc));
    }

    /**
     * Creates and register named metric.
     * Returned instance are thread safe.
     *
     * @param name Name.
     * @param delegate Delegate to which all updates from new metric will be delegated to.
     * @param desc Description.
     * @return {@link LongAdderWithDelegateMetric}.
     */
    public LongAdderWithDelegateMetric longAdderMetric(
        String name, LongAdderWithDelegateMetric.Delegate delegate, @Nullable String desc
    ) {
        return addMetric(name, new LongAdderWithDelegateMetric(metricName(regName, name), delegate, desc));
    }

    /**
     * Creates and register hit rate metric.
     *
     * It will accumulates approximate hit rate statistics.
     * Calculates number of hits in last rateTimeInterval milliseconds.
     *
     * @param rateTimeInterval Rate time interval.
     * @param size Array size for underlying calculations.
     * @return {@link HitRateMetric}
     * @see HitRateMetric
     */
    public HitRateMetric hitRateMetric(String name, @Nullable String desc, long rateTimeInterval, int size) {
        return addMetric(name, new HitRateMetric(metricName(regName, name), desc, rateTimeInterval, size));
    }

    /**
     * Creates and register named gauge.
     * Returned instance are thread safe.
     *
     * @param name Name.
     * @param desc Description.
     * @return {@link BooleanMetricImpl}
     */
    public BooleanMetricImpl booleanMetric(String name, @Nullable String desc) {
        return addMetric(name, new BooleanMetricImpl(metricName(regName, name), desc));
    }

    /**
     * Creates and registre named histogram gauge.
     *
     * @param name Name
     * @param bounds Bounds of measurements.
     * @param desc Description.
     * @return {@link HistogramMetricImpl}
     */
    public HistogramMetricImpl histogram(String name, long[] bounds, @Nullable String desc) {
        return addMetric(name, new HistogramMetricImpl(metricName(regName, name), desc, bounds));
    }

    /**
     * Adds metrics if not exists already.
     *
     * @param name Name.
     * @param metric Metric
     * @param <T> Type of metric.
     * @return {@code metric} if there were no other metric with the same name. Previous metric if can be cast to
     * {@code metric}.
     * @throws IgniteException if a metric of incompatible type is already registered.
     */
    private <T extends Metric> T addMetric(String name, T metric) throws IgniteException {
        if (metric == null)
            throw new IllegalArgumentException("Null metric passed with name '" + name + "'.");

        Metric old = metrics.putIfAbsent(metricName(name), metric);

        if (old != null) {
            if (!metric.getClass().isAssignableFrom(old.getClass()))
                throw new IgniteException("Other metric with name '" + name + "' is already registered.");

            return (T)old;
        }

        if (!customMetric(name()))
            configureMetrics(metric);

        return metric;
    }

    /**
     * Assigns metric settings if {@code metric} is configurable.
     */
    private void configureMetrics(Metric metric) {
        assert !customMetric(name()) : "Custom metrics cannot be configured yet";

        if (metric instanceof HistogramMetricImpl) {
            long[] cfgBounds = histogramCfgProvider.apply(metric.name());

            if (cfgBounds != null)
                ((HistogramMetricImpl)metric).reset(cfgBounds);
        }
        else if (metric instanceof HitRateMetric) {
            Long cfgRateTimeInterval = hitRateCfgProvider.apply(metric.name());

            if (cfgRateTimeInterval != null)
                ((HitRateMetric)metric).reset(cfgRateTimeInterval, DFLT_SIZE);
        }
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return regName;
    }
}
