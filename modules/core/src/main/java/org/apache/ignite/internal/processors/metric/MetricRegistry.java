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
import org.apache.ignite.spi.metric.BooleanMetric;
import org.apache.ignite.spi.metric.IntMetric;
import org.apache.ignite.spi.metric.Metric;
import org.apache.ignite.spi.metric.ReadOnlyMetricRegistry;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.metric.impl.HitRateMetric.DFLT_SIZE;
import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.metricName;
import static org.apache.ignite.internal.util.lang.GridFunc.nonThrowableSupplier;

/**
 * Metric registry.
 */
public class MetricRegistry implements ReadOnlyMetricRegistry {
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
    public MetricRegistry(String regName, Function<String, Long> hitRateCfgProvider,
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
     * Register existing metrics in this group with the specified name.
     *
     * @param metric Metric.
     */
    public void register(Metric metric) {
        addMetric(metric.name(), metric);
    }

    /**
     * Removes metrics with the {@code name}.
     *
     * @param name Metric name.
     */
    public void remove(String name) {
        metrics.remove(name);
    }

    /**
     * Registers {@link BooleanMetric} which value will be queried from the specified supplier.
     *
     * @param name Name.
     * @param supplier Supplier.
     * @param desc Description.
     */
    public void register(String name, BooleanSupplier supplier, @Nullable String desc) {
        addMetric(name, new BooleanGauge(metricName(regName, name), desc, nonThrowableSupplier(supplier, log)));
    }

    /**
     * Registers {@link DoubleSupplier} which value will be queried from the specified supplier.
     *
     * @param name Name.
     * @param supplier Supplier.
     * @param desc Description.
     */
    public void register(String name, DoubleSupplier supplier, @Nullable String desc) {
        addMetric(name, new DoubleGauge(metricName(regName, name), desc, nonThrowableSupplier(supplier, log)));
    }

    /**
     * Registers {@link IntMetric} which value will be queried from the specified supplier.
     *
     * @param name Name.
     * @param supplier Supplier.
     * @param desc Description.
     */
    public void register(String name, IntSupplier supplier, @Nullable String desc) {
        addMetric(name, new IntGauge(metricName(regName, name), desc, nonThrowableSupplier(supplier, log)));
    }

    /**
     * Registers {@link LongGauge} which value will be queried from the specified supplier.
     *
     * @param name Name.
     * @param supplier Supplier.
     * @param desc Description.
     * @return Metric of type {@link LongGauge}.
     */
    public LongGauge register(String name, LongSupplier supplier, @Nullable String desc) {
        return addMetric(name, new LongGauge(metricName(regName, name), desc, nonThrowableSupplier(supplier, log)));
    }

    /**
     * Registers {@link ObjectGauge} which value will be queried from the specified {@link Supplier}.
     *
     * @param name Name.
     * @param supplier Supplier.
     * @param type Type.
     * @param desc Description.
     */
    public <T> void register(String name, Supplier<T> supplier, Class<T> type, @Nullable String desc) {
        addMetric(name, new ObjectGauge<>(metricName(regName, name), desc,
            nonThrowableSupplier(supplier, log), type));
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

    /**
     * Creates and register named metric.
     * Returned instance are thread safe.
     *
     * @param name Name.
     * @param desc Description.
     * @return {@link AtomicLongMetric}.
     */
    public AtomicLongMetric longMetric(String name, @Nullable String desc) {
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
    public LongAdderMetric longAdderMetric(String name, LongConsumer delegate, @Nullable String desc) {
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
        String fullName = metricName(regName, name);

        HitRateMetric metric = addMetric(name, new HitRateMetric(fullName, desc, rateTimeInterval, size));

        Long cfgRateTimeInterval = hitRateCfgProvider.apply(fullName);

        if (cfgRateTimeInterval != null)
            metric.reset(cfgRateTimeInterval, DFLT_SIZE);

        return metric;
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
        String fullName = metricName(regName, name);

        HistogramMetricImpl metric = addMetric(name, new HistogramMetricImpl(fullName, desc, bounds));

        long[] cfgBounds = histogramCfgProvider.apply(fullName);

        if (cfgBounds != null)
            metric.reset(cfgBounds);

        return metric;
    }

    /**
     * Adds metrics if not exists already.
     *
     * @param name Name.
     * @param metric Metric
     * @param <T> Type of metric.
     * @return Registered metric.
     */
    private <T extends Metric> T addMetric(String name, T metric) {
        T old = (T)metrics.putIfAbsent(name, metric);

        if (old != null)
            return old;

        return metric;
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return regName;
    }
}
