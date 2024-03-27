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
import org.apache.ignite.spi.metric.BooleanMetric;
import org.apache.ignite.spi.metric.DoubleMetric;
import org.apache.ignite.spi.metric.IntMetric;
import org.apache.ignite.spi.metric.Metric;
import org.apache.ignite.spi.metric.ObjectMetric;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.metric.impl.HitRateMetric.DFLT_SIZE;
import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.fromFullName;
import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.metricName;
import static org.apache.ignite.internal.util.lang.GridFunc.nonThrowableSupplier;

/**
 * Metric registry.
 */
public class MetricRegistryImpl implements MetricRegistry {
    /** Registry name. */
    private String regName;

    /** Custom metrics flag. */
    private boolean custom;

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
     * @param custom Custom metrics flag.
     * @param hitRateCfgProvider HitRate config provider.
     * @param histogramCfgProvider Histogram config provider.
     * @param log Logger.
     */
    public MetricRegistryImpl(
        String regName,
        boolean custom,
        Function<String, Long> hitRateCfgProvider,
        Function<String, long[]> histogramCfgProvider,
        IgniteLogger log
    ) {
        this.regName = regName;
        this.custom = custom;
        this.log = log;
        this.hitRateCfgProvider = hitRateCfgProvider;
        this.histogramCfgProvider = histogramCfgProvider;
    }

    /** {@inheritDoc} */
    @Nullable @Override public <M extends Metric> M findMetric(String name) {
        return (M)metrics.get(name);
    }

    /** {@inheritDoc} */
    @Override public void reset() {
        for (Metric m : metrics.values())
            m.reset();
    }

    /** {@inheritDoc} */
    @Override public <T> ObjectMetricImpl<T> objectMetric(String name, Class<T> type, @Nullable String desc) {
        return addMetric(name, new ObjectMetricImpl<>(metricName(regName, name), desc, type));
    }

    /** {@inheritDoc} */
    @NotNull @Override public Iterator<Metric> iterator() {
        return metrics.values().iterator();
    }

    /** {@inheritDoc} */
    @Override public void register(Metric metric) {
        assert fromFullName(metric.name()).get1().equals(regName);

        addMetric(fromFullName(metric.name()).get2(), metric);
    }

    /** {@inheritDoc} */
    @Override public void remove(String name) {
        metrics.remove(name);
    }

    /**
     * Registers {@link BooleanMetric} which value will be queried from the specified supplier.
     *
     * @param name Name.
     * @param supplier Supplier.
     * @param desc Description.
     */
    @Override public BooleanMetric register(String name, BooleanSupplier supplier, @Nullable String desc) {
        return addMetric(name, new BooleanGauge(metricName(regName, name), desc, nonThrowableSupplier(supplier, log)));
    }

    /** {@inheritDoc} */
    @Override public DoubleMetric register(String name, DoubleSupplier supplier, @Nullable String desc) {
        return addMetric(name, new DoubleGauge(metricName(regName, name), desc, nonThrowableSupplier(supplier, log)));
    }

    /** {@inheritDoc} */
    @Override public IntMetric register(String name, IntSupplier supplier, @Nullable String desc) {
        return addMetric(name, new IntGauge(metricName(regName, name), desc, nonThrowableSupplier(supplier, log)));
    }

    /** {@inheritDoc} */
    @Override public LongGauge register(String name, LongSupplier supplier, @Nullable String desc) {
        return addMetric(name, new LongGauge(metricName(regName, name), desc, nonThrowableSupplier(supplier, log)));
    }

    /** {@inheritDoc} */
    @Override public <T> ObjectMetric<T> register(String name, Supplier<T> supplier, Class<T> type, @Nullable String desc) {
        return addMetric(name, new ObjectGauge<>(metricName(regName, name), desc, nonThrowableSupplier(supplier, log), type));
    }

    /** {@inheritDoc} */
    @Override public DoubleMetricImpl doubleMetric(String name, @Nullable String desc) {
        return addMetric(name, new DoubleMetricImpl(metricName(regName, name), desc));
    }

    /** {@inheritDoc} */
    @Override public IntMetricImpl intMetric(String name, @Nullable String desc) {
        return addMetric(name, new IntMetricImpl(metricName(regName, name), desc));
    }

    /** {@inheritDoc} */
    @Override public AtomicLongMetric longMetric(String name, @Nullable String desc) {
        return addMetric(name, new AtomicLongMetric(metricName(regName, name), desc));
    }

    /** {@inheritDoc} */
    @Override public LongAdderMetric longAdderMetric(String name, @Nullable String desc) {
        return addMetric(name, new LongAdderMetric(metricName(regName, name), desc));
    }

    /** {@inheritDoc} */
    @Override public LongAdderWithDelegateMetric longAdderMetric(
        String name, LongAdderWithDelegateMetric.Delegate delegate, @Nullable String desc
    ) {
        return addMetric(name, new LongAdderWithDelegateMetric(metricName(regName, name), delegate, desc));
    }

    /** {@inheritDoc} */
    @Override public HitRateMetric hitRateMetric(String name, @Nullable String desc, long rateTimeInterval, int size) {
        return addMetric(name, new HitRateMetric(metricName(regName, name), desc, rateTimeInterval, size));
    }

    /** {@inheritDoc} */
    @Override public BooleanMetricImpl booleanMetric(String name, @Nullable String desc) {
        return addMetric(name, new BooleanMetricImpl(metricName(regName, name), desc));
    }

    /** {@inheritDoc} */
    @Override public HistogramMetricImpl histogram(String name, long[] bounds, @Nullable String desc) {
        return addMetric(name, new HistogramMetricImpl(metricName(regName, name), desc, bounds));
    }

    /**
     * Adds metrics if not exists already.
     *
     * @param name Name.
     * @param metric Metric
     * @param <T> Type of metric.
     * @return Registered metric.
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

        configureMetrics(metric);

        return metric;
    }

    /**
     * Assigns metric settings if {@code metric} is configurable.
     */
    private void configureMetrics(Metric metric) {
        if (custom)
            return;

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
