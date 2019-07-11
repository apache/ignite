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
import java.util.function.IntSupplier;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.processors.metric.impl.BooleanGauge;
import org.apache.ignite.internal.processors.metric.impl.BooleanMetricImpl;
import org.apache.ignite.internal.processors.metric.impl.DoubleGauge;
import org.apache.ignite.internal.processors.metric.impl.DoubleMetricImpl;
import org.apache.ignite.internal.processors.metric.impl.HistogramMetric;
import org.apache.ignite.internal.processors.metric.impl.HitRateMetric;
import org.apache.ignite.internal.processors.metric.impl.IntGauge;
import org.apache.ignite.internal.processors.metric.impl.IntMetricImpl;
import org.apache.ignite.internal.processors.metric.impl.LongAdderMetricImpl;
import org.apache.ignite.internal.processors.metric.impl.LongGauge;
import org.apache.ignite.internal.processors.metric.impl.LongMetricImpl;
import org.apache.ignite.internal.processors.metric.impl.ObjectGauge;
import org.apache.ignite.internal.processors.metric.impl.ObjectMetricImpl;
import org.apache.ignite.spi.metric.Metric;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.metricName;
import static org.apache.ignite.internal.util.lang.GridFunc.nonThrowableSupplier;

/**
 * Metric registry implementation.
 *
 * @see NoOpMetricRegistry
 */
public class MetricRegistryImpl implements MetricRegistry {
    /** Registry name. */
    private String regName;

    /** Logger. */
    private IgniteLogger log;

    /** Registered metrics. */
    private final ConcurrentHashMap<String, Metric> metrics = new ConcurrentHashMap<>();

    /**
     * @param regName Registry name.
     * @param log Logger.
     */
    public MetricRegistryImpl(String regName, IgniteLogger log) {
        this.regName = regName;
        this.log = log;
    }

    /** {@inheritDoc} */
    @Override @Nullable public Metric findMetric(String name) {
        return metrics.get(name);
    }

    /** {@inheritDoc} */
    @Override public void reset() {
        metrics.values().forEach(Metric::reset);
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
        addMetric(metric.name(), metric);
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
        addMetric(name, new ObjectGauge<>(metricName(regName, name), desc,
            nonThrowableSupplier(supplier, log), type));
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
    @Override public LongMetricImpl metric(String name, @Nullable String desc) {
        return addMetric(name, new LongMetricImpl(metricName(regName, name), desc));
    }

    /** {@inheritDoc} */
    @Override public LongAdderMetricImpl longAdderMetric(String name, @Nullable String desc) {
        return addMetric(name, new LongAdderMetricImpl(metricName(regName, name), desc));
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
    @Override public HistogramMetric histogram(String name, long[] bounds, @Nullable String desc) {
        return addMetric(name, new HistogramMetric(metricName(regName, name), desc, bounds));
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

        if(old != null)
            return old;

        return metric;
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return regName;
    }
}
