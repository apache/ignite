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

import static java.util.Collections.emptyIterator;

/**
 * No-op implementation of {@link MetricRegistry}.
 *
 * @see MetricRegistryImpl
 */
public class NoOpMetricRegistry implements MetricRegistry {
    /** Registry name. */
    private String regName;

    /** Registered metrics. */
    private final ConcurrentHashMap<String, Metric> metrics = new ConcurrentHashMap<>();

    /**
     * @param regname Registry name.
     */
    public NoOpMetricRegistry(String regName) {
        this.regName = regName;
    }

    /** {@inheritDoc} */
    @Override @Nullable public Metric findMetric(String name) {
        return metrics.get(name);
    }

    /** {@inheritDoc} */
    @Override public void reset() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public <T> ObjectMetricImpl<T> objectMetric(String name, Class<T> type, @Nullable String desc) {
        return addMetric(name, ObjectMetricImpl.NO_OP);
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
        addMetric(name, BooleanGauge.NO_OP);
    }

    /** {@inheritDoc} */
    @Override public void register(String name, DoubleSupplier supplier, @Nullable String desc) {
        addMetric(name, DoubleGauge.NO_OP);
    }

    /** {@inheritDoc} */
    @Override public void register(String name, IntSupplier supplier, @Nullable String desc) {
        addMetric(name, IntGauge.NO_OP);
    }

    /** {@inheritDoc} */
    @Override public void register(String name, LongSupplier supplier, @Nullable String desc) {
        addMetric(name, LongGauge.NO_OP);
    }

    /** {@inheritDoc} */
    @Override public <T> void register(String name, Supplier<T> supplier, Class<T> type, @Nullable String desc) {
        addMetric(name, ObjectGauge.NO_OP);
    }

    /** {@inheritDoc} */
    @Override public DoubleMetricImpl doubleMetric(String name, @Nullable String desc) {
        return DoubleMetricImpl.NO_OP;
    }

    /** {@inheritDoc} */
    @Override public IntMetricImpl intMetric(String name, @Nullable String desc) {
        return IntMetricImpl.NO_OP;
    }

    /** {@inheritDoc} */
    @Override public LongMetricImpl metric(String name, @Nullable String desc) {
        return LongMetricImpl.NO_OP;
    }

    /** {@inheritDoc} */
    @Override public LongAdderMetricImpl longAdderMetric(String name, @Nullable String desc) {
        return LongAdderMetricImpl.NO_OP;
    }

    /** {@inheritDoc} */
    @Override public HitRateMetric hitRateMetric(String name, @Nullable String desc, long rateTimeInterval, int size) {
        return HitRateMetric.NO_OP;
    }

    /** {@inheritDoc} */
    @Override public BooleanMetricImpl booleanMetric(String name, @Nullable String desc) {
        return BooleanMetricImpl.NO_OP;
    }

    /** {@inheritDoc} */
    @Override public HistogramMetric histogram(String name, long[] bounds, @Nullable String desc) {
        return HistogramMetric.NO_OP;
    }

    /** {@inheritDoc} */
    @NotNull @Override public Iterator<Metric> iterator() {
        return emptyIterator();
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
