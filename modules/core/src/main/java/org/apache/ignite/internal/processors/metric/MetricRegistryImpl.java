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

import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.DoubleSupplier;
import java.util.function.IntSupplier;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.processors.metric.impl.BooleanGauge;
import org.apache.ignite.internal.processors.metric.impl.DoubleGauge;
import org.apache.ignite.internal.processors.metric.impl.IntGauge;
import org.apache.ignite.internal.processors.metric.impl.LongGauge;
import org.apache.ignite.internal.processors.metric.impl.ObjectGauge;
import org.apache.ignite.spi.metric.Metric;
import org.apache.ignite.internal.processors.metric.impl.DoubleMetricImpl;
import org.apache.ignite.internal.processors.metric.impl.HitRateMetric;
import org.apache.ignite.internal.processors.metric.impl.IntMetricImpl;
import org.apache.ignite.internal.processors.metric.impl.LongAdderMetricImpl;
import org.apache.ignite.internal.processors.metric.impl.LongMetricImpl;
import org.apache.ignite.internal.processors.metric.impl.BooleanMetricImpl;
import org.apache.ignite.internal.processors.metric.impl.HistogramMetric;
import org.apache.ignite.internal.processors.metric.impl.ObjectMetricImpl;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.metric.MetricNameUtils.metricName;
import static org.apache.ignite.internal.util.lang.GridFunc.nonThrowableSupplier;

/**
 * Simple implementation.
 */
public class MetricRegistryImpl implements MetricRegistry {
    /** Registered metrics. */
    private final ConcurrentHashMap<String, Metric> metrics = new ConcurrentHashMap<>();

    /** Logger. */
    @Nullable private final IgniteLogger log;

    /** Metric set creation listeners. */
    private final List<Consumer<Metric>> metricCreationLsnrs = new CopyOnWriteArrayList<>();

    /** For test usage only. */
    public MetricRegistryImpl() {
        this.log = null;
    }

    /**
     * @param log Logger.
     */
    public MetricRegistryImpl(IgniteLogger log) {
        this.log = log;
    }

    /** {@inheritDoc} */
    @Override public MetricRegistry withPrefix(String prefix) {
        return new MetricRegistryPrefixProxy(prefix, this);
    }

    /** {@inheritDoc} */
    @Override public MetricRegistry withPrefix(String... prefixes) {
        return withPrefix(metricName(prefixes));
    }

    /** {@inheritDoc} */
    @Override public Collection<Metric> getMetrics() {
        return metrics.values();
    }

    /** {@inheritDoc} */
    @Override public void addMetricCreationListener(Consumer<Metric> lsnr) {
        metricCreationLsnrs.add(lsnr);
    }

    /** {@inheritDoc} */
    @Override public Metric findMetric(String name) {
        return metrics.get(name);
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
    @Override public void register(String name, BooleanSupplier supplier, @Nullable String description) {
        addMetric(name, new BooleanGauge(name, description, nonThrowableSupplier(supplier, log)));
    }

    /** {@inheritDoc} */
    @Override public void register(String name, DoubleSupplier supplier, @Nullable String description) {
        addMetric(name, new DoubleGauge(name, description, nonThrowableSupplier(supplier, log)));
    }

    /** {@inheritDoc} */
    @Override public void register(String name, IntSupplier supplier, @Nullable String description) {
        addMetric(name, new IntGauge(name, description, nonThrowableSupplier(supplier, log)));
    }

    /** {@inheritDoc} */
    @Override public void register(String name, LongSupplier supplier, @Nullable String description) {
        addMetric(name, new LongGauge(name, description, nonThrowableSupplier(supplier, log)));
    }

    /** {@inheritDoc} */
    @Override public <T> void register(String name, Supplier<T> supplier, Class<T> type, @Nullable String description) {
        addMetric(name, new ObjectGauge<>(name, description, nonThrowableSupplier(supplier, log), type));
    }

    /** {@inheritDoc} */
    @Override public DoubleMetricImpl doubleMetric(String name, @Nullable String description) {
        return addMetric(name, new DoubleMetricImpl(name, description));
    }

    /** {@inheritDoc} */
    @Override public IntMetricImpl intMetric(String name, @Nullable String description) {
        return addMetric(name, new IntMetricImpl(name, description));
    }

    /** {@inheritDoc} */
    @Override public LongMetricImpl metric(String name, @Nullable String description) {
        return addMetric(name, new LongMetricImpl(name, description));
    }

    /** {@inheritDoc} */
    @Override public LongAdderMetricImpl longAdderMetric(String name, @Nullable String description) {
        return addMetric(name, new LongAdderMetricImpl(name, description));
    }

    /** {@inheritDoc} */
    @Override public HitRateMetric hitRateMetric(String name,
        @Nullable String description, long rateTimeInterval, int size) {
        return addMetric(name, new HitRateMetric(name, description, rateTimeInterval, size));
    }

    /** {@inheritDoc} */
    @Override public BooleanMetricImpl booleanMetric(String name, @Nullable String description) {
        return addMetric(name, new BooleanMetricImpl(name, description));
    }

    /** {@inheritDoc} */
    @Override public <T> ObjectMetricImpl<T> objectMetric(String name, Class<T> type, @Nullable String description) {
        return addMetric(name, new ObjectMetricImpl<>(name, description, type));
    }

    /** {@inheritDoc} */
    @Override public HistogramMetric histogram(String name, long[] bounds, @Nullable String description) {
        return addMetric(name, new HistogramMetric(name, description, bounds));
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

        if (old == null) {
            notifyListeners(metric, metricCreationLsnrs);

            return metric;
        }

        return old;
    }

    /**
     * @param t Consumed object.
     * @param lsnrs Listeners.
     * @param <T> Type of consumed object.
     */
    private <T> void notifyListeners(T t, List<Consumer<T>> lsnrs) {
        for (Consumer<T> lsnr : lsnrs) {
            try {
                lsnr.accept(t);
            }
            catch (Exception e) {
                log.warning("Metric listener error", e);
            }
        }
    }
}
