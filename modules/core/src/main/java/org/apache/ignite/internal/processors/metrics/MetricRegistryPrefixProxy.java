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

package org.apache.ignite.internal.processors.metrics;

import java.util.Collection;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.DoubleSupplier;
import java.util.function.IntSupplier;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.spi.metric.Metric;
import org.apache.ignite.spi.metric.MetricRegistry;
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
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.metrics.MetricNameUtils.metricName;

/**
 *
 */
public class MetricRegistryPrefixProxy implements MetricRegistry {
    /**
     * Prefix for underlying registry.
     */
    private String prefix;

    /**
     * Underlying implementation.
     */
    private MetricRegistry reg;

    /**
     * @param prefix Metrics prefix.
     * @param reg Underlying imlementaion.
     */
    public MetricRegistryPrefixProxy(String prefix, MetricRegistry reg) {
        this.prefix = prefix;
        this.reg = reg;
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
        String p = this.prefix + ".";

        return F.view(reg.getMetrics(), m -> m.getName().startsWith(p));
    }

    /** {@inheritDoc} */
    @Override public void addMetricCreationListener(Consumer<Metric> lsnr) {
        reg.addMetricCreationListener(m -> {
            if (m.getName().startsWith(prefix))
                lsnr.accept(m);
        });
    }

    /** {@inheritDoc} */
    @Override public @Nullable Metric findMetric(String name) {
        return reg.findMetric(fullName(name));
    }

    /** {@inheritDoc} */
    @Override public void register(Metric metric) {
        reg.register(metric);
    }

    /** {@inheritDoc} */
    @Override public void register(String name, BooleanSupplier supplier, @Nullable String description) {
        reg.register(fullName(name), supplier, description);
    }

    /** {@inheritDoc} */
    @Override public void register(String name, DoubleSupplier supplier, @Nullable String description) {
        reg.register(fullName(name), supplier, description);
    }

    /** {@inheritDoc} */
    @Override public void register(String name, IntSupplier supplier, @Nullable String description) {
        reg.register(fullName(name), supplier, description);
    }

    /** {@inheritDoc} */
    @Override public void register(String name, LongSupplier supplier, @Nullable String description) {
        reg.register(fullName(name), supplier, description);
    }

    /** {@inheritDoc} */
    @Override public <T> void register(String name, Supplier<T> supplier, Class<T> type, @Nullable String description) {
        reg.register(fullName(name), supplier, type, description);
    }

    /** {@inheritDoc} */
    @Override public DoubleCounter doubleCounter(String name, @Nullable String description) {
        return reg.doubleCounter(fullName(name), description);
    }

    /** {@inheritDoc} */
    @Override public IntCounter intCounter(String name, @Nullable String description) {
        return reg.intCounter(fullName(name), description);
    }

    /** {@inheritDoc} */
    @Override public LongCounter counter(String name, @Nullable String description) {
        return reg.counter(fullName(name), description);
    }

    @Override public LongCounter counter(String name, LongConsumer updateDelegate, @Nullable String description) {
        return reg.counter(fullName(name), updateDelegate, description);
    }

    /** {@inheritDoc} */
    @Override public HitRateCounter hitRateCounter(String name, @Nullable String description,
        long rateTimeInterval, int size) {
        return reg.hitRateCounter(fullName(name), description, rateTimeInterval, size);
    }

    /** {@inheritDoc} */
    @Override public BooleanGauge booleanGauge(String name, @Nullable String description) {
        return reg.booleanGauge(fullName(name), description);
    }

    /** {@inheritDoc} */
    @Override public DoubleGauge doubleGauge(String name, @Nullable String description) {
        return reg.doubleGauge(fullName(name), description);
    }

    /** {@inheritDoc} */
    @Override public IntGauge intGauge(String name, @Nullable String description) {
        return reg.intGauge(fullName(name), description);
    }

    /** {@inheritDoc} */
    @Override public LongGauge gauge(String name, @Nullable String description) {
        return reg.gauge(fullName(name), description);
    }

    /** {@inheritDoc} */
    @Override public <T> ObjectGauge<T> objectGauge(String name, Class<T> type, @Nullable String description) {
        return reg.objectGauge(fullName(name), type, description);
    }

    /** {@inheritDoc} */
    @Override public HistogramGauge histogram(String name, long[] bounds, @Nullable String description) {
        return reg.histogram(fullName(name), bounds, description);
    }

    /**
     * @param name Metric name.
     * @return Full name with prefix.
     */
    @NotNull private String fullName(String name) {
        return metricName(prefix, name);
    }
}
