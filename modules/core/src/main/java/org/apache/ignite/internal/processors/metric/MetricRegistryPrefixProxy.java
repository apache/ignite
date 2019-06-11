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
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.DoubleSupplier;
import java.util.function.IntSupplier;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.spi.metric.Metric;
import org.apache.ignite.internal.processors.metric.impl.DoubleMetricImpl;
import org.apache.ignite.internal.processors.metric.impl.HitRateMetric;
import org.apache.ignite.internal.processors.metric.impl.IntMetricImpl;
import org.apache.ignite.internal.processors.metric.impl.LongAdderMetricImpl;
import org.apache.ignite.internal.processors.metric.impl.LongMetricImpl;
import org.apache.ignite.internal.processors.metric.impl.BooleanMetricImpl;
import org.apache.ignite.internal.processors.metric.impl.HistogramMetric;
import org.apache.ignite.internal.processors.metric.impl.ObjectMetricImpl;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.SEPARATOR;
import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.metricName;

/**
 * Proxy registry that adds {@code prefix} to all metric names on each method call.
 */
public class MetricRegistryPrefixProxy implements MetricRegistry {
    /** Prefix for underlying registry. */
    private final String prefix;

    /** Underlying implementation. */
    private final MetricRegistry reg;

    /**
     * @param prefix Metrics prefix.
     * @param reg Underlying imlementaion.
     */
    public MetricRegistryPrefixProxy(String prefix, MetricRegistry reg) {
        assert prefix != null && !prefix.isEmpty();

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
        String p = this.prefix + SEPARATOR;

        return F.view(reg.getMetrics(), m -> m.name().startsWith(p));
    }

    /** {@inheritDoc} */
    @Override public void addMetricCreationListener(Consumer<Metric> lsnr) {
        reg.addMetricCreationListener(m -> {
            if (m.name().startsWith(prefix))
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
    @Override public void remove(String name) {
        reg.remove(fullName(name));
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
    @Override public DoubleMetricImpl doubleMetric(String name, @Nullable String description) {
        return reg.doubleMetric(fullName(name), description);
    }

    /** {@inheritDoc} */
    @Override public IntMetricImpl intMetric(String name, @Nullable String description) {
        return reg.intMetric(fullName(name), description);
    }

    /** {@inheritDoc} */
    @Override public LongMetricImpl metric(String name, @Nullable String description) {
        return reg.metric(fullName(name), description);
    }

    /** {@inheritDoc} */
    @Override public LongAdderMetricImpl longAdderMetric(String name, @Nullable String description) {
        return reg.longAdderMetric(fullName(name), description);
    }

    /** {@inheritDoc} */
    @Override public HitRateMetric hitRateMetric(String name, @Nullable String description,
        long rateTimeInterval, int size) {
        return reg.hitRateMetric(fullName(name), description, rateTimeInterval, size);
    }

    /** {@inheritDoc} */
    @Override public BooleanMetricImpl booleanMetric(String name, @Nullable String description) {
        return reg.booleanMetric(fullName(name), description);
    }

    /** {@inheritDoc} */
    @Override public <T> ObjectMetricImpl<T> objectMetric(String name, Class<T> type, @Nullable String description) {
        return reg.objectMetric(fullName(name), type, description);
    }

    /** {@inheritDoc} */
    @Override public HistogramMetric histogram(String name, long[] bounds, @Nullable String description) {
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
