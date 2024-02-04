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

import org.apache.ignite.cdc.CdcConsumer;
import org.apache.ignite.internal.processors.metric.impl.AtomicLongMetric;
import org.apache.ignite.internal.processors.metric.impl.BooleanMetricImpl;
import org.apache.ignite.internal.processors.metric.impl.DoubleMetricImpl;
import org.apache.ignite.internal.processors.metric.impl.HistogramMetricImpl;
import org.apache.ignite.internal.processors.metric.impl.HitRateMetric;
import org.apache.ignite.internal.processors.metric.impl.IntMetricImpl;
import org.apache.ignite.internal.processors.metric.impl.LongAdderMetric;
import org.apache.ignite.internal.processors.metric.impl.LongAdderWithDelegateMetric;
import org.apache.ignite.internal.processors.metric.impl.ObjectMetricImpl;
import org.apache.ignite.metric.IgniteMetric;
import org.apache.ignite.spi.metric.Metric;
import org.jetbrains.annotations.Nullable;

/** Internal facade for metric registry. Supports API of {@link CdcConsumer}. Should be migrated to {@link IgniteMetric}. */
public interface MetricRegistry extends IgniteMetric {
    /**
     * Creates and register named gauge.
     * Returned instance are thread safe.
     *
     * @param name Name.
     * @param type Type.
     * @param desc Description.
     * @return {@link ObjectMetricImpl}
     */
    <T> ObjectMetricImpl<T> objectMetric(String name, Class<T> type, @Nullable String desc);

    /**
     * Register existing metrics in this group with the specified name. Note that the name of the metric must
     * start with the name of the current registry it is registered into.
     *
     * @param metric Metric.
     */
    void register(Metric metric);

    /**
     * Creates and register named metric.
     * Returned instance are thread safe.
     *
     * @param name Name.
     * @param desc Description.
     * @return {@link DoubleMetricImpl}.
     */
    DoubleMetricImpl doubleMetric(String name, @Nullable String desc);

    /**
     * Creates and register named metric.
     * Returned instance are thread safe.
     *
     * @param name Name.
     * @param desc Description.
     * @return {@link IntMetricImpl}.
     */
    IntMetricImpl intMetric(String name, @Nullable String desc);

    /**
     * Creates and register named metric.
     * Returned instance are thread safe.
     *
     * @param name Name.
     * @param desc Description.
     * @return {@link AtomicLongMetric}.
     */
    AtomicLongMetric longMetric(String name, @Nullable String desc);

    /**
     * Creates and register named metric.
     * Returned instance are thread safe.
     *
     * @param name Name.
     * @param desc Description.
     * @return {@link LongAdderMetric}.
     */
    LongAdderMetric longAdderMetric(String name, @Nullable String desc);

    /**
     * Creates and register named metric.
     * Returned instance are thread safe.
     *
     * @param name     Name.
     * @param delegate Delegate to which all updates from new metric will be delegated to.
     * @param desc     Description.
     * @return {@link LongAdderWithDelegateMetric}.
     */
    LongAdderWithDelegateMetric longAdderMetric(String name, LongAdderWithDelegateMetric.Delegate delegate, @Nullable String desc);

    /**
     * Creates and register hit rate metric.
     * <p>
     * It will accumulates approximate hit rate statistics.
     * Calculates number of hits in last rateTimeInterval milliseconds.
     *
     * @param rateTimeInterval Rate time interval.
     * @param size             Array size for underlying calculations.
     * @return {@link HitRateMetric}
     * @see HitRateMetric
     */
    HitRateMetric hitRateMetric(String name, @Nullable String desc, long rateTimeInterval, int size);

    /**
     * Creates and register named gauge.
     * Returned instance are thread safe.
     *
     * @param name Name.
     * @param desc Description.
     * @return {@link BooleanMetricImpl}
     */
    BooleanMetricImpl booleanMetric(String name, @Nullable String desc);

    /**
     * Creates and registre named histogram gauge.
     *
     * @param name   Name
     * @param bounds Bounds of measurements.
     * @param desc   Description.
     * @return {@link HistogramMetricImpl}
     */
    HistogramMetricImpl histogram(String name, long[] bounds, @Nullable String desc);
}
