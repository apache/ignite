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

package org.apache.ignite.metric;

import java.util.function.BooleanSupplier;
import java.util.function.DoubleSupplier;
import java.util.function.IntSupplier;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import org.apache.ignite.lang.IgniteExperimental;
import org.apache.ignite.spi.metric.ReadOnlyMetricRegistry;
import org.jetbrains.annotations.Nullable;

/**
 * Metric registry. Allows to get, add or remove metrics.
 *
 * @see IgniteMetrics
 * @see ReadOnlyMetricRegistry
 */
@IgniteExperimental
public interface IgniteMetricRegistry extends ReadOnlyMetricRegistry {
     /**
     * Registers an int metric which value will be queried from the specified supplier.
     *
     * @param name Metric name.
     * @param supplier Metric value supplier.
     * @param desc Metric description.
     * @return {@code True} if new metric was added. {@code False} is other metric already exists with the same name.
     */
    boolean gauge(String name, IntSupplier supplier, @Nullable String desc);

    /**
     * Registers a long which value will be queried from the specified supplier.
     *
     * @param name Metric name.
     * @param supplier Metric value supplier.
     * @param desc Metric description.
     * @return {@code True} if new metric was added. {@code False} is other metric already exists with the same name.
     */
    boolean gauge(String name, LongSupplier supplier, @Nullable String desc);

    /**
     * Registers a double metric which value will be queried from the specified supplier.
     *
     * @param name Metric name.
     * @param supplier Metric value supplier.
     * @param desc Metric description.
     * @return {@code True} if new metric was added. {@code False} is other metric already exists with the same name.
     */
    boolean gauge(String name, DoubleSupplier supplier, @Nullable String desc);

    /**
     * Registers an object metric which value will be queried from the specified supplier.
     *
     * @param name Metric name.
     * @param supplier Metric value supplier.
     * @param type Metric value type.
     * @param desc Metric description.
     * @param <T> Metric value type.
     * @return {@code True} if new metric was added. {@code False} is other metric already exists with the same name.
     */
    <T> boolean gauge(String name, Supplier<T> supplier, Class<T> type, @Nullable String desc);

    /**
     * Registers a boolean metric which value will be queried from the specified supplier.
     *
     * @param name Metric name.
     * @param supplier Metric value supplier.
     * @param desc Metric description.
     * @return {@code True} if new metric was added. {@code False} is other metric already exists with the same name.
     */
    boolean gauge(String name, BooleanSupplier supplier, @Nullable String desc);

    /**
     * Registers an updatable int metric.
     *
     * @param name Metric name.
     * @param desc Metric description.
     * @return New {@link IntValueMetric} or previous one with the same name. {@code Null} if previous metric exists and
     * is not a {@link IntValueMetric}.
     */
    @Nullable IntValueMetric intMetric(String name, @Nullable String desc);

    /**
     * Registers an updatable long metric.
     *
     * @param name Metric name.
     * @param desc Metric description.
     * @return New {@link LongValueMetric} or previous one with the same name. {@code Null} if previous metric exists and
     * is not a {@link LongValueMetric}.
     */
    @Nullable LongValueMetric longMetric(String name, @Nullable String desc);

    /**
     * Registers an updatable long adder metric.
     *
     * @param name Metric name.
     * @param desc Metric description.
     * @return New {@link LongValueMetric} or previous one with the same name. {@code Null} if previous metric exists and
     * is not a {@link LongValueMetric}.
     */
    @Nullable LongSumMetric longAdderMetric(String name, @Nullable String desc);

    /**
     * Registers an updatable double metric.
     *
     * @param name Metric name.
     * @param desc Metric description.
     * @return New {@link DoubleValueMetric} or previous one with the same name. {@code Null} if previous metric exists and
     * is not a {@link DoubleValueMetric}.
     */
    @Nullable DoubleValueMetric doubleMetric(String name, @Nullable String desc);

    /**
     * Registers an updatable double metric.
     *
     * @param name Metric name.
     * @param type Metric value type.
     * @param desc Metric description.
     * @param <T> Metric value type.
     * @return New {@link AnyValueMetric} or previous one with the same name. {@code Null} if previous metric exists and
     * is not a {@link AnyValueMetric}.
     */
    @Nullable <T> AnyValueMetric<T> objectMetric(String name, Class<T> type, @Nullable String desc);

    /**
     * Removes metrics with the {@code name}.
     *
     * @param name Metric name..
     */
    void remove(String name);

    /** Resets all metrics of this metric registry. */
    void reset();
}
