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
public interface MetricRegistry extends ReadOnlyMetricRegistry {
    /**
     * Registers an int metric which value will be queried from the specified supplier.
     *
     * @param name Metric short name. Doesn't include registry name. 
     * @param supplier Metric value supplier.
     * @param desc Metric description.
     */
    void register(String name, IntSupplier supplier, @Nullable String desc);

    /**
     * Registers a long metric which value will be queried from the specified supplier.
     *
     * @param name Metric short name. Doesn't include registry name. 
     * @param supplier Metric value supplier.
     * @param desc Metric description.
     */
    void register(String name, LongSupplier supplier, @Nullable String desc);

    /**
     * Registers a double metric which value will be queried from the specified supplier.
     *
     * @param name Metric short name. Doesn't include the registry name.
     * @param supplier Metric value supplier.
     * @param desc Metric description.
     */
    void register(String name, DoubleSupplier supplier, @Nullable String desc);

    /**
     * Registers an object metric which value will be queried from the specified supplier.
     *
     * @param name Metric short name. Doesn't include registry name. 
     * @param supplier Metric value supplier.
     * @param type Metric value type.
     * @param desc Metric description.
     * @param <T> Metric value type.
     */
    <T> void register(String name, Supplier<T> supplier, Class<T> type, @Nullable String desc);

    /**
     * Registers a boolean metric which value will be queried from the specified supplier.
     *
     * @param name Metric short name. Doesn't include registry name. 
     * @param supplier Metric value supplier.
     * @param desc Metric description.
     */
    void register(String name, BooleanSupplier supplier, @Nullable String desc);

    /**
     * Removes metrics with the {@code name}.
     *
     * @param name Metric short name. Doesn't include registry name.
     */
    void remove(String name);
}
