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

import org.apache.ignite.lang.IgniteExperimental;
import org.apache.ignite.spi.metric.ReadOnlyMetricRegistry;
import org.jetbrains.annotations.Nullable;

/**
 * Allows to manage custom metrics.
 * <p>
 * Metrics are grouped into registries (groups). Every metric has full name which is the conjunction of registry name
 * and the metric short name. Within a registry metric has only its own short name.
 * <p>
 * Note: Names of custom metric registries are required to start with 'custom.' (lower case) and may have additional
 * dot-separated qualifiers. The prefix is automatically added if missed. For example, if provided custom registry name
 * is "a.b.c.mname", it is automatically extended to "custom.a.b.c.mname".
 * <p>
 * Any name or dot-separated name part cannot have spaces and must not be empty.
 * <p>
 * Examples of custom metric registry names: "custom.admin", "custom.admin.sessions", "custom.processes", etc.
 *
 * @see ReadOnlyMetricRegistry
 * @see MetricRegistry
 */
@IgniteExperimental
public interface IgniteMetrics extends Iterable<ReadOnlyMetricRegistry> {
    /**
     * Gets or creates custom metric registry named "custom." + {@code registryName}.
     *
     * @param registryName name part to add to the prefix "custom.".
     * @return {@link MetricRegistry} registry.
     */
    MetricRegistry customRegistry(String registryName);


    /**
     * Gets custom metric registry.
     * <p>
     * Note: Names of custom metric registries always start with 'custom.'.
     *
     * @param registryName Registry name.
     * @return Certain read-only metric registry. {@code Null} if registry is not found.
     */
    @Nullable ReadOnlyMetricRegistry findRegistry(String registryName);

    /**
     * Removes custom metric registry.
     * <p>
     * Note: The registry name have to follow the name convention as described at {@link IgniteMetrics}.
     *
     * @param registryName Registry name starting with 'custom.'.
     */
    void removeCustomRegistry(String registryName);
}
