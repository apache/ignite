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

/**
 * Allows managing the custom metrics.
 * <p>
 * Metrics are grouped into registries (groups). Every metric has a full name which is the conjunction of registry name
 * and the metric short name. Within a registry metric has only its own short name.
 * <p>
 * Note: The prefix 'custom.' is automatically added to the registry name. For example, if provided registry name
 * is "a.b.c.mname", it is automatically extended to "custom.a.b.c.mname".
 * <p>
 * Note: Custom metrics are registered on demand and aren't stored. If node restarts, the metrics require registration anew.
 * <p>
 * Any name or dot-separated name part must not be empty and cannot have spaces.
 * Examples of custom metric registry names: "admin.sessions", "processes.management", "monitoring.load.idle", etc.
 *
 * @see ReadOnlyMetricRegistry
 * @see MetricRegistry
 */
@IgniteExperimental
public interface IgniteMetrics extends Iterable<ReadOnlyMetricRegistry> {
    /**
     * Gets or creates custom metric registry.
     *
     * @param registryName Registry name part to add to the prefix "custom.".
     * @return {@link MetricRegistry} registry.
     */
    MetricRegistry getOrCreate(String registryName);

    /**
     * Removes custom metric registry.
     *
     * @param registryName Registry name part to add to the prefix "custom.".
     */
    void remove(String registryName);
}
