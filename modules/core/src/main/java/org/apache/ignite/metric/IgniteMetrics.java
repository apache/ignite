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
 * Allows to manage custom metrics and to get any read only metrics including the Ignite's internals.
 *
 * Note: Names of custom metric registries are required to start with 'custom.' (lower case), may have additional
 * dot-separated qualifiers. Any custom name qualifier cannot have spaces and must not be empty. Names of internal
 * metrics do not meet such requirement.
 * Examples of custom metric registry names: "custom", "custom.admin", "custom.admin.sessions", "custom.processes", etc.
 *
 * @see ReadOnlyMetricRegistry
 * @see IgniteMetricRegistry
 */
@IgniteExperimental
public interface IgniteMetrics extends Iterable<ReadOnlyMetricRegistry> {
    /**
     * Gets or creates custom metric registry named "custom." + the passed name.
     *
     * @param registryName name part to add to the prefix "custom.".
     * @return {@link IgniteMetricRegistry} registry.
     */
    IgniteMetricRegistry customRegistry(String registryName);

    /**
     * Gets or creates custom metric registry named "custom.".
     *
     * @return {@link IgniteMetricRegistry} registry.
     */
    default IgniteMetricRegistry customRegistry() {
        return customRegistry(null);
    }

    /**
     * Gets metric registry including the Ignite's internal registries.
     * <p>
     * Note: Names of custom metric registries always start with 'custom.'.
     *
     * @param registryName Registry name.
     * @return Certain read-only metric registry.
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

    /**
     * Removes custom metric registry with name '.custom'.
     */
    default void removeCustomRegistry() {
        removeCustomRegistry(null);
    }
}
