/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.spi.metric;

import java.util.Collection;
import java.util.function.Consumer;
import org.jetbrains.annotations.Nullable;

/**
 * Read only metric registry.
 */
public interface ReadOnlyMetricRegistry {
    /**
     * @param prefix prefix for all metrics.
     * @return Proxy implementation that will search and create only metrics with specified prefix.
     */
    public <T extends ReadOnlyMetricRegistry> T withPrefix(String prefix);

    /**
     * Prefixes combined using dot notation {@code ["io", "stat"] -> "io.stat"}
     *
     * @param prefixes prefixes for all metrics.
     * @return Proxy implementation that will search and create only metrics with specified prefixes.
     */
    public <T extends ReadOnlyMetricRegistry> T withPrefix(String... prefixes);

    /**
     * @return Metrics stored in this group.
     */
    public Collection<Metric> getMetrics();

    /**
     * Adds listener of metrics sets creation events.
     *
     * @param lsnr Listener.
     */
    public void addMetricCreationListener(Consumer<Metric> lsnr);

    /**
     * @param name Name of the metric
     * @return Metric with specified name if exists. Null otherwise.
     */
    @Nullable public Metric findMetric(String name);
}
