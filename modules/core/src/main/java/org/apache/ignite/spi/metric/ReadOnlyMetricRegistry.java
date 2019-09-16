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

import java.util.function.Consumer;
import org.apache.ignite.internal.processors.metric.MetricRegistry;

/**
 * Read only metric registry.
 */
public interface ReadOnlyMetricRegistry extends Iterable<MetricRegistry> {
    /**
     * Adds listener of metrics group creation events.
     *
     * @param lsnr Listener.
     */
    public void addMetricRegistryCreationListener(Consumer<MetricRegistry> lsnr);

    /**
     * Adds listener of metrics group remove events.
     *
     * @param lsnr Listener.
     */
    public void addMetricRegistryRemoveListener(Consumer<MetricRegistry> lsnr);
}
