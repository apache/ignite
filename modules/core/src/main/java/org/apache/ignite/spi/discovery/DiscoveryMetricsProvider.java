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

package org.apache.ignite.spi.discovery;

import java.util.Map;
import org.apache.ignite.cache.CacheMetrics;
import org.apache.ignite.cluster.ClusterMetrics;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;

/**
 * Provides metrics to discovery SPI. It is responsibility of discovery SPI
 * to make sure that all nodes have updated metrics data about each other.
 * <p>
 * Ignite implementation will supply discovery SPI with metrics provider
 * via {@link DiscoverySpi#setMetricsProvider(DiscoveryMetricsProvider)}
 * method.
 */
@GridToStringExclude
public interface DiscoveryMetricsProvider {
    /**
     * This method always returns up-to-date metrics data about local node.
     *
     * @return Up to date metrics data about local node.
     */
    public ClusterMetrics metrics();

    /**
     * Returns metrics data about all caches on local node.
     *
     * @return metrics data about all caches on local node.
     */
    public Map<Integer, CacheMetrics> cacheMetrics();
}