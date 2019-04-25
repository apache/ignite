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

package org.apache.ignite.plugin;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.DiscoverySpi;

/**
 * Cache plugin context.
 */
public interface CachePluginContext<C extends CachePluginConfiguration> {
    /**
     * @return Ignite configuration.
     */
    public IgniteConfiguration igniteConfiguration();
    
    /**
     * @return Ignite cache configuration.
     */
    public CacheConfiguration igniteCacheConfiguration();

    /**
     * @return Grid.
     */
    public Ignite grid();

    /**
     * Gets local grid node. Instance of local node is provided by underlying {@link DiscoverySpi}
     * implementation used.
     *
     * @return Local grid node.
     * @see DiscoverySpi
     */
    public ClusterNode localNode();

    /**
     * Gets logger for given class.
     *
     * @param cls Class to get logger for.
     * @return Logger.
     */
    public IgniteLogger log(Class<?> cls);
}