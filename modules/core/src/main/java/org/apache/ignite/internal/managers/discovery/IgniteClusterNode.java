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

package org.apache.ignite.internal.managers.discovery;

import java.io.Serializable;
import java.util.Map;
import org.apache.ignite.cache.CacheMetrics;
import org.apache.ignite.cluster.ClusterMetrics;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;

/**
 *
 */
public interface IgniteClusterNode extends ClusterNode {
    /**
     * Sets consistent globally unique node ID which survives node restarts.
     *
     * @param consistentId Consistent globally unique node ID.
     */
    public void setConsistentId(Serializable consistentId);

    /**
     * Sets node metrics.
     *
     * @param metrics Node metrics.
     */
    public void setMetrics(ClusterMetrics metrics);

    /**
     * Gets collections of cache metrics for this node. Note that node cache metrics are constantly updated
     * and provide up to date information about caches.
     * <p>
     * Cache metrics are updated with some delay which is directly related to metrics update
     * frequency. For example, by default the update will happen every {@code 2} seconds.
     *
     * @return Runtime metrics snapshots for this node.
     */
    public Map<Integer, CacheMetrics> cacheMetrics();

    /**
     * Sets node cache metrics.
     *
     * @param cacheMetrics Cache metrics.
     */
    public void setCacheMetrics(Map<Integer, CacheMetrics> cacheMetrics);
}
