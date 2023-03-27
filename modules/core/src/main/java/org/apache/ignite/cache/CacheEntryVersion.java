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

package org.apache.ignite.cache;

import java.io.Serializable;
import java.util.Map;
import org.apache.ignite.internal.processors.cache.CacheConflictResolutionManager;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersionManager;
import org.apache.ignite.lang.IgniteExperimental;

/**
 * Entry event order.
 * Two concurrent updates of the same entry can be ordered based on {@link CacheEntryVersion} comparsion.
 * Greater value means that event occurs later.
 *
 * @see CacheConflictResolutionManager
 * @see GridCacheVersionManager#dataCenterId(byte)
 */
@IgniteExperimental
public interface CacheEntryVersion extends Comparable<CacheEntryVersion>, Serializable {
    /**
     * Order of the update. Value is an incremental counter value. Scope of counter is node.
     * @return Version order.
     */
    public long order();

    /** @return Node order on which this version was assigned. */
    public int nodeOrder();

    /**
     * Cluster id is a value to distinguish updates in case user wants to aggregate and sort updates from several
     * Ignite clusters. {@code clusterId} id can be set for the node using
     * {@link GridCacheVersionManager#dataCenterId(byte)}.
     *
     * @return Cluster id.
     */
    public byte clusterId();

    /** @return Topology version plus number of seconds from the start time of the first grid node. */
    public int topologyVersion();

    /**
     * If source of the update is "local" cluster then {@code null} will be returned.
     * If updated comes from the other cluster using {@link IgniteInternalCache#putAllConflict(Map)}
     * then entry version for other cluster.
     * @return Replication version.
     * @see IgniteInternalCache#putAllConflict(Map)
     * @see IgniteInternalCache#removeAllConflict(Map)
     */
    public CacheEntryVersion otherClusterVersion();
}
