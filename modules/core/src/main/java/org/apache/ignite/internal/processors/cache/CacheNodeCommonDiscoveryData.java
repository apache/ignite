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

package org.apache.ignite.internal.processors.cache;

import java.util.Map;
import java.util.UUID;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageFactory;
import org.jetbrains.annotations.Nullable;

/** Cache information sent in discovery data to joining node. */
public class CacheNodeCommonDiscoveryData implements Message {
    /** */
    @Order(0)
    @GridToStringInclude
    Map<String, CacheData> caches;

    /** */
    @Order(1)
    @GridToStringInclude
    Map<String, CacheData> templates;

    /** */
    @Order(2)
    @GridToStringInclude
    Map<Integer, CacheGroupData> cacheGrps;

    /** */
    @Order(3)
    Map<String, Map<UUID, Boolean>> clientNodesMap;

    /** */
    @Order(4)
    @Nullable ClusterCacheGroupRecoveryData clusterCacheGrpRecoveryData;

    /** Default constructor for {@link MessageFactory}. */
    public CacheNodeCommonDiscoveryData() {
        // No-op.
    }

    /**
     * @param caches Started caches.
     * @param templates Configured templates.
     * @param cacheGrps Started cache groups.
     * @param clientNodesMap Information about cache client nodes.
     * @param clusterCacheGrpRecoveryData Cluster cache group recovery data.
     */
    public CacheNodeCommonDiscoveryData(Map<String, CacheData> caches,
        Map<String, CacheData> templates,
        Map<Integer, CacheGroupData> cacheGrps,
        Map<String, Map<UUID, Boolean>> clientNodesMap,
        @Nullable ClusterCacheGroupRecoveryData clusterCacheGrpRecoveryData
    ) {
        assert caches != null;
        assert templates != null;
        assert cacheGrps != null;
        assert clientNodesMap != null;

        this.caches = caches;
        this.templates = templates;
        this.cacheGrps = cacheGrps;
        this.clientNodesMap = clientNodesMap;
        this.clusterCacheGrpRecoveryData = clusterCacheGrpRecoveryData;
    }

    /** @return Started cache groups. */
    Map<Integer, CacheGroupData> cacheGroups() {
        return cacheGrps;
    }

    /** */
    @Nullable ClusterCacheGroupRecoveryData clusterCacheGroupRecoveryData() {
        return clusterCacheGrpRecoveryData;
    }

    /** @return Started caches. */
    public Map<String, CacheData> caches() {
        return caches;
    }

    /** @return Configured templates. */
    public Map<String, CacheData> templates() {
        return templates;
    }

    /** @return Information about cache client nodes. */
    public Map<String, Map<UUID, Boolean>> clientNodesMap() {
        return clientNodesMap;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(CacheNodeCommonDiscoveryData.class, this);
    }
}
