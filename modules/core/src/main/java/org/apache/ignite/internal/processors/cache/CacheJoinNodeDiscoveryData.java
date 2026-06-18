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
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.jetbrains.annotations.Nullable;

/**
 * Information about configured caches sent from joining node.
 */
public class CacheJoinNodeDiscoveryData implements Message {
    /** */
    @Order(0)
    @GridToStringInclude
    Map<String, CacheJoinInfo> caches;

    /** */
    @Order(1)
    @GridToStringInclude
    Map<String, CacheJoinInfo> templates;

    /** */
    @Order(2)
    @GridToStringInclude
    IgniteUuid cacheDeploymentId;

    /** */
    @Order(3)
    boolean startCaches;

    /** */
    @Order(4)
    @Nullable ClusterCacheGroupRecoveryData clusterCacheGrpRecoveryData;

    /** */
    public CacheJoinNodeDiscoveryData() { }

    /**
     * @param cacheDeploymentId Deployment ID for started caches.
     * @param caches Caches.
     * @param templates Templates.
     * @param startCaches {@code True} if required to start all caches on joining node.
     */
    public CacheJoinNodeDiscoveryData(
        IgniteUuid cacheDeploymentId,
        Map<String, CacheJoinInfo> caches,
        Map<String, CacheJoinInfo> templates,
        boolean startCaches
    ) {
        this.cacheDeploymentId = cacheDeploymentId;
        this.caches = caches;
        this.templates = templates;
        this.startCaches = startCaches;
    }

    /**
     * @return {@code True} if required to start all caches on joining node.
     */
    boolean startCaches() {
        return startCaches;
    }

    /**
     * @return Deployment ID assigned on joining node.
     */
    public IgniteUuid cacheDeploymentId() {
        return cacheDeploymentId;
    }

    /**
     * @return Templates configured on joining node.
     */
    public Map<String, CacheJoinInfo> templates() {
        return templates;
    }

    /**
     * @return Caches configured on joining node.
     */
    public Map<String, CacheJoinInfo> caches() {
        return caches;
    }

    /** */
    public void clusterCacheGroupRecoveryData(@Nullable ClusterCacheGroupRecoveryData clusterCacheGrpRecoveryData) {
        this.clusterCacheGrpRecoveryData = clusterCacheGrpRecoveryData;
    }

    /** */
    @Nullable public ClusterCacheGroupRecoveryData clusterCacheGroupRecoveryData() {
        return clusterCacheGrpRecoveryData;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(CacheJoinNodeDiscoveryData.class, this);
    }
}
