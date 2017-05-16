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

import org.apache.ignite.internal.util.typedef.internal.S;

import java.util.Map;
import java.util.Set;

/**
 *
 */
class ClusterCachesReconnectResult {
    /** */
    private final Set<Integer> stoppedCacheGrps;

    /** */
    private final Set<String> stoppedCaches;

    /** */
    private final Map<Integer, Integer> newCacheGrpIds;

    /**
     * @param stoppedCacheGrps Stopped cache groups.
     * @param stoppedCaches Stopped caches.
     */
    ClusterCachesReconnectResult(Set<Integer> stoppedCacheGrps,
        Set<String> stoppedCaches,
        Map<Integer, Integer> newCacheGrpIds) {
        this.stoppedCacheGrps = stoppedCacheGrps;
        this.stoppedCaches = stoppedCaches;
        this.newCacheGrpIds = newCacheGrpIds;
    }

    Map<Integer, Integer> newCacheGroupIds() {
        return newCacheGrpIds;
    }

    Set<Integer> stoppedCacheGroups() {
        return stoppedCacheGrps;
    }

    Set<String> stoppedCaches() {
        return stoppedCaches;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ClusterCachesReconnectResult.class, this);
    }
}
