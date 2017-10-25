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
import java.util.Set;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 *
 */
class ClusterCachesReconnectResult {
    /** */
    private final Set<Integer> stoppedCacheGrps;

    /** */
    private final Set<String> stoppedCaches;

    /** */
    private final Set<Integer> reusedCacheGrps;

    /** */
    private final Map<String, DynamicCacheDescriptor> reusedCaches;

    /**
     * @param stoppedCacheGrps Stopped cache groups.
     * @param stoppedCaches Stopped caches.
     * @param reusedCacheGrps Reused cache groups with configurations.
     * @param reusedCaches Reused caches with configurations.
     */
    ClusterCachesReconnectResult(Set<Integer> stoppedCacheGrps, Set<String> stoppedCaches,
        Set<Integer> reusedCacheGrps, Map<String, DynamicCacheDescriptor> reusedCaches) {
        this.stoppedCacheGrps = stoppedCacheGrps;
        this.stoppedCaches = stoppedCaches;
        this.reusedCacheGrps = reusedCacheGrps;
        this.reusedCaches = reusedCaches;
    }

    /**
     * @return Stopped cache groups.
     */
    Set<Integer> stoppedCacheGroups() {
        return stoppedCacheGrps;
    }

    /**
     * @return Stopped caches.
     */
    Set<String> stoppedCaches() {
        return stoppedCaches;
    }

    /**
     * @return Restarted cache groups.
     */
    Set<Integer> reusedCacheGroups() {
        return reusedCacheGrps;
    }

    /**
     * @return Restarted caches.
     */
    Map<String, DynamicCacheDescriptor> reusedCaches() {
        return reusedCaches;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ClusterCachesReconnectResult.class, this);
    }
}
