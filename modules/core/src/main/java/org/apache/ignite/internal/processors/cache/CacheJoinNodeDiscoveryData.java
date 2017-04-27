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

import java.io.Serializable;
import java.util.Map;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.lang.IgniteUuid;

/**
 * Information about configured caches sent from joining node.
 */
class CacheJoinNodeDiscoveryData implements Serializable {
    /** */
    private final Map<String, CacheInfo> caches;

    /** */
    private final Map<String, CacheInfo> templates;

    /** */
    private final IgniteUuid cacheDeploymentId;

    /**
     * @param cacheDeploymentId Deployment ID for started caches.
     * @param caches Caches.
     * @param templates Templates.
     */
    CacheJoinNodeDiscoveryData(
        IgniteUuid cacheDeploymentId,
        Map<String, CacheJoinNodeDiscoveryData.CacheInfo> caches,
        Map<String, CacheJoinNodeDiscoveryData.CacheInfo> templates) {
        this.cacheDeploymentId = cacheDeploymentId;
        this.caches = caches;
        this.templates = templates;
    }

    IgniteUuid cacheDeploymentId() {
        return cacheDeploymentId;
    }

    Map<String, CacheInfo> templates() {
        return templates;
    }

    Map<String, CacheInfo> caches() {
        return caches;
    }

    /**
     *
     */
    static class CacheInfo implements Serializable {
        /** */
        private final CacheConfiguration ccfg;

        /** */
        private final CacheType cacheType;

        /** */
        private final byte flags;

        CacheInfo(CacheConfiguration ccfg, CacheType cacheType, byte flags) {
            this.ccfg = ccfg;
            this.cacheType = cacheType;
            this.flags = flags;
        }

        CacheConfiguration config() {
            return ccfg;
        }

        CacheType cacheType() {
            return cacheType;
        }
    }
}
