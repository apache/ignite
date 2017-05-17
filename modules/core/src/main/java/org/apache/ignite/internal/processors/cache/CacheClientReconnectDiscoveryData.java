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
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;

/**
 * Discovery data sent from client reconnecting to cluster.
 */
public class CacheClientReconnectDiscoveryData implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private final Map<String, CacheInfo> clientCaches;

    /**
     * @param clientCaches Information about caches started on re-joining client node.
     */
    CacheClientReconnectDiscoveryData(Map<String, CacheInfo> clientCaches) {
        this.clientCaches = clientCaches;
    }

    /**
     * @return Information about caches started on re-joining client node.
     */
    Map<String, CacheInfo> clientCaches() {
        return clientCaches;
    }

    /**
     *
     */
    static class CacheInfo implements Serializable {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private final CacheConfiguration ccfg;

        /** */
        private final CacheType cacheType;

        /** */
        private final IgniteUuid deploymentId;

        /** */
        private final boolean nearCache;

        /** Flags added for future usage. */
        private final byte flags;

        /**
         * @param ccfg Cache configuration.
         * @param cacheType Cache type.
         * @param deploymentId Cache deployment ID.
         * @param nearCache Near cache flag.
         * @param flags Flags (for future usage).
         */
        CacheInfo(CacheConfiguration ccfg,
            CacheType cacheType,
            IgniteUuid deploymentId,
            boolean nearCache,
            byte flags) {
            assert ccfg != null;
            assert cacheType != null;
            assert deploymentId != null;

            this.ccfg = ccfg;
            this.cacheType = cacheType;
            this.deploymentId = deploymentId;
            this.nearCache = nearCache;
            this.flags = flags;
        }

        /**
         * @return Cache configuration.
         */
        CacheConfiguration config() {
            return ccfg;
        }

        /**
         * @return Cache type.
         */
        CacheType cacheType() {
            return cacheType;
        }

        /**
         * @return Cache deployment ID.
         */
        IgniteUuid deploymentId() {
            return deploymentId;
        }

        /**
         * @return Near cache flag.
         */
        boolean nearCache() {
            return nearCache;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(CacheInfo.class, this);
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(CacheClientReconnectDiscoveryData.class, this);
    }
}
