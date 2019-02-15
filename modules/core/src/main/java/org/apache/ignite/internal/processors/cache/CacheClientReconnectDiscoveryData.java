/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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
    private final Map<Integer, CacheGroupInfo> clientCacheGrps;

    /** */
    private final Map<String, CacheInfo> clientCaches;

    /**
     * @param clientCaches Information about caches started on re-joining client node.
     * @param clientCacheGrps Information about cach groups started on re-joining client node.
     */
    CacheClientReconnectDiscoveryData(Map<Integer, CacheGroupInfo> clientCacheGrps,
        Map<String, CacheInfo> clientCaches) {
        this.clientCacheGrps = clientCacheGrps;
        this.clientCaches = clientCaches;
    }

    /**
     * @return Information about caches started on re-joining client node.
     */
    Map<Integer, CacheGroupInfo> clientCacheGroups() {
        return clientCacheGrps;
    }

    /**
     * @return Information about caches started on re-joining client node.
     */
   public Map<String, CacheInfo> clientCaches() {
        return clientCaches;
    }

    /**
     *
     */
    static class CacheGroupInfo implements Serializable {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private final CacheConfiguration ccfg;

        /** */
        private final IgniteUuid deploymentId;

        /** Flags added for future usage. */
        private final long flags;

        /**
         * @param ccfg Cache group configuration.
         * @param deploymentId Cache group deployment ID.
         * @param flags Flags (for future usage).
         */
        CacheGroupInfo(CacheConfiguration ccfg,
            IgniteUuid deploymentId,
            long flags) {
            assert ccfg != null;
            assert deploymentId != null;

            this.ccfg = ccfg;
            this.deploymentId = deploymentId;
            this.flags = flags;
        }

        /**
         * @return Cache group configuration.
         */
        CacheConfiguration config() {
            return ccfg;
        }

        /**
         * @return Cache group deployment ID.
         */
        IgniteUuid deploymentId() {
            return deploymentId;
        }
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
        private final long flags;

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
            long flags) {
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
