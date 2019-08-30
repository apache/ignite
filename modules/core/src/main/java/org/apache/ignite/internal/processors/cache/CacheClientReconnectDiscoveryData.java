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

package org.apache.ignite.internal.processors.cache;

import java.io.Serializable;
import java.util.Map;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Discovery data sent from client reconnecting to cluster.
 */
public class CacheClientReconnectDiscoveryData implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Cache name -> Cache info. */
    private final Map<String, CacheDiscoveryInfo> clientCaches;

    /**
     * @param clientCaches Information about caches started on re-joining client node.
     */
    public CacheClientReconnectDiscoveryData(Map<String, CacheDiscoveryInfo> clientCaches) {
        this.clientCaches = clientCaches;
    }

    /**
     * @return Information about caches started on re-joining client node.
     */
    public Map<String, CacheDiscoveryInfo> clientCaches() {
        return clientCaches;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(CacheClientReconnectDiscoveryData.class, this);
    }
}
