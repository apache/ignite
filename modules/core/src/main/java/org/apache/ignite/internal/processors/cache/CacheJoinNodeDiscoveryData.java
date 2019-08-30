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
import java.util.Collections;
import java.util.Map;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;

/**
 * Information about configured caches sent from joining node.
 */
public class CacheJoinNodeDiscoveryData implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    @GridToStringInclude
    private final IgniteUuid deploymentId;

    /** */
    @GridToStringInclude
    private final Map<String, CacheDiscoveryInfo> caches;

    /** */
    private final boolean startCaches;

    /**
     * @param deploymentId Deployment ID for started caches.
     * @param caches Caches.
     * @param startCaches {@code True} if required to start all caches on joining node.
     */
    public CacheJoinNodeDiscoveryData(
        IgniteUuid deploymentId,
        Map<String, CacheDiscoveryInfo> caches,
        boolean startCaches
    ) {
        this.deploymentId = deploymentId;
        this.caches = caches;
        this.startCaches = startCaches;
    }

    /**
     *
     * @return {@code True} if required to start all caches on joining node if it's client.
     */
    boolean startCaches() {
        return startCaches;
    }

    /**
     * @return Deployment ID assigned on joining node.
     */
    public IgniteUuid deploymentId() {
        return deploymentId;
    }

    /**
     * @return Caches configured on joining node.
     */
    public Map<String, CacheDiscoveryInfo> caches() {
        return Collections.unmodifiableMap(caches);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(CacheJoinNodeDiscoveryData.class, this);
    }
}
