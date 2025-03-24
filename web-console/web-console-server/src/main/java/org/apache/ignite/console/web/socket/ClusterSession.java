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

package org.apache.ignite.console.web.socket;

import java.util.Objects;
import java.util.UUID;

/**
 * Connected cluster descriptor.
 */
public class ClusterSession {
    /** Backend node ID. */
    private final UUID nid;
    
    /** Cluster ID. */
    private final String clusterId;

    /**
     * @param nid Backend node ID.
     * @param clusterId Cluster ID.
     */
    public ClusterSession(UUID nid, String clusterId) {
        this.nid = nid;
        this.clusterId = clusterId;
    }

    /**
     * @return value of backend node ID.
     */
    public UUID getNid() {
        return nid;
    }

    /**
     * @return value of cluster ID.
     */
    public String getClusterId() {
        return clusterId;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        ClusterSession key = (ClusterSession)o;

        return nid.equals(key.nid) && clusterId.equals(key.clusterId);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return Objects.hash(nid, clusterId);
    }
}
