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
 * Connected agent key.
 */
public class AgentKey {
    /** Account ID. */
    private final UUID accId;

    /** Cluster ID. */
    private final String clusterId;

    /**
     * @param accId Account ID.
     * @param clusterId Cluster ID.
     */
    public AgentKey(UUID accId, String clusterId) {
        this.accId = accId;
        this.clusterId = clusterId;
    }

    /**
     * @param accId Account ID.
     */
    public AgentKey(UUID accId) {
        this(accId, null);
    }

    /**
     * @param clusterId Cluster ID.
     */
    public AgentKey(String clusterId) {
        this(null, clusterId);
    }

    /**
     * @return value of account ID.
     */
    public UUID getAccId() {
        return accId;
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

        AgentKey key = (AgentKey)o;

        return accId.equals(key.accId) && Objects.equals(clusterId, key.clusterId);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return Objects.hash(accId, clusterId);
    }
}
