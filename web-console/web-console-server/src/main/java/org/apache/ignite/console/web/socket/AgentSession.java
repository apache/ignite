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

import java.util.Collections;
import java.util.Set;
import java.util.UUID;

/**
 * Agent session descriptor.
 */
public class AgentSession {
    /** */
    private final Set<UUID> accIds;

    /** */
    private Set<String> clusterIds = Collections.emptySet();

    /**
     * @param accIds Account IDs.
     */
    AgentSession(Set<UUID> accIds) {
        this.accIds = accIds;
    }

    /**
     * @param accId Account ID.
     * @return {@code True} if contained the specified account.
     */
    boolean revokeAccount(UUID accId) {
        return accIds.remove(accId);
    }

    /**
     * @return {@code True} if connection to agent can be closed.
     */
    boolean canBeClosed() {
        return accIds.isEmpty();
    }

    /**
     * @return Account IDs.
     */
    public Set<UUID> getAccIds() {
        return accIds;
    }

    /**
     * @return Cluster IDs.
     */
    public Set<String> getClusterIds() {
        return clusterIds;
    }

    /**
     * @param clusterIds Cluster IDs.
     */
    public void setClusterIds(Set<String> clusterIds) {
        this.clusterIds = clusterIds;
    }
}
