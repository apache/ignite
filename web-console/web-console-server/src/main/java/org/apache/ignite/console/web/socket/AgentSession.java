

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
