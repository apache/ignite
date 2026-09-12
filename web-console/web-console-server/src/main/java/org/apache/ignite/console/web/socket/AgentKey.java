

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
