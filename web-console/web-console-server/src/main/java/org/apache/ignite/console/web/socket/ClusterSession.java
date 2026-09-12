

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
