package org.apache.ignite.internal.processors.service;

import java.io.Serializable;
import java.util.Map;
import java.util.UUID;

/**
 * Service deployment topology.
 * Iterate {@link GridServiceTopology} instances to learn how many and which nodes services are deployed on.
 * The entry's key is a cluster node ID and the entry's value is the number of service instances deployed on that node.
 *
 * Implementations of this interface are not thread safe.
 */
public interface GridServiceTopology extends Iterable<Map.Entry<UUID, Integer>>, Serializable {
    /**
     * @param node Cluster node ID
     * @return Number of service instances on specific node
     */
    int nodeServiceCount(UUID node);

    /**
     * @return Number of nodes
     */
    int nodeCount();
}
