package org.apache.ignite.internal.processors.service;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.NotNull;

/**
 * Service deployment topology when one or more instances of the service is deployed on only one node in the cluster.
 */
public class SingleNodeServiceTopology implements GridServiceTopology {
    /** Serialization version. */
    private static final long serialVersionUID = 0L;

    /** Number of service instances deployed on each node */
    @GridToStringInclude
    final private int cnt;

    /** Nodes in the topology */
    @GridToStringInclude
    final private UUID node;

    /**
     * Initializes new instance of {@link SingleNodeServiceTopology}
     *
     * @param cnt Number of service instances deployed on each node
     * @param node Nodes in the topology
     */
    public SingleNodeServiceTopology(int cnt, UUID node) {
        A.ensure(cnt > 0, "cnt must be positive");
        A.notNull(node, "node");

        this.cnt = cnt;
        this.node = node;
    }

    /** {@inheritDoc} */
    @NotNull @Override public Iterator<Map.Entry<UUID, Integer>> iterator() {
        Map<UUID, Integer> map = new HashMap<>(1);

        map.put(node, cnt);

        return map.entrySet().iterator();
    }

    /** {@inheritDoc} */
    @Override public int nodeServiceCount(UUID node) {
        return this.node.equals(node) ? cnt : 0;
    }

    /** {@inheritDoc} */
    @Override public int nodeCount() {
        return 1;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(SingleNodeServiceTopology.class, this);
    }
}
