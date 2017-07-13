package org.apache.ignite.internal.processors.service;

import java.util.AbstractMap;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.NotNull;

/**
 * Service deployment topology when same number of services is deployed on each node.
 */
public class HomomorphicServiceTopology implements GridServiceTopology {
    /** Serialization version. */
    private static final long serialVersionUID = 0L;

    /** Number of service instances deployed on each node */
    @GridToStringInclude
    final private int cntPerNode;

    /** Nodes in the topology */
    @GridToStringInclude
    final private Collection<UUID> nodes;

    /**
     * Initializes new instance of {@link HomomorphicServiceTopology}
     *
     * @param cntPerNode Number of service instances deployed on each node
     * @param nodes Nodes in the topology
     */
    public HomomorphicServiceTopology(int cntPerNode, Collection<UUID> nodes) {
        A.ensure(cntPerNode > 0, "cntPerNode must be positive");
        A.notNull(nodes, "nodes");

        this.cntPerNode = cntPerNode;
        this.nodes = nodes;
    }

    /** {@inheritDoc} */
    @NotNull @Override public Iterator<Map.Entry<UUID, Integer>> iterator() {
        final Iterator<UUID> it = nodes.iterator();

        return new Iterator<Map.Entry<UUID, Integer>>() {
            @Override public boolean hasNext() {
                return it.hasNext();
            }

            @Override public Map.Entry<UUID, Integer> next() {
                return new AbstractMap.SimpleEntry<>(it.next(), HomomorphicServiceTopology.this.cntPerNode);
            }

            @Override public void remove() {
                it.remove();
            }
        };
    }

    /** {@inheritDoc} */
    @Override public int nodeServiceCount(UUID node) {
        for (UUID n : nodes) {
            if (n.equals(node))
                return cntPerNode;
        }

        return 0;
    }

    /** {@inheritDoc} */
    @Override public int nodeCount() {
        return nodes.size();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(HomomorphicServiceTopology.class, this);
    }
}
