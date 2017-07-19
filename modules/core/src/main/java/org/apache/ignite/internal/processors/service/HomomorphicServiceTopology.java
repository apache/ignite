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
    final private int cnt;

    /** Nodes in the topology */
    @GridToStringInclude
    final private Collection<UUID> nodes;

    /**
     * Initializes new instance of {@link HomomorphicServiceTopology}. Use {@link GridServiceTopologyFactory} to create
     * instances of this class.
     *
     * @param nodes Nodes in the topology
     * @param cnt Number of service instances deployed on each node
     */
    HomomorphicServiceTopology(Collection<UUID> nodes, int cnt) {
        A.ensure(cnt > 0, "cnt must be positive");
        A.notNull(nodes, "nodes");
        A.ensure(nodes.size() > 0, "nodes must not be empty");

        this.cnt = cnt;
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
                return new AbstractMap.SimpleEntry<>(it.next(), HomomorphicServiceTopology.this.cnt);
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
                return cnt;
        }

        return 0;
    }

    /** {@inheritDoc} */
    @Override public int nodeCount() {
        return nodes.size();
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object other) {
        if (this == other)
            return true;

        if (!(other instanceof HomomorphicServiceTopology))
            return false;

        HomomorphicServiceTopology otherTop = (HomomorphicServiceTopology)other;

        if (cnt != otherTop.cnt)
            return false;

        if (nodes.size() != otherTop.nodes.size())
            return false;

        Iterator<UUID> it = nodes.iterator(), otherIt = otherTop.nodes.iterator();

        while (it.hasNext()) {
            UUID id = it.next();
            UUID otherId = otherIt.next();

            if (!id.equals(otherId))
                return false;
        }

        return true;

    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int res = 33;

        res = 31 * res + cnt;

        for (UUID id : nodes)
            res = 31 * res + id.hashCode();

        return res;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(HomomorphicServiceTopology.class, this);
    }
}
