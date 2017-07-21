package org.apache.ignite.internal.processors.service;

import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.NotNull;

/**
 * Service deployment topology with different number of service instances on different nodes.
 */
public class PolymorphicServiceTopology implements GridServiceTopology {
    /** Serialization version. */
    private static final long serialVersionUID = 0L;

    /** Node ID -> number of service instances map */
    @GridToStringInclude
    private final Map<UUID, Integer> nodeCntMap;

    /**
     * Initializes new instance of {@link PolymorphicServiceTopology}. Use {@link GridServiceTopologyFactory} to create
     * instances of this class.
     *
     * @param nodeCntMap Node ID -> number of service instances map
     */
    PolymorphicServiceTopology(Map<UUID, Integer> nodeCntMap) {
        A.notNull(nodeCntMap, "nodeCntMap");
        A.ensure(nodeCntMap.size() > 1, "nodeCntMap must contain multiple items");

        this.nodeCntMap = nodeCntMap;
    }

    /** {@inheritDoc} */
    @NotNull @Override public Iterator<Map.Entry<UUID, Integer>> iterator() {
        return nodeCntMap.entrySet().iterator();
    }

    /** {@inheritDoc} */
    @Override public int nodeServiceCount(UUID node) {
        Integer cnt = nodeCntMap.get(node);

        return cnt == null ? 0 : cnt;
    }

    /** {@inheritDoc} */
    @Override public int nodeCount() {
        return nodeCntMap.size();
    }

    /** {@inheritDoc} */
    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @Override public boolean equals(Object other) {
        // We assume here the nodeCntMap's Map implementation is based on java.util.AbstractMap, which properly
        // implements equals()
        return nodeCntMap.equals(other);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return nodeCntMap.hashCode();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(PolymorphicServiceTopology.class, this);
    }
}
