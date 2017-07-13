package org.apache.ignite.internal.processors.service;

import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
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
    private final Map<UUID, Integer> nodeCntMap;

    /**
     * Initializes new instance of {@link PolymorphicServiceTopology}
     *
     * @param nodeCntMap Node ID -> number of service instances map
     */
    public PolymorphicServiceTopology(Map<UUID, Integer> nodeCntMap) {
        A.notNull(nodeCntMap, "nodeCntMap");

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
    @Override public String toString() {
        return S.toString(PolymorphicServiceTopology.class, this);
    }
}
