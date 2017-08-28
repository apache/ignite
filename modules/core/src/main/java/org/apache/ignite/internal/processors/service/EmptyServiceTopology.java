package org.apache.ignite.internal.processors.service;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.services.ServiceTopology;
import org.jetbrains.annotations.NotNull;

/**
 * "Empty" service deployment topology to describe the edge case.
 */
public class EmptyServiceTopology implements ServiceTopology {
    /** Serialization version. */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @NotNull @Override public Iterator<Map.Entry<UUID, Integer>> iterator() {
        return new HashMap<UUID, Integer>(0).entrySet().iterator();
    }

    /** {@inheritDoc} */
    @Override public int nodeServiceCount(UUID node) {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public int nodeCount() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object other) {
        return this == other || other instanceof EmptyServiceTopology;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return EmptyServiceTopology.class.hashCode();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(EmptyServiceTopology.class, this);
    }
}
