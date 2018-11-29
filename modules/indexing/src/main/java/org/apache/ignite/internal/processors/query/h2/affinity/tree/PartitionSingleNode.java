package org.apache.ignite.internal.processors.query.h2.affinity.tree;

import java.util.Collection;
import java.util.Collections;

/**
 * Node with a single partition.
 */
public abstract class PartitionSingleNode implements PartitionNode {
    /** {@inheritDoc} */
    @Override public Collection<Integer> apply(PartitionResolver resolver, Object... args) {
        return Collections.singletonList(applySingle(resolver, args));
    }

    /**
     * Apply arguments and get single partition.
     *
     * @param args Arguments.
     * @return Partition.
     */
    public abstract int applySingle(PartitionResolver resolver, Object... args);
}
