package org.apache.ignite.internal.processors.query.h2.affinity.join;

import java.util.Collection;
import java.util.Collections;
import java.util.IdentityHashMap;

public class PartitionJoinGroup {
    /** Tables within a group. */
    private final Collection<PartitionJoinTable> tbls = Collections.newSetFromMap(new IdentityHashMap<>());

    /** Tables that were left joined to the group (i.e. these are tables that were on the right side of LJ. */
    private final Collection<PartitionJoinGroup> outerTbls = Collections.newSetFromMap(new IdentityHashMap<>());

    /** Affinity function identifier. */
    private final PartitionJoinAffinityIdentifier affIdentifier;

    /** Whether this is replicated group. */
    private final boolean replicated;

    public PartitionJoinGroup(PartitionJoinAffinityIdentifier affIdentifier, boolean replicated) {
        this.affIdentifier = affIdentifier;
        this.replicated = replicated;
    }
}
