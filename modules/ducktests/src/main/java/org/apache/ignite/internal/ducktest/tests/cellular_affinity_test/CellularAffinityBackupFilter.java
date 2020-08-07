package org.apache.ignite.internal.ducktest.tests.cellular_affinity_test;

import java.util.List;
import java.util.Objects;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.lang.IgniteBiPredicate;

/**
 *
 */
public class CellularAffinityBackupFilter implements IgniteBiPredicate<ClusterNode, List<ClusterNode>> {
    /** */
    private static final long serialVersionUID = 1L;

    /** Attribute name. */
    private final String attrName;

    /**
     * @param attrName The attribute name for the attribute to compare.
     */
    public CellularAffinityBackupFilter(String attrName) {
        this.attrName = attrName;
    }

    /**
     * Defines a predicate which returns {@code true} if a node is acceptable for a backup
     * or {@code false} otherwise. An acceptable node is one where its attribute value
     * is exact match with previously selected nodes.  If an attribute does not
     * exist on exactly one node of a pair, then the attribute does not match.  If the attribute
     * does not exist both nodes of a pair, then the attribute matches.
     *
     * @param candidate          A node that is a candidate for becoming a backup node for a partition.
     * @param previouslySelected A list of primary/backup nodes already chosen for a partition.
     *                           The primary is first.
     */
    @Override public boolean apply(ClusterNode candidate, List<ClusterNode> previouslySelected) {
        for (ClusterNode node : previouslySelected)
            return Objects.equals(candidate.attribute(attrName), node.attribute(attrName));

        return true;
    }
}