package org.apache.ignite.internal.processors.query.h2.affinity.tree;

import java.util.Collection;

/**
 * Composite node which consists of two child nodes and a relation between them.
 */
public class PartitionCompositeNode implements PartitionNode {
    /** Left node. */
    private final PartitionNode left;

    /** Right node. */
    private final PartitionNode right;

    /** Operator. */
    private final PartitionCompositeNodeOperator op;

    /**
     * Constructor.
     *
     * @param left Left node.
     * @param right Right node.
     * @param op Operator.
     */
    public PartitionCompositeNode(PartitionNode left, PartitionNode right, PartitionCompositeNodeOperator op) {
        this.left = left;
        this.right = right;
        this.op = op;
    }

    /** {@inheritDoc} */
    @Override public Collection<Integer> apply(PartitionResolver resolver, Object... args) {
        Collection<Integer> leftParts = left.apply(resolver, args);
        Collection<Integer> rightParts = right.apply(resolver, args);

        if (op == PartitionCompositeNodeOperator.AND)
            leftParts.retainAll(rightParts);
        else {
            assert op == PartitionCompositeNodeOperator.OR;

            leftParts.addAll(rightParts);
        }

        return leftParts;
    }
}
