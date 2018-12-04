/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.query.h2.affinity.tree;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

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
    @Override public Collection<Integer> apply(Object... args) {
        Collection<Integer> leftParts = left.apply(args);
        Collection<Integer> rightParts = right.apply(args);

        if (op == PartitionCompositeNodeOperator.AND)
            leftParts.retainAll(rightParts);
        else {
            assert op == PartitionCompositeNodeOperator.OR;

            leftParts.addAll(rightParts);
        }

        return leftParts;
    }

    /** {@inheritDoc} */
    @Override public PartitionNode optimize() {
        // If one of child nodes cannot be optimized, nothing can be done.
        if (left instanceof PartitionCompositeNode || right instanceof PartitionCompositeNode)
            return this;

        // ALL and NONE always can be optimized.
        if (left == PartitionAllNode.INSTANCE || left == PartitionNoneNode.INSTANCE)
            return optimizeSpecial(left, right);

        if (right == PartitionAllNode.INSTANCE || right == PartitionNoneNode.INSTANCE)
            return optimizeSpecial(right, left);

        // Try optimizing composite nodes.
        if (left instanceof PartitionGroupNode)
            return optimizeGroup((PartitionGroupNode)left, right);

        if (right instanceof PartitionGroupNode)
            return optimizeGroup((PartitionGroupNode)right, left);

        // Finally, optimize simple nodes.
        assert left instanceof PartitionSingleNode;
        assert right instanceof PartitionSingleNode;

        return optimizeSimple((PartitionSingleNode)left, (PartitionSingleNode)right);
    }

    /**
     * Optimize special nodes.
     *
     * @param left Left (always special).
     * @param right Right (may be special).
     * @return Result.
     */
    private PartitionNode optimizeSpecial(PartitionNode left, PartitionNode right) {
        if (left == PartitionAllNode.INSTANCE) {
            if (op == PartitionCompositeNodeOperator.OR)
                // ALL or (...) -> ALL.
                return PartitionAllNode.INSTANCE;
            else {
                // ALL and (...) -> (...).
                assert op == PartitionCompositeNodeOperator.AND;

                return right;
            }
        }
        else {
            assert left == PartitionNoneNode.INSTANCE;

            if (op == PartitionCompositeNodeOperator.OR)
                // NONE or (...) -> (...).
                return right;
            else {
                // NONE and (...) -> NONE.
                assert op == PartitionCompositeNodeOperator.AND;

                return PartitionNoneNode.INSTANCE;
            }
        }
    }

    /**
     * Optimize group node.
     *
     * @param left Left node (group).
     * @param right Right node (group or simple).
     * @return Optimization result.
     */
    private PartitionNode optimizeGroup(PartitionGroupNode left, PartitionNode right) {
        if (op == PartitionCompositeNodeOperator.AND)
            return optimizeGroupAnd(left, right);
        else {
            assert op == PartitionCompositeNodeOperator.OR;

            return optimizeGroupOr(left, right);
        }
    }

    /**
     * Optimize conjunction between group node and group or single node.
     *
     * @param left Left node.
     * @param right Right node.
     * @return Optimized node.
     */
    private PartitionNode optimizeGroupAnd(PartitionGroupNode left, PartitionNode right) {
        // Optimistic check whether both sides are equal.
        if (right instanceof PartitionGroupNode) {
            PartitionGroupNode right0 = (PartitionGroupNode)right;

            if (left.containsExact(right0.siblings()))
                // (X, :Y) and (X, :Y) -> (X, :Y)
                return left;
        }

        // Check if both sides are constants. If yes, then extract common partitions.
        if (left.constantsOnly()) {
            Set<PartitionSingleNode> consts = new HashSet<>(left.siblings());
            Set<PartitionSingleNode> rightConsts = null;

            if (right instanceof PartitionConstantSingleNode)
                rightConsts = Collections.singleton((PartitionSingleNode)right);
            else if (right instanceof PartitionGroupNode) {
                PartitionGroupNode right0 = (PartitionGroupNode)right;

                if (right0.constantsOnly())
                    rightConsts = right0.siblings();
            }

            if (rightConsts != null) {
                // {A, B) and (B, C) -> (B).
                consts.retainAll(rightConsts);

                if (consts.size() == 0)
                    // {A, B) and (C, D) -> NONE.
                    return PartitionNoneNode.INSTANCE;
                else if (consts.size() == 1)
                    // {A, B) and (B, C) -> (B).
                    return consts.iterator().next();
                else
                    // {A, B, C) and (B, C, D) -> (B, C).
                    return new PartitionGroupNode(consts);
            }
        }

        // Otherwise it is a mixed set of concrete partitions and arguments. Cancel optimization.
        // Note that in fact we can optimize expression to certain extent (e.g. (A) and (B, :C) -> (A) and (:C)),
        // but resulting expression is always composite node still, which cannot be optimized on upper levels.
        // So we skip any fine-grained optimization in favor of simplicity.
        return this;
    }

    /**
     * Optimize disjunction between group node and group or single node.
     *
     * @param left Left node.
     * @param right Right node.
     * @return Optimized node.
     */
    private PartitionNode optimizeGroupOr(PartitionGroupNode left, PartitionNode right) {
        HashSet<PartitionSingleNode> siblings = new HashSet<>(left.siblings());

        if (right instanceof PartitionSingleNode)
            siblings.add((PartitionSingleNode)right);
        else {
            assert right instanceof PartitionGroupNode;

            siblings.addAll(((PartitionGroupNode)right).siblings());
        }

        return new PartitionGroupNode(siblings);
    }

    /**
     * Optimize simple nodes.
     *
     * @param left Left node.
     * @param right Right node.
     * @return Optimized node.
     */
    private PartitionNode optimizeSimple(PartitionSingleNode left, PartitionSingleNode right) {
        if (op == PartitionCompositeNodeOperator.AND)
            return optimizeSimpleAnd(left, right);
        else {
            assert op == PartitionCompositeNodeOperator.OR;

            return optimizeSimpleOr(left, right);
        }
    }

    /**
     * Optimize two simple conjunctive nodes.
     *
     * @param left Left node.
     * @param right Right node.
     * @return Optimized node.
     */
    private PartitionNode optimizeSimpleAnd(PartitionSingleNode left, PartitionSingleNode right) {
        // Check if both sides are equal.
        if (left.equals(right))
            // (X) and (X) -> X
            // (:X) and (:X) -> "X
            return left;

        // If both sides are constants, and they are not equal, this is empty set.
        if (left.constant() && right.constant())
            // X and Y -> NONE
            return PartitionNoneNode.INSTANCE;

        // Otherwise it is a mixed set, cannot reduce.
        // X and :Y -> (X) AND (:Y)
        return this;
    }

    /**
     * Optimize two simple disjunctive nodes.
     *
     * @param left Left node.
     * @param right Right node.
     * @return Optimized node.
     */
    private PartitionNode optimizeSimpleOr(PartitionSingleNode left, PartitionSingleNode right) {
        if (left.equals(right))
            return left;
        else
            return PartitionGroupNode.merge(left, right);
    }
}
