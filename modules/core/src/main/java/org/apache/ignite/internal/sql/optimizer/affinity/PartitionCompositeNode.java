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

package org.apache.ignite.internal.sql.optimizer.affinity;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Composite node which consists of two child nodes and a relation between them.
 */
public class PartitionCompositeNode implements PartitionNode {
    /** Left node. */
    @GridToStringInclude
    private final PartitionNode left;

    /** Right node. */
    @GridToStringInclude
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
    @Override public Collection<Integer> apply(PartitionClientContext cliCtx, Object... args)
        throws IgniteCheckedException {
        Collection<Integer> leftParts = left.apply(cliCtx, args);
        Collection<Integer> rightParts = right.apply(cliCtx, args);

        // Failed to resolve partitions on both sides, return.
        if (leftParts == null && rightParts == null)
            return null;

        if (op == PartitionCompositeNodeOperator.AND) {
            // (ALL) and (...) -> (...)
            if (leftParts == null)
                return rightParts;
            else if (rightParts == null)
                return leftParts;

            // (A, B) and (B, C) -> (B)
            leftParts = new HashSet<>(leftParts);

            leftParts.retainAll(rightParts);
        }
        else {
            assert op == PartitionCompositeNodeOperator.OR;

            // (ALL) or (...) -> (ALL)
            if (leftParts == null || rightParts == null)
                return null;

            // (A, B) or (B, C) -> (A, B, C)
            leftParts = new HashSet<>(leftParts);

            leftParts.addAll(rightParts);
        }

        return leftParts;
    }

    /** {@inheritDoc} */
    @Override public int joinGroup() {
        // Similar to group node, we cannot cache join group value here as it may be changed dynamically.
        return left.joinGroup();
    }

    /** {@inheritDoc} */
    @Override public PartitionNode optimize() {
        PartitionNode left = this.left;
        PartitionNode right = this.right;

        // Optimize composite nodes if possible.
        if (left instanceof PartitionCompositeNode)
            left = left.optimize();

        if (right instanceof PartitionCompositeNode)
            right = right.optimize();

        // ALL and NONE always can be optimized.
        if (left == PartitionAllNode.INSTANCE || left == PartitionNoneNode.INSTANCE)
            return optimizeSpecial(left, right);

        if (right == PartitionAllNode.INSTANCE || right == PartitionNoneNode.INSTANCE)
            return optimizeSpecial(right, left);

        // If one of child nodes cannot be optimized, nothing can be done further.
        // Note that we cannot return "this" here because left or right parts might have been changed.
        if (left instanceof PartitionCompositeNode || right instanceof PartitionCompositeNode) {
            // Should be "NONE" for AND in fact, but this would violate current non-collocated join semantics as
            // explained in "optimizeSimpleAnd" method below.
            if (left.joinGroup() != right.joinGroup())
                return PartitionAllNode.INSTANCE;

            return new PartitionCompositeNode(left, right, op);
        }

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
        assert op == PartitionCompositeNodeOperator.AND;

        // Should be "NONE" for AND in fact, but this would violate current non-collocated join semantics as
        // explained in "optimizeSimpleAnd" method below.
        if (left.joinGroup() != right.joinGroup())
            return PartitionAllNode.INSTANCE;

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

            if (right instanceof PartitionConstantNode)
                rightConsts = Collections.singleton((PartitionSingleNode)right);
            else if (right instanceof PartitionGroupNode) {
                PartitionGroupNode right0 = (PartitionGroupNode)right;

                if (right0.constantsOnly())
                    rightConsts = right0.siblings();
            }

            if (rightConsts != null) {
                // Try to merge nodes if they belong to the same table.
                boolean sameTbl = true;
                String curTblAlias = null;

                for (PartitionSingleNode curConst : consts) {
                    if (curTblAlias == null)
                        curTblAlias = curConst.table().alias();
                    else if (!F.eq(curTblAlias, curConst.table().alias())) {
                        sameTbl = false;

                        break;
                    }
                }

                if (sameTbl) {
                    for (PartitionSingleNode curConst : rightConsts) {
                        if (curTblAlias == null)
                            curTblAlias = curConst.table().alias();
                        else if (!F.eq(curTblAlias, curConst.table().alias())) {
                            sameTbl = false;

                            break;
                        }
                    }
                }

                if (sameTbl) {
                    // {A, B) and (B, C) -> (B).
                    consts.retainAll(rightConsts);

                    if (consts.isEmpty())
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
        }

        // Otherwise it is a mixed set of concrete partitions and arguments possibly from different caches.
        // Note that in fact we can optimize expression to certain extent (e.g. (A) and (B, :C) -> (A) and (:C)),
        // but resulting expression is always composite node still, which cannot be optimized on upper levels.
        // So we skip any fine-grained optimization in favor of simplicity.
        return new PartitionCompositeNode(left, right, PartitionCompositeNodeOperator.AND);
    }

    /**
     * Optimize disjunction between group node and group or single node.
     *
     * @param left Left node.
     * @param right Right node.
     * @return Optimized node.
     */
    private PartitionNode optimizeGroupOr(PartitionGroupNode left, PartitionNode right) {
        assert op == PartitionCompositeNodeOperator.OR;

        // Cannot merge disjunctive nodes if they belong to different join groups.
        if (left.joinGroup() != right.joinGroup())
            return PartitionAllNode.INSTANCE;

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
        assert op == PartitionCompositeNodeOperator.AND;

        // Currently we do not merge such nodes because it may violate existing broken (!!!) join semantics.
        // Normally, if we have two non-collocated partition sets, then this should be an empty set for collocated
        // query mode. Unfortunately, current semantics of collocated query mode assume that even though both sides
        // of expression are located on random nodes, there is a slight chance that they may accidentally reside on
        // a single node and hence return some rows. We return "ALL" here to keep this broken semantics consistent
        // irrespective of whether partition pruning is used or not. Once non-collocated joins are fixed, this
        // condition will be changed to "NONE".
        if (left.joinGroup() != right.joinGroup())
            return PartitionAllNode.INSTANCE;

        // Check if both sides are equal.
        if (left.equals(right))
            // (X) and (X) -> X
            // (:X) and (:X) -> :X
            return left;

        // If both sides are constants from the same table and they are not equal, this is empty set.
        if (left.constant() && right.constant() && F.eq(left.table().alias(), right.table().alias()))
            // X and Y -> NONE
            return PartitionNoneNode.INSTANCE;

        // Otherwise this is a mixed set, cannot reduce.
        // X and :Y -> (X) AND (:Y)
        return new PartitionCompositeNode(left, right, PartitionCompositeNodeOperator.AND);
    }

    /**
     * Optimize two simple disjunctive nodes.
     *
     * @param left Left node.
     * @param right Right node.
     * @return Optimized node.
     */
    private PartitionNode optimizeSimpleOr(PartitionSingleNode left, PartitionSingleNode right) {
        assert op == PartitionCompositeNodeOperator.OR;

        // Cannot merge disjunctive nodes if they belong to different join groups.
        if (left.joinGroup() != right.joinGroup())
            return PartitionAllNode.INSTANCE;

        // (A) or (A) -> (A)
        if (left.equals(right))
            return left;

        // (A) or (B) -> (A, B)
        HashSet<PartitionSingleNode> nodes = new HashSet<>();

        nodes.add(left);
        nodes.add(right);

        return new PartitionGroupNode(nodes);
    }

    /**
     * @return Left node.
     */
    public PartitionNode left() {
        return left;
    }

    /**
     * @return Right node.
     */
    public PartitionNode right() {
        return right;
    }

    /**
     * @return Operator.
     */
    public PartitionCompositeNodeOperator operator() {
        return op;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(PartitionCompositeNode.class, this);
    }

    /** {@inheritDoc} */
    @Override public String cacheName() {
        String leftCacheName = left.cacheName();

        return leftCacheName != null ? leftCacheName : right.cacheName();
    }
}
