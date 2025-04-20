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

package org.apache.ignite.internal.processors.query.calcite.metadata;

import org.apache.calcite.rel.core.Intersect;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Minus;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMdRowCount;
import org.apache.calcite.rel.metadata.RelMdUtil;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.Util;
import org.apache.ignite.internal.processors.query.calcite.exec.rel.NestedLoopJoinNode;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteAggregate;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteLimit;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteMergeJoin;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteNestedLoopJoin;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteSortedIndexSpool;
import org.apache.ignite.internal.util.typedef.F;
import org.jetbrains.annotations.Nullable;

import static org.apache.calcite.util.NumberUtil.multiply;

/** */
@SuppressWarnings("unused") // actually all methods are used by runtime generated classes
public class IgniteMdRowCount extends RelMdRowCount {
    /** */
    public static final RelMetadataProvider SOURCE =
        ReflectiveRelMetadataProvider.reflectiveSource(
            BuiltInMethod.ROW_COUNT.method, new IgniteMdRowCount());

    /** {@inheritDoc} */
    @Override public Double getRowCount(Join rel, RelMetadataQuery mq) {
        return joinRowCount(mq, rel);
    }

    /** {@inheritDoc} */
    @Override public Double getRowCount(Sort rel, RelMetadataQuery mq) {
        return rel.estimateRowCount(mq);
    }

//    @Override
//    public @org.checkerframework.checker.nullness.qual.Nullable Double getRowCount(Exchange rel, RelMetadataQuery mq) {
//        return rel.estimateRowCount(mq);
//    }
//
//    public static void printCosts(RelNode rel, RelMetadataQuery mq){
//        new RelShuttleImpl(){
//            @Override public RelNode visit(RelNode other) {
//                Commons.context(rel).logger().error("Rel " + other + ", rows: " + other.estimateRowCount(mq));
//
//                return super.visit(other);
//            }
//        }.visit(rel);
//    }

    /** */
    @Nullable public static Double joinRowCount(RelMetadataQuery mq, Join rel) {
        if (!rel.getJoinType().projectsRight()) {
            // Create a RexNode representing the selectivity of the
            // semijoin filter and pass it to getSelectivity
            RexNode semiJoinSelectivity =
                RelMdUtil.makeSemiJoinSelectivityRexNode(mq, rel);

            return multiply(mq.getSelectivity(rel.getLeft(), semiJoinSelectivity),
                mq.getRowCount(rel.getLeft()));
        }

        // Row count estimates of 0 will be rounded up to 1.
        // So, use maxRowCount where the product is very small.
        final Double left = mq.getRowCount(rel.getLeft());
        final Double right = mq.getRowCount(rel.getRight());

        if (left == null || right == null)
            return null;

        if (left == 1 && right == 1)
            return 1d;

        if (left <= 1D || right <= 1D) {
            Double max = mq.getMaxRowCount(rel);

            if (max != null && max <= 1D)
                return max;
        }

        JoinInfo joinInfo = rel.analyzeCondition();

        ImmutableIntList leftKeys = joinInfo.leftKeys;
        ImmutableIntList rightKeys = joinInfo.rightKeys;

        double selectivity = mq.getSelectivity(rel, rel.getCondition());

        if (left <= 1D || right <= 1D || F.isEmpty(leftKeys) || F.isEmpty(rightKeys))
            return left * right * selectivity;

        double leftDistinct = Util.first(
            mq.getDistinctRowCount(rel.getLeft(), ImmutableBitSet.of(leftKeys), null), left);
        double rightDistinct = Util.first(
            mq.getDistinctRowCount(rel.getRight(), ImmutableBitSet.of(rightKeys), null), right);

        double leftCardinality = leftDistinct / left;
        double rightCardinality = rightDistinct / right;

        double rowsCnt = (Math.min(left, right) / (leftCardinality * rightCardinality)) * selectivity;

        JoinRelType type = rel.getJoinType();

        if (type == JoinRelType.LEFT)
            rowsCnt += left;
        else if (type == JoinRelType.RIGHT)
            rowsCnt += right;
        else if (type == JoinRelType.FULL)
            rowsCnt += left + right;

        return rowsCnt;
    }

    /**
     * RowCount of Spool equals to estimated row count of its child by default,
     * but IndexSpool has internal filter that could filter out some rows,
     * hence we need to estimate it differently.
     */
    public double getRowCount(IgniteSortedIndexSpool rel, RelMetadataQuery mq) {
        return rel.estimateRowCount(mq);
    }

    @Override
    public @org.checkerframework.checker.nullness.qual.Nullable Double getRowCount(TableScan rel, RelMetadataQuery mq) {
        Double res = super.getRowCount(rel, mq);

        return res;
    }

    /** {@inheritDoc} */
    @Override public Double getRowCount(Intersect rel, RelMetadataQuery mq) {
        return rel.estimateRowCount(mq);
    }

    /** {@inheritDoc} */
    @Override public Double getRowCount(Minus rel, RelMetadataQuery mq) {
        return rel.estimateRowCount(mq);
    }

    /**
     * Estimation of row count for Aggregate operator.
     */
    public double getRowCount(IgniteAggregate rel, RelMetadataQuery mq) {
        return rel.estimateRowCount(mq);
    }

    /**
     * Estimation of row count for Limit operator.
     */
    public double getRowCount(IgniteLimit rel, RelMetadataQuery mq) {
        return rel.estimateRowCount(mq);
    }
}
