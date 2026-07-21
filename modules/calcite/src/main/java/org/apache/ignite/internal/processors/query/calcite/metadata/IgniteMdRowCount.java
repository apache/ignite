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

import java.util.BitSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Intersect;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Minus;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.metadata.BuiltInMetadata;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelColumnOrigin;
import org.apache.calcite.rel.metadata.RelMdRowCount;
import org.apache.calcite.rel.metadata.RelMdUtil;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.Util;
import org.apache.calcite.util.mapping.IntPair;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteAggregate;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteLimit;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteSortedIndexSpool;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteTableModify;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteTable;
import org.apache.ignite.internal.util.GridLeanMap;
import org.apache.ignite.internal.util.typedef.F;
import org.jetbrains.annotations.Nullable;

import static org.apache.calcite.util.NumberUtil.multiply;


/** */
@SuppressWarnings("unused") // actually all methods are used by runtime generated classes
public class IgniteMdRowCount extends RelMdRowCount {
    /** */
    private static final double NON_EQUI_COEFF = 0.7;

    /** */
    public static final double EQUI_COEFF = 0.8;

    /** */
    public static final RelMetadataProvider SOURCE =
        ReflectiveRelMetadataProvider.reflectiveSource(new IgniteMdRowCount(), BuiltInMetadata.RowCount.Handler.class);

    /** {@inheritDoc} */
    @Override public Double getRowCount(Join rel, RelMetadataQuery mq) {
        return joinRowCount(mq, rel);
    }

    /** {@inheritDoc} */
    @Override public Double getRowCount(Sort rel, RelMetadataQuery mq) {
        return rel.estimateRowCount(mq);
    }

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

        JoinInfo joinInfo = rel.analyzeCondition();

        if (joinInfo.pairs().isEmpty()) {
            // Fall-back to calcite's implementation.
            return RelMdUtil.getJoinRowCount(mq, rel, rel.getCondition());
        }

        // Row count estimates of 0 will be rounded up to 1.
        // So, use maxRowCount where the product is very small.
        final Double leftRowCnt = mq.getRowCount(rel.getLeft());
        final Double rightRowCnt = mq.getRowCount(rel.getRight());

        if (leftRowCnt == null || rightRowCnt == null)
            return null;

        if (leftRowCnt <= 1D || rightRowCnt <= 1D) {
            Double max = mq.getMaxRowCount(rel);
            if (max != null && max <= 1D)
                return max;
        }

        Map<Integer, KeyColumnOrigin> columnsFromLeft = resolveOrigins(mq, rel.getLeft(), joinInfo.leftKeys);
        Map<Integer, KeyColumnOrigin> columnsFromRight = resolveOrigins(mq, rel.getRight(), joinInfo.rightKeys);

        if (columnsFromLeft.isEmpty() || columnsFromRight.isEmpty())
            return crudeEstimation(mq, joinInfo, rel, leftRowCnt, rightRowCnt);

        Map<TablesPair, JoinContext> joinCtxts = new HashMap<>();
        for (IntPair joinKeys : joinInfo.pairs()) {
            KeyColumnOrigin leftKey = columnsFromLeft.get(joinKeys.source);
            KeyColumnOrigin rightKey = columnsFromRight.get(joinKeys.target);

            if (leftKey == null || rightKey == null) {
                continue;
            }

            joinCtxts.computeIfAbsent(
                new TablesPair(
                    leftKey.origin.getOriginTable(),
                    rightKey.origin.getOriginTable()
                ),
                key -> {
                    IgniteTable leftTable = key.left.unwrap(IgniteTable.class);
                    IgniteTable rightTable = key.right.unwrap(IgniteTable.class);

                    assert leftTable != null && rightTable != null;

                    int leftPkSize = leftTable.distribution().getKeys().size();
                    int rightPkSize = rightTable.distribution().getKeys().size();

                    return new JoinContext(leftPkSize, rightPkSize);
                }
            ).countKeys(leftKey, rightKey);
        }

        if (joinCtxts.isEmpty()) {
            // Fall-back to calcite's implementation.
            return RelMdUtil.getJoinRowCount(mq, rel, rel.getCondition());
        }

        Iterator<JoinContext> it = joinCtxts.values().iterator();
        JoinContext joinCtx = it.next();
        while (it.hasNext()) {
            JoinContext nextCtx = it.next();
            if (nextCtx.joinType().strength > joinCtx.joinType().strength) {
                joinCtx = nextCtx;
            }

            if (joinCtx.joinType().strength == JoiningRelationType.PK_ON_PK.strength) {
                break;
            }
        }

        if (joinCtx.joinType() == JoiningRelationType.UNKNOWN) {
            // Fall-back to calcite's implementation.
            return RelMdUtil.getJoinRowCount(mq, rel, rel.getCondition());
        }

        double postFiltrationAdjustment = 1.0;

        switch (rel.getJoinType()) {
            case INNER:
            case SEMI:
                // Extra join keys as well as non-equi conditions serves as post-filtration,
                // therefore we need to adjust final result with a little factor.
                if (joinCtxts.size() != 1 || !joinInfo.isEqui())
                    postFiltrationAdjustment = NON_EQUI_COEFF;

                break;
            default:
                break;
        }

        double baseRowCnt = 0.0;
        Double percentageAdjustment = null;
        if (joinCtx.joinType() == JoiningRelationType.PK_ON_PK) {
            postFiltrationAdjustment = EQUI_COEFF;

            if (rel.getJoinType() == JoinRelType.INNER || rel.getJoinType() == JoinRelType.SEMI) {
                // Assume we have two fact tables SALES and RETURNS sharing the same primary key. Every item
                // can be sold, but only items which were sold can be returned back, therefore
                // size(SALES) > size(RETURNS). When joining SALES on RETURNS by primary key, the estimated
                // result size will be the same as the size of the smallest table (RETURNS in this case),
                // adjusted by the percentage of rows of the biggest table (SALES in this case; percentage
                // adjustment is required to account for predicates pushed down to the table, e.g. we are
                // interested in returns of items with certain category)
                if (leftRowCnt > rightRowCnt) {
                    baseRowCnt = rightRowCnt;
                    percentageAdjustment = mq.getPercentageOriginalRows(rel.getLeft());
                }
                else {
                    baseRowCnt = leftRowCnt;
                    percentageAdjustment = mq.getPercentageOriginalRows(rel.getRight());
                }
            }
            else if (rel.getJoinType() == JoinRelType.LEFT) {
                baseRowCnt = leftRowCnt;
            }
            else if (rel.getJoinType() == JoinRelType.RIGHT) {
                baseRowCnt = rightRowCnt;
            }
            else if (rel.getJoinType() == JoinRelType.FULL) {
                Double selectivity = mq.getSelectivity(rel, rel.getCondition());

                // Fall-back to calcite's implementation.
                if (selectivity == null) {
                    return RelMdUtil.getJoinRowCount(mq, rel, rel.getCondition());
                }

                baseRowCnt = rightRowCnt + leftRowCnt;
                percentageAdjustment = 1.0 - selectivity;
            }
        }
        else if (joinCtx.joinType() == JoiningRelationType.FK_ON_PK) {
            // For foreign key joins the base table is the one which is joined by non-primary key columns.
            if (rel.getJoinType() == JoinRelType.INNER || rel.getJoinType() == JoinRelType.SEMI) {
                baseRowCnt = leftRowCnt;
                percentageAdjustment = mq.getPercentageOriginalRows(rel.getRight());
            }
            else if (rel.getJoinType() == JoinRelType.LEFT || rel.getJoinType() == JoinRelType.RIGHT) {
                baseRowCnt = leftRowCnt;
            }
            else if (rel.getJoinType() == JoinRelType.FULL) {
                Double selectivity = mq.getSelectivity(rel, rel.getCondition());

                // Fall-back to calcite's implementation.
                if (selectivity == null) {
                    return RelMdUtil.getJoinRowCount(mq, rel, rel.getCondition());
                }

                baseRowCnt = rightRowCnt + leftRowCnt;
                percentageAdjustment = 1.0 - selectivity;
            }
        }
        else { // PK_ON_FK
            if (rel.getJoinType() == JoinRelType.INNER || rel.getJoinType() == JoinRelType.SEMI) {
                baseRowCnt = rightRowCnt;
                percentageAdjustment = mq.getPercentageOriginalRows(rel.getLeft());
            }
            else if (rel.getJoinType() == JoinRelType.RIGHT || rel.getJoinType() == JoinRelType.LEFT) {
                baseRowCnt = rightRowCnt;
            }
            else if (rel.getJoinType() == JoinRelType.FULL) {
                Double selectivity = mq.getSelectivity(rel, rel.getCondition());

                // Fall-back to calcite's implementation.
                if (selectivity == null) {
                    return RelMdUtil.getJoinRowCount(mq, rel, rel.getCondition());
                }

                baseRowCnt = rightRowCnt + leftRowCnt;
                percentageAdjustment = 1.0 - selectivity;
            }
        }

        if (percentageAdjustment == null) {
            // No info, let's be conservative.
            percentageAdjustment = 1.0;
        }

        return baseRowCnt * percentageAdjustment * postFiltrationAdjustment;
    }

    /** */
    private static Map<Integer, KeyColumnOrigin> resolveOrigins(RelMetadataQuery mq, RelNode joinShoulder, ImmutableIntList keys) {
        GridLeanMap<Integer, KeyColumnOrigin> origins = new GridLeanMap<>();

        for (int i : keys) {
            if (origins.containsKey(i)) {
                continue;
            }

            RelColumnOrigin origin = mq.getColumnOrigin(joinShoulder, i);
            if (origin == null) {
                continue;
            }

            IgniteTable table = origin.getOriginTable().unwrap(IgniteTable.class);
            if (table == null || !table.distribution().function().affinity())
                continue;

            // Keys can relate to affinity not pk, just assumption here.
            ImmutableIntList distKeys = table.distribution().getKeys();

            int idx = distKeys.indexOf(origin.getOriginColumnOrdinal());

            origins.put(i, new KeyColumnOrigin(origin, idx));
        }

        return origins;
    }

    /**
     * @param origin
     * @param positionInKey
     */
    private record KeyColumnOrigin(RelColumnOrigin origin, int positionInKey) { }

    /** This part of estimation is applicable for distributions different from hash, i.e. broadcast, single. */
    private static double crudeEstimation(RelMetadataQuery mq, JoinInfo joinInfo, Join rel, Double leftRowCnt, Double rightRowCnt) {
        ImmutableIntList leftKeys = joinInfo.leftKeys;
        ImmutableIntList rightKeys = joinInfo.rightKeys;

        double selectivity = mq.getSelectivity(rel, rel.getCondition());

        if (F.isEmpty(leftKeys) || F.isEmpty(rightKeys))
            return leftRowCnt * rightRowCnt * selectivity;

        double leftDistinct = Util.first(
            mq.getDistinctRowCount(rel.getLeft(), ImmutableBitSet.of(leftKeys), null), leftRowCnt);
        double rightDistinct = Util.first(
            mq.getDistinctRowCount(rel.getRight(), ImmutableBitSet.of(rightKeys), null), rightRowCnt);

        double leftCardinality = leftDistinct / leftRowCnt;
        double rightCardinality = rightDistinct / rightRowCnt;

        double rowsCnt = (Math.min(leftRowCnt, rightRowCnt) / (leftCardinality * rightCardinality)) * selectivity;

        JoinRelType type = rel.getJoinType();

        if (type == JoinRelType.LEFT)
            rowsCnt += leftRowCnt;
        else if (type == JoinRelType.RIGHT)
            rowsCnt += rightRowCnt;
        else if (type == JoinRelType.FULL)
            rowsCnt += leftRowCnt + rightRowCnt;

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

    /**
     * Estimation of row count for Table modify operator.
     */
    public double getRowCount(IgniteTableModify rel, RelMetadataQuery mq) {
        return rel.estimateRowCount(mq);
    }

    /** */
    private record TablesPair(RelOptTable left, RelOptTable right) { }

    /** */
    private static class JoinContext {
        /** Used columns of primary key of table from left side. */
        private final BitSet leftKeys;

        /** Used columns of primary key of table from right side. */
        private final BitSet rightKeys;

        /**
         * Used columns of primary key in both tables.
         *
         * <p>This bitset is initialized when PK of both tables has equal columns count, and
         * bits are cleared when join pair contains columns with equal positions in PK of corresponding
         * table. For example, having tables T1 and T2 with primary keys in both tables defined as
         * CONSTRAINT PRIMARY KEY (a, b), in case of query
         * {@code SELECT ... FROM t1 JOIN t2 ON t1.a = t2.a AND t1.b = t2.b} commonKeys will be initialized
         * and cleared, but in case of query {@code SELECT ... FROM t1 JOIN t2 ON t1.a = t2.b AND t1.b = t2.a}
         * (mind the join condition, where column A of one table compared with column B of another), will be
         * only initialized (since size of the primary keys are equal), but not cleared.
         */
        private final @Nullable BitSet commonKeys;

        /** */
        JoinContext(int leftPkSize, int rightPkSize) {
            leftKeys = new BitSet();
            rightKeys = new BitSet();
            commonKeys = leftPkSize == rightPkSize && leftPkSize != 0 ? new BitSet() : null;

            leftKeys.set(0, leftPkSize);
            rightKeys.set(0, rightPkSize);

            if (commonKeys != null) {
                assert leftPkSize == rightPkSize;

                commonKeys.set(0, leftPkSize);
            }
        }

        /** */
        void countKeys(KeyColumnOrigin left, KeyColumnOrigin right) {
            if (left.positionInKey >= 0) {
                leftKeys.clear(left.positionInKey);
            }

            if (right.positionInKey >= 0) {
                rightKeys.clear(right.positionInKey);
            }

            if (commonKeys != null && left.positionInKey == right.positionInKey && left.positionInKey >= 0) {
                commonKeys.clear(left.positionInKey);
            }
        }

        /** */
        JoiningRelationType joinType() {
            if (commonKeys != null && commonKeys.isEmpty()) {
                return JoiningRelationType.PK_ON_PK;
            }

            if (rightKeys.isEmpty()) {
                return JoiningRelationType.FK_ON_PK;
            }

            if (leftKeys.isEmpty()) {
                return JoiningRelationType.PK_ON_FK;
            }

            return JoiningRelationType.UNKNOWN;
        }
    }

    /** Enumeration of join types by their semantic. */
    private enum JoiningRelationType {
        /**
         * Join by non-primary key columns.
         *
         * <p>Semantic is unknown.
         */
        UNKNOWN(0),

        /**
         * Join by primary keys on non-primary keys.
         *
         * <p>Currently we don't support Foreign Keys, thus we will assume such types of joins
         * as joins by foreign key.
         */
        PK_ON_FK(UNKNOWN.strength + 1),

        /**
         * Join by non-primary keys on primary keys.
         *
         * <p>Currently we don't support Foreign Keys, thus we will assume such types of joins
         * as joins by foreign key.
         */
        FK_ON_PK(PK_ON_FK.strength + 1),

        /**
         * Join of two tables which sharing the same primary key.
         *
         * <p>For example, join of tables CATALOG_SALES and CATALOG_RETURN from TPC-DS suite: both tables
         * have the same primary key (ITEM_ID, ORDER_ID).
         */
        PK_ON_PK(FK_ON_PK.strength + 1);

        /** The higher, the better. */
        private final int strength;

        /** */
        JoiningRelationType(int strength) {
            this.strength = strength;
        }
    }
}
