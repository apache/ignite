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
import java.util.Objects;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Intersect;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Minus;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelColumnOrigin;
import org.apache.calcite.rel.metadata.RelMdRowCount;
import org.apache.calcite.rel.metadata.RelMdUtil;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.mapping.IntPair;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteAggregate;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteLimit;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteSortedIndexSpool;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteIndex;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteTable;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.util.collection.IntMap;
import org.apache.ignite.internal.util.collection.IntRWHashMap;
import org.apache.ignite.internal.util.typedef.F;
import org.jetbrains.annotations.Nullable;

/** */
@SuppressWarnings("unused") // actually all methods are used by runtime generated classes
public class IgniteMdRowCount extends RelMdRowCount {
    /** */
    public static final RelMetadataProvider SOURCE =
        ReflectiveRelMetadataProvider.reflectiveSource(
            BuiltInMethod.ROW_COUNT.method, new IgniteMdRowCount());

    /** {@inheritDoc} */
    @Override public Double getRowCount(Join rel, RelMetadataQuery mq) {
        return rel.estimateRowCount(mq);
    }

    /** {@inheritDoc} */
    @Override public Double getRowCount(Sort rel, RelMetadataQuery mq) {
        return rel.estimateRowCount(mq);
    }

    /**
     * Estimates the number of rows produced by a join operation.
     *
     * <p>This method calculates an estimated row count for a join by analyzing the join type,
     * join keys, and the cardinality of the left and right inputs. It provides specialized
     * handling for primary key and foreign key relationships (see {@link JoiningRelationType for details}).
     * When certain metadata is unavailable or when specific conditions are not met, it falls back to
     * Calcite's default implementation for estimating the row count.
     *
     * <p>Implementation details:</p>
     * <ul>
     *   <li>If the join type is not {@link JoinRelType#INNER}, Calcite's default implementation is used.</li>
     *   <li>If the join is non-equi join, Calcite's default implementation is used.</li>
     *   <li>The row counts of the left and right inputs are retrieved using
     *   {@link RelMetadataQuery#getRowCount}. If either value is unavailable, the result is {@code null}.</li>
     *   <li>If the row counts are very small (≤ 1.0), the method uses the maximum row count as a fallback.</li>
     *   <li>Join key origins are resolved for the left and right inputs, and relationships between tables
     *   (e.g., primary key or foreign key associations) are identified and grouped into join contexts.</li>
     *   <li>If no valid join context is found, the method falls back to Calcite's implementation.</li>
     *   <li>The base row count is determined by the type of join relationship:
     *       <ul>
     *           <li>For primary key-to-primary key joins, the row count is based on the smaller table,
     *           adjusted by a percentage of the larger table's rows.</li>
     *           <li>For foreign key joins, the base table is determined based on which table is
     *           joined using non-primary key columns.</li>
     *       </ul>
     *   </li>
     *   <li>An additional adjustment factor is applied for post-filtration conditions, such as extra join keys
     *   or non-equi conditions.</li>
     *   <li>If metadata for the percentage of original rows is unavailable, the adjustment defaults to 1.0.</li>
     * </ul>
     *
     * <p>If none of the above criteria are satisfied, the method defaults to
     * {@link RelMdUtil#getJoinRowCount} for the estimation.</p>
     *
     * @param mq The {@link RelMetadataQuery} used to retrieve metadata about relational expressions.
     * @param rel The {@link Join} relational expression representing the join operation.
     * @return The estimated number of rows resulting from the join, or {@code null} if the estimation cannot be determined.
     *
     * @see RelMetadataQuery#getRowCount
     * @see RelMdUtil#getJoinRowCount
     * @see JoiningRelationType
     */
    public static @Nullable Double joinRowCount(RelMetadataQuery mq, Join rel) {
        if (rel.getJoinType() != JoinRelType.INNER)
            return RelMdUtil.getJoinRowCount(mq, rel, rel.getCondition());

        JoinInfo joinInfo = rel.analyzeCondition();

        if (joinInfo.pairs().isEmpty())
            return RelMdUtil.getJoinRowCount(mq, rel, rel.getCondition());

        Double leftRowCnt = mq.getRowCount(rel.getLeft());

        if (leftRowCnt == null)
            return null;

        Double rightRowCnt = mq.getRowCount(rel.getRight());

        if (rightRowCnt == null)
            return null;

        // Zero row count is considered as 1. If product is very small, we use maximal row count.
        if (leftRowCnt <= 1.0 || rightRowCnt <= 1.0) {
            Double max = mq.getMaxRowCount(rel);

            if (max != null && max <= 1.0)
                return max;
        }

        IntMap<KeyColumnOrigin> leftColumns = findOrigins(mq, rel.getLeft(), joinInfo.leftKeys);
        IntMap<KeyColumnOrigin> rightColumns = findOrigins(mq, rel.getRight(), joinInfo.rightKeys);

        /** Check {@link IgniteMdColumnOrigins} and/or {@link RelMdColumnOrigins} if no origin is found. */
        if (leftColumns.isEmpty() || rightColumns.isEmpty())
            return RelMdUtil.getJoinRowCount(mq, rel, rel.getCondition());

        Map<TablesPair, JoinCtx> ctxs = new HashMap<>();

        for (IntPair joinKeys : joinInfo.pairs()) {
            KeyColumnOrigin leftKey = leftColumns.get(joinKeys.source);
            KeyColumnOrigin rightKey = rightColumns.get(joinKeys.target);

            if (leftKey == null || rightKey == null)
                continue;

            ctxs.computeIfAbsent(
                new TablesPair(
                    leftKey.origin.getOriginTable(),
                    rightKey.origin.getOriginTable()
                ),
                key -> {
                    IgniteTable leftTbl = key.left.unwrap(IgniteTable.class);
                    IgniteTable rightTbl = key.right.unwrap(IgniteTable.class);

                    assert leftTbl != null && rightTbl != null;

                    return new JoinCtx(keyColumns(leftTbl).size(), keyColumns(rightTbl).size());
                }
            ).resolveKeys(leftKey, rightKey);
        }

        if (ctxs.isEmpty())
            return RelMdUtil.getJoinRowCount(mq, rel, rel.getCondition());

        Iterator<JoinCtx> it = ctxs.values().iterator();
        JoinCtx ctx = it.next();

        while (it.hasNext()) {
            JoinCtx nextCtx = it.next();

            if (nextCtx.joinType().strength > ctx.joinType().strength)
                ctx = nextCtx;

            if (ctx.joinType().strength == JoiningRelationType.PK_ON_PK.strength)
                break;
        }

        if (ctx.joinType() == JoiningRelationType.UNKNOWN)
            return RelMdUtil.getJoinRowCount(mq, rel, rel.getCondition());

        double rowCnt;
        Double percentage;

        if (ctx.joinType() == JoiningRelationType.PK_ON_PK) {
            // Consider some fact tables SALES and RETURNS refer to the same primary key. Sold items can be returned.
            // So, size(SALES) > size(RETURNS). If joining SALES and RETURNS by primary key, result size is equal to
            // the smallest table (RETURNS) adjusted by the percentage of rows of the biggest table (SALES). The percentage
            // adjustment is required to account for predicates pushed down to the table. As an instance, we are
            // interested in returns of items of certain category.
            if (leftRowCnt > rightRowCnt) {
                rowCnt = rightRowCnt;

                percentage = mq.getPercentageOriginalRows(rel.getLeft());
            }
            else {
                rowCnt = leftRowCnt;

                percentage = mq.getPercentageOriginalRows(rel.getRight());
            }
        }
        else {
            // For foreign key joins the base table is the one which is joined by non-primary key columns.
            if (ctx.joinType() == JoiningRelationType.FK_ON_PK) {
                rowCnt = leftRowCnt;

                percentage = mq.getPercentageOriginalRows(rel.getRight());
            }
            else {
                assert ctx.joinType() == JoiningRelationType.PK_ON_FK : ctx.joinType();

                rowCnt = rightRowCnt;

                percentage = mq.getPercentageOriginalRows(rel.getLeft());
            }
        }

        // Got no info.
        if (percentage == null)
            percentage = 1.0;

        // Additional join keys and non-equi conditions work as post-filtration. We should adjust the result.
        double adjustment = ctxs.size() == 1 && joinInfo.isEqui() ? 1.0 : 0.7;

        return rowCnt * percentage * adjustment;
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

    /** */
    private static IntMap<KeyColumnOrigin> findOrigins(RelMetadataQuery mq, RelNode joinInput, ImmutableIntList keys) {
        IntMap<KeyColumnOrigin> res = new IntRWHashMap<>();

        for (int keyColIdx : keys) {
            if (res.containsKey(keyColIdx))
                continue;

            RelColumnOrigin origin = mq.getColumnOrigin(joinInput, keyColIdx);

            if (origin == null)
                continue;

            IgniteTable table = origin.getOriginTable().unwrap(IgniteTable.class);

            if (table == null)
                continue;

            int srcKeyColIdx = origin.getOriginColumnOrdinal();

            RelDataType insertRowType = table.descriptor().insertRowType(Commons.typeFactory(joinInput));
            RelDataType curRowType = origin.getOriginTable().getRowType();

            assert curRowType.getFieldCount() >= insertRowType.getFieldCount();

            if (curRowType.getFieldCount() > insertRowType.getFieldCount()) {
                /** Current row type probably contains {@link QueryUtils#KEY_FIELD_NAME} and {@link QueryUtils#VAL_FIELD_NAME}. */
                srcKeyColIdx -= curRowType.getFieldCount() - insertRowType.getFieldCount();
            }

            int keyPos = keyColumns(table).indexOf(srcKeyColIdx);

            res.put(keyColIdx, new KeyColumnOrigin(origin, keyPos));
        }

        return res;
    }

    /** Returns column numbers of the primary index. */
    private static ImmutableIntList keyColumns(IgniteTable table) {
        Map<String, IgniteIndex> indexes = table.indexes();

        if (F.isEmpty(indexes))
            return ImmutableIntList.of();

        IgniteIndex idx = indexes.get(QueryUtils.PRIMARY_KEY_INDEX);

        return idx == null ? ImmutableIntList.of() : idx.collation().getKeys();
    }

    /** */
    private static class KeyColumnOrigin {
        /** */
        private final RelColumnOrigin origin;

        /** */
        private final int positionInKey;

        /** */
        private KeyColumnOrigin(RelColumnOrigin origin, int positionInKey) {
            this.origin = origin;
            this.positionInKey = positionInKey;
        }
    }

    /** */
    private static class TablesPair {
        /** */
        private final RelOptTable left;

        /** */
        private final RelOptTable right;

        /** */
        private TablesPair(RelOptTable left, RelOptTable right) {
            this.left = left;
            this.right = right;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            TablesPair that = (TablesPair)o;

            // Reference equality on purpose.
            return left == that.left && right == that.right;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return Objects.hash(left, right);
        }
    }

    /** Join context. */
    private static class JoinCtx {
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
        private JoinCtx(int leftPkSize, int rightPkSize) {
            this.leftKeys = new BitSet();
            this.rightKeys = new BitSet();
            this.commonKeys = leftPkSize == rightPkSize ? new BitSet() : null;

            leftKeys.set(0, leftPkSize);
            rightKeys.set(0, rightPkSize);

            if (commonKeys != null) {
                assert leftPkSize == rightPkSize;

                commonKeys.set(0, leftPkSize);
            }
        }

        /** */
        private void resolveKeys(KeyColumnOrigin left, KeyColumnOrigin right) {
            if (left.positionInKey >= 0)
                leftKeys.clear(left.positionInKey);

            if (right.positionInKey >= 0)
                rightKeys.clear(right.positionInKey);

            if (commonKeys != null && left.positionInKey == right.positionInKey && left.positionInKey >= 0)
                commonKeys.clear(left.positionInKey);
        }

        /** */
        private JoiningRelationType joinType() {
            if (commonKeys != null && commonKeys.isEmpty())
                return JoiningRelationType.PK_ON_PK;

            if (rightKeys.isEmpty())
                return JoiningRelationType.FK_ON_PK;

            if (leftKeys.isEmpty())
                return JoiningRelationType.PK_ON_FK;

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

        /** The higher the better. */
        private final int strength;

        /** */
        JoiningRelationType(int strength) {
            this.strength = strength;
        }
    }
}
