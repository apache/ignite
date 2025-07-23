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

import java.util.List;
import java.util.Map;
import java.util.Set;
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
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.mapping.IntPair;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteAggregate;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteLimit;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteSortedIndexSpool;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteIndex;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteTable;
import org.apache.ignite.internal.processors.query.schema.management.SchemaManager;
import org.apache.ignite.internal.util.collection.IntMap;
import org.apache.ignite.internal.util.collection.IntRWHashMap;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

/** */
@SuppressWarnings("unused") // actually all methods are used by runtime generated classes
public class IgniteMdRowCount extends RelMdRowCount {
    /** */
    public static final RelMetadataProvider SOURCE =
        ReflectiveRelMetadataProvider.reflectiveSource(
            BuiltInMethod.ROW_COUNT.method, new IgniteMdRowCount());

    /** */
    private static final String PK_PROXY_NAME = SchemaManager.generateProxyIdxName(QueryUtils.PRIMARY_KEY_INDEX);

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
     * join keys, and the cardinality of the left and right inputs.
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
     *   (e.g., primary key key associations) are identified.</li>
     *   <li>If no valid keys association is found, the method falls back to Calcite's implementation.</li>
     *   <li>The base row count is determined by the type of join relationship:
     *       <ul>
     *           <li>For primary key-to-primary key joins, the row count is based on the smaller table,
     *           adjusted by a percentage of the larger table's rows.</li>
     *           <li>For just one table's primary key joins, the base table is determined based on which table is
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
     * @param join The {@link Join} relational expression representing the join operation.
     * @return The estimated number of rows resulting from the join, or {@code null} if the estimation cannot be determined.
     *
     * @see RelMetadataQuery#getRowCount
     * @see RelMdUtil#getJoinRowCount
     */
    public static @Nullable Double joinRowCount(RelMetadataQuery mq, Join join) {
        if (join.getJoinType() != JoinRelType.INNER)
            return RelMdUtil.getJoinRowCount(mq, join, join.getCondition());

        JoinInfo joinInfo = join.analyzeCondition();

        if (joinInfo.pairs().isEmpty())
            return RelMdUtil.getJoinRowCount(mq, join, join.getCondition());

        Double leftRowCnt = mq.getRowCount(join.getLeft());

        if (leftRowCnt == null)
            return null;

        Double rightRowCnt = mq.getRowCount(join.getRight());

        if (rightRowCnt == null)
            return null;

        // Zero row count is considered as 1. If product is very small, we use maximal row count.
        if (leftRowCnt <= 1.0 || rightRowCnt <= 1.0) {
            Double max = mq.getMaxRowCount(join);

            if (max != null && max <= 1.0)
                return max;
        }

        IntMap<RelColumnOrigin> leftOrigins = colOrigins(mq, join.getLeft(), joinInfo.leftKeys);
        IntMap<RelColumnOrigin> rightOrigins = colOrigins(mq, join.getRight(), joinInfo.rightKeys);

        /** Check {@link IgniteMdColumnOrigins} and/or {@link RelMdColumnOrigins} if no origin is found. */
        if (leftOrigins.isEmpty() || rightOrigins.isEmpty())
            return RelMdUtil.getJoinRowCount(mq, join, join.getCondition());

        List<IntPair> joinPairs = joinInfo.pairs();

        Set<IntPair> uniquePairs = U.newHashSet(joinPairs.size());

        int leftPosInPk = -1;
        int rightPosInPk = -1;

        for (IntPair p : joinInfo.pairs()) {
            uniquePairs.add(p);

            // Pproper usage of a PK on both hands is already found. Just store the unique pair.
            if (leftPosInPk == 0 && rightPosInPk == 0)
                continue;

            RelColumnOrigin leftOrigin = leftOrigins.get(p.source);
            RelColumnOrigin rightOrigin = rightOrigins.get(p.target);

            if (leftOrigin == null || rightOrigin == null)
                continue;

            IgniteTable leftTbl = leftOrigin.getOriginTable().unwrap(IgniteTable.class);
            int curLeftPkPos = pkColumns(leftTbl).indexOf(leftOrigin.getOriginColumnOrdinal());

            IgniteTable rightTbl = rightOrigin.getOriginTable().unwrap(IgniteTable.class);
            int curRightPkPos = pkColumns(rightTbl).indexOf(rightOrigin.getOriginColumnOrdinal());

            // No proper index usage found on any hand.
            if (curLeftPkPos != 0 && curRightPkPos != 0)
                continue;

            // Pproper usage of a PK on both hands is found. The best case.
            if (curLeftPkPos == 0 && curRightPkPos == 0) {
                leftPosInPk = rightPosInPk = 0;

                continue;
            }

            // Found first index usage on one of the hands. Or a bit 'stronger' (PK on the right) condition is found.
            if ((leftPosInPk < 0 && rightPosInPk < 0) || (leftPosInPk == 0 && curRightPkPos == 0)) {
                leftPosInPk = curLeftPkPos;
                rightPosInPk = curRightPkPos;
            }
        }

        // No index proper usages found.
        if (leftPosInPk != 0 && rightPosInPk != 0)
            return RelMdUtil.getJoinRowCount(mq, join, join.getCondition());

        double rowCnt;
        Double percentage;

        // PKs on both hands are in use.
        if (leftPosInPk == 0 && rightPosInPk == 0) {
            // Consider some fact tables SALES and RETURNS refer to the same primary key. Sold items can be returned.
            // So, size(SALES) > size(RETURNS). If joining SALES and RETURNS by primary key, result size is equal to
            // the smallest table (RETURNS) adjusted by the percentage of rows of the biggest table (SALES). The percentage
            // adjustment is required to account for predicates pushed down to the table. As an instance, we are
            // interested in returns of items of certain category.
            if (leftRowCnt > rightRowCnt) {
                rowCnt = rightRowCnt;

                percentage = mq.getPercentageOriginalRows(join.getLeft());
            }
            else {
                rowCnt = leftRowCnt;

                percentage = mq.getPercentageOriginalRows(join.getRight());
            }
        }
        else if (leftPosInPk != 0 && rightPosInPk == 0) { // PK only on the right used.

            rowCnt = leftRowCnt;

            percentage = mq.getPercentageOriginalRows(join.getRight());
        }
        else { // PK only on the left used.
            assert leftPosInPk == 0 && rightPosInPk != 0;

            rowCnt = rightRowCnt;

            percentage = mq.getPercentageOriginalRows(join.getLeft());
        }

        // Got no info.
        if (percentage == null)
            percentage = 1.0;

        // Additional join keys and non-equi conditions work as post-filtration. We should adjust the result.
        double adjustment = uniquePairs.size() == 1 && joinInfo.isEqui() ? 1.0 : 0.7f;

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
    private static IntMap<RelColumnOrigin> colOrigins(RelMetadataQuery mq, RelNode input, ImmutableIntList keys) {
        IntMap<RelColumnOrigin> res = new IntRWHashMap<>();

        for (int joinKey : keys) {
            if (res.get(joinKey) != null)
                continue;

            RelColumnOrigin colOrigin = mq.getColumnOrigin(input, joinKey);

            if (colOrigin == null)
                continue;

            IgniteTable table = colOrigin.getOriginTable().unwrap(IgniteTable.class);

            if (table == null)
                continue;

            res.put(joinKey, colOrigin);
        }

        return res;
    }

    /** Returns column numbers of the table's primary key. */
    private static ImmutableIntList pkColumns(IgniteTable table) {
        Map<String, IgniteIndex> indexes = table.indexes();

        if (F.isEmpty(indexes))
            return ImmutableIntList.of();

        IgniteIndex idx = indexes.get(PK_PROXY_NAME);

        if (idx == null)
            idx = indexes.get(QueryUtils.PRIMARY_KEY_INDEX);

        return idx == null ? ImmutableIntList.of() : idx.collation().getKeys();
    }
}
