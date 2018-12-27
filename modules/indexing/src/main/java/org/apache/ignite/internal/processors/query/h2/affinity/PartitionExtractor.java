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

package org.apache.ignite.internal.processors.query.h2.affinity;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.internal.processors.cache.query.GridCacheSqlQuery;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlAlias;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlAst;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlColumn;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlConst;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlElement;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlOperation;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlOperationType;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlParameter;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlQuery;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlSelect;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlTable;
import org.apache.ignite.internal.util.typedef.F;
import org.h2.table.Column;
import org.h2.value.Value;
import org.h2.value.ValueLong;
import org.jetbrains.annotations.Nullable;

/**
 * Partition tree extractor.
 */
public class PartitionExtractor {
    /**
     * Maximum number of partitions to be used in case of between expression.
     * In case of excessing all partitions will be used.
     */
    private static final int MAX_PARTITIONS_COUNT_BETWEEN = 16;

    /** Indexing. */
    private final IgniteH2Indexing idx;

    /**
     * Constructor.
     *
     * @param idx Indexing.
     */
    public PartitionExtractor(IgniteH2Indexing idx) {
        this.idx = idx;
    }

    /**
     * Extract partitions.
     *
     * @param qry Query.
     * @return Partitions.
     */
    public PartitionResult extract(GridSqlQuery qry) throws IgniteCheckedException {
        // No unions support yet.
        if (!(qry instanceof GridSqlSelect))
            return null;

        GridSqlSelect select = (GridSqlSelect)qry;

        // Currently we can extract data only from a single table.
        GridSqlTable tbl = unwrapTable(select.from());

        if (tbl == null)
            return null;

        // Do extract.
        PartitionNode tree = extractFromExpression(select.where());

        assert tree != null;

        // Reduce tree if possible.
        tree = tree.optimize();

        if (tree instanceof PartitionAllNode)
            return null;

        // Return.
        PartitionTableDescriptor desc = descriptor(tbl.dataTable());

        return new PartitionResult(desc, tree);
    }

    /**
     * Merge partition info from multiple queries.
     *
     * @param qrys Queries.
     * @return Partition result or {@code null} if nothing is resolved.
     */
    @SuppressWarnings("IfMayBeConditional")
    public PartitionResult merge(List<GridCacheSqlQuery> qrys) {
        // Check if merge is possible.
        PartitionTableDescriptor desc = null;

        for (GridCacheSqlQuery qry : qrys) {
            PartitionResult qryRes = (PartitionResult)qry.derivedPartitions();

            if (qryRes == null)
                // Failed to get results for one query -> broadcast.
                return null;

            if (desc == null)
                desc = qryRes.descriptor();
            else if (!F.eq(desc, qryRes.descriptor()))
                // Queries refer to different tables, cannot merge -> broadcast.
                return null;
        }

        // Merge.
        PartitionNode tree = null;

        for (GridCacheSqlQuery qry : qrys) {
            PartitionResult qryRes = (PartitionResult)qry.derivedPartitions();

            if (tree == null)
                tree = qryRes.tree();
            else
                tree = new PartitionCompositeNode(tree, qryRes.tree(), PartitionCompositeNodeOperator.OR);
        }

        // Optimize.
        assert tree != null;

        tree = tree.optimize();

        if (tree instanceof PartitionAllNode)
            return null;

        return new PartitionResult(desc, tree);
    }

    /**
     * Try unwrapping the table.
     *
     * @param from From.
     * @return Table or {@code null} if not a table.
     */
    @Nullable private static GridSqlTable unwrapTable(GridSqlAst from) {
        if (from instanceof GridSqlAlias)
            from = from.child();

        if (from instanceof GridSqlTable)
            return (GridSqlTable)from;

        return null;
    }

    /**
     * Extract partitions from expression.
     *
     * @param expr Expression.
     * @return Partition tree.
     */
    @SuppressWarnings("EnumSwitchStatementWhichMissesCases")
    private PartitionNode extractFromExpression(GridSqlAst expr) throws IgniteCheckedException {
        PartitionNode res = PartitionAllNode.INSTANCE;

        if (expr instanceof GridSqlOperation) {
            GridSqlOperation op = (GridSqlOperation)expr;

            switch (op.operationType()) {
                case AND:
                    res = extractFromAnd(op);

                    break;

                case OR:
                    res = extractFromOr(op);

                    break;

                case IN:
                    res = extractFromIn(op);

                    break;

                case EQUAL:
                    res = extractFromEqual(op);
            }
        }

        // Cannot determine partition.
        return res;
    }

    /**
     * Extract partition information from AND.
     *
     * @param op Operation.
     * @return Partition.
     */
    private PartitionNode extractFromAnd(GridSqlOperation op) throws IgniteCheckedException {
        assert op.size() == 2;

        PartitionNode  betweenNodes = tryExtractBetween(op);

        if (betweenNodes != null)
            return betweenNodes;

        PartitionNode part1 = extractFromExpression(op.child(0));
        PartitionNode part2 = extractFromExpression(op.child(1));

        return new PartitionCompositeNode(part1, part2, PartitionCompositeNodeOperator.AND);
    }

    /**
     * Extract partition information from OR.
     *
     * @param op Operation.
     * @return Partition.
     */
    private PartitionNode extractFromOr(GridSqlOperation op) throws IgniteCheckedException {
        assert op.size() == 2;

        PartitionNode part1 = extractFromExpression(op.child(0));
        PartitionNode part2 = extractFromExpression(op.child(1));

        return new PartitionCompositeNode(part1, part2, PartitionCompositeNodeOperator.OR);
    }

    /**
     * Extract partition information from IN.
     *
     * @param op Operation.
     * @return Partition.
     */
    private PartitionNode extractFromIn(GridSqlOperation op) throws IgniteCheckedException {
        // Operation should contain at least two children: left (column) and right (const or column).
        if (op.size() < 2)
            return PartitionAllNode.INSTANCE;

        // Left operand should be column.
        GridSqlAst left = op.child();

        GridSqlColumn leftCol;

        if (left instanceof GridSqlColumn)
            leftCol = (GridSqlColumn)left;
        else
            return PartitionAllNode.INSTANCE;

        // Can work only with Ignite tables.
        if (!(leftCol.column().getTable() instanceof GridH2Table))
            return PartitionAllNode.INSTANCE;

        Set<PartitionSingleNode> parts = new HashSet<>();

        for (int i = 1; i < op.size(); i++) {
            GridSqlAst right = op.child(i);

            GridSqlConst rightConst;
            GridSqlParameter rightParam;

            if (right instanceof GridSqlConst) {
                rightConst = (GridSqlConst)right;
                rightParam = null;
            }
            else if (right instanceof GridSqlParameter) {
                rightConst = null;
                rightParam = (GridSqlParameter)right;
            }
            else
                // One of members of "IN" list is neither const, nor param, so we do no know it's partition.
                // As this is disjunction, not knowing partition of a single element leads to unknown partition
                // set globally. Hence, returning null.
                return PartitionAllNode.INSTANCE;

            // Do extract.
            PartitionSingleNode part = extractSingle(leftCol.column(), rightConst, rightParam);

            // Same thing as above: single unknown partition in disjunction defeats optimization.
            if (part == null)
                return PartitionAllNode.INSTANCE;

            parts.add(part);
        }

        return parts.size() == 1 ? parts.iterator().next() : new PartitionGroupNode(parts);
    }

    /**
     * Extract partition information from equality.
     *
     * @param op Operation.
     * @return Partition.
     */
    private PartitionNode extractFromEqual(GridSqlOperation op) throws IgniteCheckedException {
        assert op.operationType() == GridSqlOperationType.EQUAL;

        GridSqlElement left = op.child(0);
        GridSqlElement right = op.child(1);

        GridSqlColumn leftCol;

        if (left instanceof GridSqlColumn)
            leftCol = (GridSqlColumn)left;
        else
            return PartitionAllNode.INSTANCE;

        if (!(leftCol.column().getTable() instanceof GridH2Table))
            return PartitionAllNode.INSTANCE;

        GridSqlConst rightConst;
        GridSqlParameter rightParam;

        if (right instanceof GridSqlConst) {
            rightConst = (GridSqlConst)right;
            rightParam = null;
        }
        else if (right instanceof GridSqlParameter) {
            rightConst = null;
            rightParam = (GridSqlParameter)right;
        }
        else
            return PartitionAllNode.INSTANCE;

        PartitionSingleNode part = extractSingle(leftCol.column(), rightConst, rightParam);

        return part != null ? part : PartitionAllNode.INSTANCE;
    }

    /**
     * Extract single partition.
     *
     * @param leftCol Left column.
     * @param rightConst Right constant.
     * @param rightParam Right parameter.
     * @return Partition or {@code null} if failed to extract.
     */
    @Nullable private PartitionSingleNode extractSingle(Column leftCol, GridSqlConst rightConst,
        GridSqlParameter rightParam) throws IgniteCheckedException {
        assert leftCol != null;
        assert leftCol.getTable() != null;
        assert leftCol.getTable() instanceof GridH2Table;

        GridH2Table tbl = (GridH2Table)leftCol.getTable();

        if (!tbl.isColumnForPartitionPruning(leftCol))
            return null;

        PartitionTableDescriptor tblDesc = descriptor(tbl);

        if (rightConst != null) {
            int part = idx.kernalContext().affinity().partition(tbl.cacheName(), rightConst.value().getObject());

            return new PartitionConstantNode(tblDesc, part);
        }
        else if (rightParam != null)
            return new PartitionParameterNode(tblDesc, idx, rightParam.index(), leftCol.getType());
        else
            return null;
    }

    /**
     * Get descriptor from table.
     *
     * @param tbl Table.
     * @return Descriptor.
     */
    private static PartitionTableDescriptor descriptor(GridH2Table tbl) {
        return new PartitionTableDescriptor(tbl.cacheName(), tbl.getName());
    }

    // TODO: 27.12.18 comment
    PartitionNode tryExtractBetween(GridSqlOperation op) throws IgniteCheckedException {
        // Between operation (or similar range) should contain exact two children.

        // TODO: 26.12.18 ensure that it's actually between or same range
        if (op.size() != 2)
            return null;

        if (op.child().size() != 2)
            return null;

        if (op.child(1).size() != 2)
            return null;

        if (op.child().child().size() != 0 || op.child(1).child().size() != 0)
            return null;

        // Try parse left AST.
        GridSqlAst left = op.child();
        GridSqlOperation leftOp;
        GridSqlOperationType leftOpType;
        GridSqlColumn leftCol;

        if (left instanceof GridSqlOperation) {
            leftOp = (GridSqlOperation)left;
            leftOpType = leftOp.operationType();

            if (leftOp.child() instanceof GridSqlColumn) {
                leftCol = leftOp.child();

                if (!(leftCol.column().getTable() instanceof GridH2Table))
                    return null;
            }
            else
                return null;
        }
        else
            return null;

        // Try parse right AST.
        GridSqlAst right = op.child(1);
        GridSqlOperation rightOp;
        GridSqlOperationType rightOpType;
        GridSqlColumn rightCol;

        if (right instanceof GridSqlOperation) {
            rightOp = (GridSqlOperation)right;
            rightOpType = rightOp.operationType();

            if (rightOp.child() instanceof GridSqlColumn) {
                rightCol = rightOp.child();

                if (!(rightCol.column().getTable() instanceof GridH2Table))
                    return null;
            }
            else
                return null;
        }
        else
            return null;

        // Check that both left and right AST use same column.
        if (!leftCol.equals(rightCol))
            return null;

        // Check that both columns might be used for partition prunning.
        if (!((GridH2Table)leftCol.column().getTable()).isColumnForPartitionPruning(leftCol.column()))
            return null;

        // Check that left AST operation is '>' or '>=' and right AST operation is '<' or '<='.
        if (!((leftOpType == GridSqlOperationType.BIGGER || leftOpType == GridSqlOperationType.BIGGER_EQUAL)
            && (rightOpType == GridSqlOperationType.SMALLER || rightOpType == GridSqlOperationType.SMALLER_EQUAL)))
            return null;

        // Try parse left AST right value (value to the right of '>' or '>=').
        GridSqlAst leftVal = leftOp.child(1);

        GridSqlConst leftConst;
        GridSqlParameter leftParam;

        if (leftVal instanceof GridSqlConst) {
            leftConst = (GridSqlConst)leftVal;
            leftParam = null;
        }
        else if (leftVal instanceof GridSqlParameter) {
            leftConst = null;
            leftParam = (GridSqlParameter)leftVal;
        }
        else
            return null;

        // Try parse right AST right value (value to the right of '<' or '<=').
        GridSqlAst rightVal = rightOp.child(1);

        GridSqlConst rightConst;
        GridSqlParameter rightParam;

        if (rightVal instanceof GridSqlConst) {
            rightConst = (GridSqlConst)rightVal;
            rightParam = null;
        }
        else if (rightVal instanceof GridSqlParameter) {
            rightConst = null;
            rightParam = (GridSqlParameter)rightVal;
        }
        else
            return null;

        if (leftParam != null || rightParam != null)
            return null;

        // Check const dataTypes
        int leftConstType = leftConst.value().getType();

        // TODO: 26.12.18 use leftCol.column().getType() instead of leftConstType/rightConstType ?
        if (!(leftConstType == Value.BYTE || leftConstType == Value.SHORT || leftConstType == Value.INT ||
            leftConstType == Value.LONG))
            return null;

        int rightConstType = rightConst.value().getType();

        if (!(rightConstType == Value.BYTE || rightConstType == Value.SHORT || rightConstType == Value.INT ||
            rightConstType == Value.LONG))
            return null;

//        Long lowerLongVal;
//        Long upperLongVal;
//
//        switch (leftOpType) {
//            case BIGGER_EQUAL:
//
//                break;
//            case BIGGER:
//                break;
//
//            case SMALLER:
//
//                break;
//
//            case SMALLER_EQUAL:
//
//                break;
//
//                default:
//                    return null;
//        }
//
//        if (leftOpType == GridSqlOperationType.BIGGER)
//            lowerLongVal = leftConst.value().getLong() + 1;
//        else if (leftOpType == GridSqlOperationType.BIGGER_EQUAL)
//            lowerLongVal = leftConst.value().getLong();
//        else if (leftOpType == GridSqlOperationType.SMALLER)
//            upperLongVal = leftConst.value().getLong() - 1;
//        else if (leftOpType == GridSqlOperationType.SMALLER_EQUAL)
//            upperLongVal = leftConst.value().getLong();
//
//        if (rightOpType == GridSqlOperationType.BIGGER)
//            lowerLongVal = leftConst.value().getLong() + 1;
//        else if (rightOpType == GridSqlOperationType.BIGGER_EQUAL)
//            lowerLongVal = leftConst.value().getLong();
//        else if (rightOpType == GridSqlOperationType.SMALLER)
//            upperLongVal = leftConst.value().getLong() - 1;
//        else if (rightOpType == GridSqlOperationType.SMALLER_EQUAL)
//            upperLongVal = leftConst.value().getLong();
//
//        = leftOpType == GridSqlOperationType.BIGGER_EQUAL leftConst.value().getLong();

        long leftLongVal = leftConst.value().getLong();
        long rightLongVal = rightConst.value().getLong();

//        // Swap long values if right is less then left.
//        if (rightLongVal < leftLongVal) {
//            leftLongVal = rightLongVal;
//            rightLongVal = leftConst.value().getLong();
//        }

        // Increment left long value if '>' is used.
        if (leftOpType == GridSqlOperationType.BIGGER)
            leftLongVal++;

        // Decrement right long value if '<' is used.
        if (rightOpType == GridSqlOperationType.SMALLER)
            rightLongVal--;

        Set<PartitionSingleNode> parts = new HashSet<>();

        for (long i = leftLongVal; i <= rightLongVal; i++) {
            // Do extract.
            PartitionSingleNode part = extractSingle(leftCol.column(), new GridSqlConst(ValueLong.get(i)),
                null);

            if (part == null)
                return null;

            parts.add(part);

            int maxPartitionsCnt = Integer.getInteger(
                IgniteSystemProperties.IGNITE_PARTITIONS_PRUNNING_MAX_PARTIONS_BETWEEN, MAX_PARTITIONS_COUNT_BETWEEN);

            if (parts.size() > maxPartitionsCnt)
                return null;
        }

        if (parts.isEmpty())
            return PartitionNoneNode.INSTANCE;

        return parts.size() == 1 ? parts.iterator().next() : new PartitionGroupNode(parts);
    }
}
