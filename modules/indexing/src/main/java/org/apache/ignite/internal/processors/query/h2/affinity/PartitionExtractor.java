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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.query.GridCacheSqlQuery;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.processors.query.h2.affinity.join.PartitionJoinAffinityDescriptor;
import org.apache.ignite.internal.processors.query.h2.affinity.join.PartitionJoinCondition;
import org.apache.ignite.internal.processors.query.h2.affinity.join.PartitionTableModel;
import org.apache.ignite.internal.processors.query.h2.affinity.join.PartitionJoinTable;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlAlias;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlAst;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlColumn;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlConst;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlElement;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlJoin;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlOperation;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlOperationType;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlParameter;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlQuery;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlSelect;
import org.h2.table.Column;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Partition tree extractor.
 */
public class PartitionExtractor {
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

        // Prepare table model.
        PartitionTableModel tblModel = prepareTableModel(select.from());

        // Do extract.
        PartitionNode tree = extractFromExpression(select.where(), tblModel, false);

        assert tree != null;

        // Reduce tree if possible.
        tree = tree.optimize();

        if (tree instanceof PartitionAllNode)
            return null;

        // Done.
        return new PartitionResult(tree, tblModel.joinGroupAffinity(tree.joinGroup()));
    }

    /**
     * Merge partition info from multiple queries.
     *
     * @param qrys Queries.
     * @return Partition result or {@code null} if nothing is resolved.
     */
    @SuppressWarnings("IfMayBeConditional")
    public PartitionResult mergeMapQueries(List<GridCacheSqlQuery> qrys) {
        // Check if merge is possible.
        PartitionJoinAffinityDescriptor aff = null;

        for (GridCacheSqlQuery qry : qrys) {
            PartitionResult qryRes = (PartitionResult)qry.derivedPartitions();

            // Failed to get results for one query -> broadcast.
            if (qryRes == null)
                return null;

            // This only possible if query is resolved to "NONE". Will be skipped later during map request prepare.
            if (qryRes.affinity() == null)
                continue;

            if (aff == null)
                aff = qryRes.affinity();
            else if (!aff.isCompatible(qryRes.affinity()))
                // Queries refer to incompatible affinity groups, cannot merge -> broadcast.
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

        // If there is no affinity, then we assume "NONE" result.
        assert aff != null || tree == PartitionNoneNode.INSTANCE;

        return new PartitionResult(tree, aff);
    }

    /**
     * Prepare table model.
     *
     * @param from FROM clause.
     * @return Join model.
     */
    private PartitionTableModel prepareTableModel(GridSqlAst from) {
        PartitionTableModel res = new PartitionTableModel();

        prepareTableModel0(from, res);

        return res;
    }

    /**
     * Prepare tables which will be used in join model.
     *
     * @param from From flag.
     * @param model Table model.
     * @return {@code True} if extracted tables successfully, {@code false} if failed to extract.
     */
    private List<PartitionJoinTable> prepareTableModel0(GridSqlAst from, PartitionTableModel model) {
        if (from instanceof GridSqlJoin) {
            // Process JOIN recursively.
            GridSqlJoin join = (GridSqlJoin)from;

            List<PartitionJoinTable> leftTbls = prepareTableModel0(join.leftTable(), model);
            List<PartitionJoinTable> rightTbls = prepareTableModel0(join.rightTable(), model);

            if (join.isLeftOuter()) {
                // "a LEFT JOIN b" is transformed into "a", and "b" is put into special stop-list.
                // If a condition is met on "b" afterwards, we will stop partition pruning process.
                for (PartitionJoinTable rightTbl : rightTbls)
                    model.addExcludedTable(rightTbl.alias());

                return leftTbls;
            }

            // Extract equi-join or cross-join from condition. For normal INNER JOINs most likely we will have "1=1"
            // cross join here, real join condition will be found in WHERE clause later.
            PartitionJoinCondition cond = PartitionExtractorUtils.parseJoinCondition(join.on());

            if (cond != null && cond.cross())
                model.addJoin(cond);

            ArrayList<PartitionJoinTable> res = new ArrayList<>(leftTbls.size() + rightTbls.size());

            res.addAll(leftTbls);
            res.addAll(rightTbls);

            return res;
        }

        PartitionJoinTable tbl = PartitionExtractorUtils.prepareTable(from, model);

        return Collections.singletonList(tbl);
    }

    /**
     * Extract partitions from expression.
     *
     * @param expr Expression.
     * @param tblModel Table model.
     * @param disjunct Whether current processing frame is located under disjunction ("OR"). In this case we cannot
     *                 rely on join expressions like (A.a = B.b) to build co-location model because another conflicting
     *                 join expression on the same tables migth be located on the other side of the "OR".
     *                 Example: "JOIN on A.a = B.b OR A.a > B.b".
     * @return Partition tree.
     */
    @SuppressWarnings("EnumSwitchStatementWhichMissesCases")
    private PartitionNode extractFromExpression(GridSqlAst expr, PartitionTableModel tblModel, boolean disjunct)
        throws IgniteCheckedException {
        PartitionNode res = PartitionAllNode.INSTANCE;

        if (expr instanceof GridSqlOperation) {
            GridSqlOperation op = (GridSqlOperation)expr;

            switch (op.operationType()) {
                case AND:
                    res = extractFromAnd(op, tblModel, disjunct);

                    break;

                case OR:
                    res = extractFromOr(op, tblModel);

                    break;

                case IN:
                    res = extractFromIn(op, tblModel);

                    break;

                case EQUAL:
                    res = extractFromEqual(op, tblModel, disjunct);
            }
        }

        // Cannot determine partition.
        return res;
    }

    /**
     * Extract partition information from AND.
     *
     * @param op Operation.
     * @param tblModel Table model.
     * @param disjunct Disjunction marker.
     * @return Partition.
     */
    private PartitionNode extractFromAnd(GridSqlOperation op, PartitionTableModel tblModel, boolean disjunct)
        throws IgniteCheckedException {
        assert op.size() == 2;

        PartitionNode part1 = extractFromExpression(op.child(0), tblModel, disjunct);
        PartitionNode part2 = extractFromExpression(op.child(1), tblModel, disjunct);

        return new PartitionCompositeNode(part1, part2, PartitionCompositeNodeOperator.AND);
    }

    /**
     * Extract partition information from OR.
     *
     * @param op Operation.
     * @param tblModel Table model.
     * @return Partition.
     */
    private PartitionNode extractFromOr(GridSqlOperation op, PartitionTableModel tblModel)
        throws IgniteCheckedException {
        assert op.size() == 2;

        // Parse inner expressions recursively with disjuncion flag set.
        PartitionNode part1 = extractFromExpression(op.child(0), tblModel, true);
        PartitionNode part2 = extractFromExpression(op.child(1), tblModel, true);

        return new PartitionCompositeNode(part1, part2, PartitionCompositeNodeOperator.OR);
    }

    /**
     * Extract partition information from IN.
     *
     * @param op Operation.
     * @param tblModel Table model.
     * @return Partition.
     */
    private PartitionNode extractFromIn(GridSqlOperation op, PartitionTableModel tblModel)
        throws IgniteCheckedException {
        // Operation should contain at least two children: left (column) and right (const or column).
        if (op.size() < 2)
            return PartitionAllNode.INSTANCE;

        // Left operand should be column.
        GridSqlAst left = op.child();

        GridSqlColumn leftCol = unwrapColumn(left);

        if (leftCol == null)
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

            // Extract.
            PartitionSingleNode part = extractSingle(leftCol, rightConst, rightParam, tblModel);

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
     * @param tblModel Table model.
     * @param disjunct Disjunction flag. When set possible join expression will not be processed.
     * @return Partition.
     */
    private PartitionNode extractFromEqual(GridSqlOperation op, PartitionTableModel tblModel, boolean disjunct)
        throws IgniteCheckedException {
        assert op.operationType() == GridSqlOperationType.EQUAL;

        GridSqlElement left = op.child(0);
        GridSqlElement right = op.child(1);

        GridSqlColumn leftCol = unwrapColumn(left);

        if (leftCol == null)
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
        else if (right instanceof GridSqlColumn) {
            if (!disjunct) {
                PartitionJoinCondition cond = PartitionExtractorUtils.parseJoinCondition(op);

                if (cond != null && cond.cross())
                    tblModel.addJoin(cond);
            }

            return PartitionAllNode.INSTANCE;
        }
        else
            return PartitionAllNode.INSTANCE;

        PartitionSingleNode part = extractSingle(leftCol, rightConst, rightParam, tblModel);

        return part != null ? part : PartitionAllNode.INSTANCE;
    }

    /**
     * Extract single partition.
     *
     * @param leftCol Left column.
     * @param rightConst Right constant.
     * @param rightParam Right parameter.
     * @param tblModel Table model.
     * @return Partition or {@code null} if failed to extract.
     */
    @Nullable private PartitionSingleNode extractSingle(
        GridSqlColumn leftCol,
        GridSqlConst rightConst,
        GridSqlParameter rightParam,
        PartitionTableModel tblModel
    ) throws IgniteCheckedException {
        assert leftCol != null;

        Column leftCol0 = leftCol.column();

        assert leftCol0.getTable() != null;
        assert leftCol0.getTable() instanceof GridH2Table;

        GridH2Table tbl = (GridH2Table)leftCol0.getTable();

        if (!tbl.isColumnForPartitionPruning(leftCol0))
            return null;

        PartitionJoinTable tbl0 = tblModel.table(leftCol.tableAlias());

        // If table is in ignored set, then we cannot use it for partition extraction.
        if (tbl0 == null)
            return null;

        if (rightConst != null) {
            int part = idx.kernalContext().affinity().partition(tbl.cacheName(), rightConst.value().getObject());

            return new PartitionConstantNode(tbl0, part);
        }
        else if (rightParam != null)
            return new PartitionParameterNode(tbl0, idx, rightParam.index(), leftCol0.getType());
        else
            return null;
    }

    /**
     * Unwrap constant if possible.
     *
     * @param ast AST.
     * @return Constant or {@code null} if not a constant.
     */
    @Nullable public static GridSqlConst unwrapConst(GridSqlAst ast) {
        return ast instanceof GridSqlConst ? (GridSqlConst)ast : null;
    }

    /**
     * Unwrap column if possible.
     *
     * @param ast AST.
     * @return Column or {@code null} if not a column.
     */
    @Nullable public static GridSqlColumn unwrapColumn(GridSqlAst ast) {
        if (ast instanceof GridSqlAlias)
            ast = ast.child();

        return ast instanceof GridSqlColumn ? (GridSqlColumn)ast : null;
    }
}
