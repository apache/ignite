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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.query.GridCacheSqlQuery;
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
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlTable;
import org.apache.ignite.internal.sql.optimizer.affinity.PartitionAffinityFunctionType;
import org.apache.ignite.internal.sql.optimizer.affinity.PartitionAllNode;
import org.apache.ignite.internal.sql.optimizer.affinity.PartitionCompositeNode;
import org.apache.ignite.internal.sql.optimizer.affinity.PartitionCompositeNodeOperator;
import org.apache.ignite.internal.sql.optimizer.affinity.PartitionConstantNode;
import org.apache.ignite.internal.sql.optimizer.affinity.PartitionGroupNode;
import org.apache.ignite.internal.sql.optimizer.affinity.PartitionJoinCondition;
import org.apache.ignite.internal.sql.optimizer.affinity.PartitionNode;
import org.apache.ignite.internal.sql.optimizer.affinity.PartitionNoneNode;
import org.apache.ignite.internal.sql.optimizer.affinity.PartitionParameterNode;
import org.apache.ignite.internal.sql.optimizer.affinity.PartitionParameterType;
import org.apache.ignite.internal.sql.optimizer.affinity.PartitionResult;
import org.apache.ignite.internal.sql.optimizer.affinity.PartitionSingleNode;
import org.apache.ignite.internal.sql.optimizer.affinity.PartitionTable;
import org.apache.ignite.internal.sql.optimizer.affinity.PartitionTableAffinityDescriptor;
import org.apache.ignite.internal.sql.optimizer.affinity.PartitionTableModel;
import org.apache.ignite.internal.util.typedef.F;
import org.h2.table.Column;
import org.h2.value.Value;
import org.jetbrains.annotations.Nullable;

/**
 * Partition tree extractor.
 */
public class PartitionExtractor {
    /**
     * Maximum number of partitions to be used in case of between expression.
     * In case of exceeding all partitions will be used.
     */
    private static final int DFLT_MAX_EXTRACTED_PARTS_FROM_BETWEEN = 16;

    /** Partition resolver. */
    private final H2PartitionResolver partResolver;

    /** Maximum number of partitions to be used in case of between expression. */
    private final int maxPartsCntBetween;

    /** Grid kernal context. */
    private final GridKernalContext ctx;

    /**
     * Constructor.
     *
     * @param partResolver Partition resolver.
     * @param ctx Grid kernal context.
     */
    public PartitionExtractor(H2PartitionResolver partResolver, GridKernalContext ctx) {
        this.partResolver = partResolver;

        maxPartsCntBetween = Integer.getInteger(
            IgniteSystemProperties.IGNITE_SQL_MAX_EXTRACTED_PARTS_FROM_BETWEEN,
            DFLT_MAX_EXTRACTED_PARTS_FROM_BETWEEN
        );

        this.ctx = ctx;
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
        return new PartitionResult(tree, tblModel.joinGroupAffinity(tree.joinGroup()),
            ctx.cache().context().exchange().readyAffinityVersion());
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
        PartitionTableAffinityDescriptor aff = null;

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

        AffinityTopologyVersion affinityTopVer = null;

        for (GridCacheSqlQuery qry : qrys) {
            PartitionResult qryRes = (PartitionResult)qry.derivedPartitions();

            if (tree == null)
                tree = qryRes.tree();
            else
                tree = new PartitionCompositeNode(tree, qryRes.tree(), PartitionCompositeNodeOperator.OR);

            if (affinityTopVer == null)
                affinityTopVer = qryRes.topologyVersion();
            else
                assert affinityTopVer.equals(qryRes.topologyVersion());
        }

        // Optimize.
        assert tree != null;

        tree = tree.optimize();

        if (tree instanceof PartitionAllNode)
            return null;

        // If there is no affinity, then we assume "NONE" result.
        assert aff != null || tree == PartitionNoneNode.INSTANCE;

        // Affinity topology version expected to be the same for all partition results derived from map queries.
        // TODO: 09.04.19 IGNITE-11507: SQL: Ensure that affinity topology version doesn't change
        // TODO: during PartitionResult construction/application.
        assert affinityTopVer != null;
        
        return new PartitionResult(tree, aff, affinityTopVer);
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
    private List<PartitionTable> prepareTableModel0(GridSqlAst from, PartitionTableModel model) {
        if (from instanceof GridSqlJoin) {
            // Process JOIN recursively.
            GridSqlJoin join = (GridSqlJoin)from;

            List<PartitionTable> leftTbls = prepareTableModel0(join.leftTable(), model);
            List<PartitionTable> rightTbls = prepareTableModel0(join.rightTable(), model);

            if (join.isLeftOuter()) {
                // "a LEFT JOIN b" is transformed into "a", and "b" is put into special stop-list.
                // If a condition is met on "b" afterwards, we will ignore it.
                for (PartitionTable rightTbl : rightTbls)
                    model.addExcludedTable(rightTbl.alias());

                return leftTbls;
            }

            // Extract equi-join or cross-join from condition. For normal INNER JOINs most likely we will have "1=1"
            // cross join here, real join condition will be found in WHERE clause later.
            PartitionJoinCondition cond = parseJoinCondition(join.on());

            if (cond != null && !cond.cross())
                model.addJoin(cond);

            ArrayList<PartitionTable> res = new ArrayList<>(leftTbls.size() + rightTbls.size());

            res.addAll(leftTbls);
            res.addAll(rightTbls);

            return res;
        }

        PartitionTable tbl = prepareTable(from, model);

        return tbl != null ? Collections.singletonList(tbl) : Collections.emptyList();
    }

    /**
     * Try parsing condition as simple JOIN codition. Only equijoins are supported for now, so anything more complex
     * than "A.a = B.b" are not processed.
     *
     * @param on Initial AST.
     * @return Join condition or {@code null} if not simple equijoin.
     */
    private static PartitionJoinCondition parseJoinCondition(GridSqlElement on) {
        if (on instanceof GridSqlOperation) {
            GridSqlOperation on0 = (GridSqlOperation)on;

            if (on0.operationType() == GridSqlOperationType.EQUAL) {
                // Check for cross-join first.
                GridSqlConst leftConst = unwrapConst(on0.child(0));
                GridSqlConst rightConst = unwrapConst(on0.child(1));

                if (leftConst != null && rightConst != null) {
                    try {
                        int leftConstval = leftConst.value().getInt();
                        int rightConstVal = rightConst.value().getInt();

                        if (leftConstval == rightConstVal)
                            return PartitionJoinCondition.CROSS;
                    }
                    catch (Exception ignore) {
                        // No-op.
                    }
                }

                // This is not cross-join, neither normal join between columns.
                if (leftConst != null || rightConst != null)
                    return null;

                // Check for normal equi-join.
                GridSqlColumn left = unwrapColumn(on0.child(0));
                GridSqlColumn right = unwrapColumn(on0.child(1));

                if (left != null && right != null) {
                    String leftAlias = left.tableAlias();
                    String rightAlias = right.tableAlias();

                    String leftCol = left.columnName();
                    String rightCol = right.columnName();

                    return new PartitionJoinCondition(leftAlias, rightAlias, leftCol, rightCol);
                }
            }
        }

        return null;
    }

    /**
     * Prepare single table.
     *
     * @param from Expression.
     * @param tblModel Table model.
     * @return Added table or {@code null} if table is exlcuded from the model.
     */
    private static PartitionTable prepareTable(GridSqlAst from, PartitionTableModel tblModel) {
        // Unwrap alias. We assume that every table must be aliased.
        assert from instanceof GridSqlAlias;

        String alias = ((GridSqlAlias)from).alias();

        from = from.child();

        if (from instanceof GridSqlTable) {
            // Normal table.
            GridSqlTable from0 = (GridSqlTable)from;

            GridH2Table tbl0 = from0.dataTable();

            // Unknown table type, e.g. temp table.
            if (tbl0 == null) {
                tblModel.addExcludedTable(alias);

                return null;
            }

            String cacheName = tbl0.cacheName();

            String affColName = null;
            String secondAffColName = null;

            for (Column col : tbl0.getColumns()) {
                if (tbl0.isColumnForPartitionPruningStrict(col)) {
                    if (affColName == null)
                        affColName = col.getName();
                    else {
                        secondAffColName = col.getName();

                        // Break as we cannot have more than two affinity key columns.
                        break;
                    }
                }
            }

            PartitionTable tbl = new PartitionTable(alias, cacheName, affColName, secondAffColName);

            PartitionTableAffinityDescriptor aff = affinityForCache(tbl0.cacheInfo().config());

            if (aff == null) {
                // Non-standard affinity, exclude table.
                tblModel.addExcludedTable(alias);

                return null;
            }

            tblModel.addTable(tbl, aff);

            return tbl;
        }
        else {
            // Subquery/union/view, etc.
            assert alias != null;

            tblModel.addExcludedTable(alias);

            return null;
        }
    }

    /**
     * Prepare affinity identifier for cache.
     *
     * @param ccfg Cache configuration.
     * @return Affinity identifier.
     */
    private static PartitionTableAffinityDescriptor affinityForCache(CacheConfiguration ccfg) {
        // Partition could be extracted only from PARTITIONED caches.
        if (ccfg.getCacheMode() != CacheMode.PARTITIONED)
            return null;

        PartitionAffinityFunctionType aff = ccfg.getAffinity().getClass().equals(RendezvousAffinityFunction.class) ?
            PartitionAffinityFunctionType.RENDEZVOUS : PartitionAffinityFunctionType.CUSTOM;

        boolean hasNodeFilter = ccfg.getNodeFilter() != null &&
            !(ccfg.getNodeFilter() instanceof CacheConfiguration.IgniteAllNodesPredicate);

        return new PartitionTableAffinityDescriptor(
            aff,
            ccfg.getAffinity().partitions(),
            hasNodeFilter,
            ccfg.getDataRegionName()
        );
    }

    /**
     * Extract partitions from expression.
     *
     * @param expr Expression.
     * @param tblModel Table model.
     * @param disjunct Whether current processing frame is located under disjunction ("OR"). In this case we cannot
     *                 rely on join expressions like (A.a = B.b) to build co-location model because another conflicting
     *                 join expression on the same tables migth be located on the other side of the "OR".
     *                 Example: "JOIN ON A.a = B.b OR A.a > B.b".
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

        PartitionNode betweenNodes = tryExtractBetween(op, tblModel);

        if (betweenNodes != null)
            return betweenNodes;

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
        else {
            if (right instanceof GridSqlColumn) {
                if (!disjunct) {
                    PartitionJoinCondition cond = parseJoinCondition(op);

                    if (cond != null && !cond.cross())
                        tblModel.addJoin(cond);
                }

            }
            return PartitionAllNode.INSTANCE;
        }

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

        PartitionTable tbl0 = tblModel.table(leftCol.tableAlias());

        // If table is in ignored set, then we cannot use it for partition extraction.
        if (tbl0 == null)
            return null;

        if (rightConst != null) {
            int part = partResolver.partition(
                rightConst.value().getObject(),
                leftCol0.getType(),
                tbl.cacheName()
            );

            return new PartitionConstantNode(tbl0, part);
        }
        else if (rightParam != null) {
            int colType = leftCol0.getType();

            return new PartitionParameterNode(
                tbl0,
                partResolver,
                rightParam.index(),
                leftCol0.getType(),
                mappedType(colType)
            );
        }
        else
            return null;
    }

    /**
     * Mapped Ignite type for H2 type.
     *
     * @param type H2 type.
     * @return ignite type.
     */
    @Nullable private static PartitionParameterType mappedType(int type) {
        // Try map if possible.
        switch (type) {
            case Value.BOOLEAN:
                return PartitionParameterType.BOOLEAN;

            case Value.BYTE:
                return PartitionParameterType.BYTE;

            case Value.SHORT:
                return PartitionParameterType.SHORT;

            case Value.INT:
                return PartitionParameterType.INT;

            case Value.LONG:
                return PartitionParameterType.LONG;

            case Value.FLOAT:
                return PartitionParameterType.FLOAT;

            case Value.DOUBLE:
                return PartitionParameterType.DOUBLE;

            case Value.STRING:
                return PartitionParameterType.STRING;

            case Value.DECIMAL:
                return PartitionParameterType.DECIMAL;

            case Value.UUID:
                return PartitionParameterType.UUID;
        }

        // Otherwise we do not support it.
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

    /**
     * Try to extract partitions from {@code op} assuming that it's between operation or simple range.
     *
     * @param op Sql operation.
     * @param tblModel Table model.
     * @return {@code PartitionSingleNode} if operation reduced to one partition,
     *   {@code PartitionGroupNode} if operation reduced to multiple partitions or null if operation is neither
     *   between nor simple range. Null also returns if it's not possible to extract partitions from given operation.
     * @throws IgniteCheckedException If failed.
     */
    private PartitionNode tryExtractBetween(GridSqlOperation op, PartitionTableModel tblModel)
        throws IgniteCheckedException {
        // Between operation (or similar range) should contain exact two children.
        assert op.size() == 2;

        GridSqlAst left = op.child();
        GridSqlAst right = op.child(1);

        GridSqlOperationType leftOpType = retrieveOperationType(left);
        GridSqlOperationType rightOpType = retrieveOperationType(right);

        if ((GridSqlOperationType.BIGGER == rightOpType || GridSqlOperationType.BIGGER_EQUAL == rightOpType) &&
            (GridSqlOperationType.SMALLER == leftOpType || GridSqlOperationType.SMALLER_EQUAL == leftOpType)) {
            GridSqlAst tmp = left;
            left = right;
            right = tmp;
        }
        else if (!((GridSqlOperationType.BIGGER == leftOpType || GridSqlOperationType.BIGGER_EQUAL == leftOpType) &&
            (GridSqlOperationType.SMALLER == rightOpType || GridSqlOperationType.SMALLER_EQUAL == rightOpType)))
            return null;

        // Try parse left AST.
        GridSqlColumn leftCol;

        if (left instanceof GridSqlOperation && left.child() instanceof GridSqlColumn &&
            (((GridSqlColumn)left.child()).column().getTable() instanceof GridH2Table))
            leftCol = left.child();
        else
            return null;

        // Try parse right AST.
        GridSqlColumn rightCol;

        if (right instanceof GridSqlOperation && right.child() instanceof GridSqlColumn)
            rightCol = right.child();
        else
            return null;

        GridH2Table tbl = (GridH2Table)leftCol.column().getTable();

        // Check that columns might be used for partition pruning.
        if (!tbl.isColumnForPartitionPruning(leftCol.column()))
            return null;

        // Check that both left and right AST use same column.
        if (!F.eq(leftCol.schema(), rightCol.schema()) ||
            !F.eq(leftCol.columnName(), rightCol.columnName()) ||
            !F.eq(leftCol.tableAlias(), rightCol.tableAlias()))
            return null;

        // Check columns type
        if (!(leftCol.column().getType() == Value.BYTE || leftCol.column().getType() == Value.SHORT ||
            leftCol.column().getType() == Value.INT || leftCol.column().getType() == Value.LONG))
            return null;

        // Try parse left AST right value (value to the right of '>' or '>=').
        GridSqlConst leftConst;

        if (left.child(1) instanceof GridSqlConst)
            leftConst = left.child(1);
        else
            return null;

        // Try parse right AST right value (value to the right of '<' or '<=').
        GridSqlConst rightConst;

        if (right.child(1) instanceof GridSqlConst)
            rightConst = right.child(1);
        else
            return null;

        long leftLongVal;
        long rightLongVal;

        try {
            leftLongVal = leftConst.value().getLong();
            rightLongVal = rightConst.value().getLong();
        }
        catch (Exception e) {
            return null;
        }

        // Increment left long value if '>' is used.
        if (((GridSqlOperation)left).operationType() == GridSqlOperationType.BIGGER)
            leftLongVal++;

        // Decrement right long value if '<' is used.
        if (((GridSqlOperation)right).operationType() == GridSqlOperationType.SMALLER)
            rightLongVal--;

        Set<PartitionSingleNode> parts = new HashSet<>();

        PartitionTable tbl0 = tblModel.table(leftCol.tableAlias());

        // If table is in ignored set, then we cannot use it for partition extraction.
        if (tbl0 == null)
            return null;

        for (long i = leftLongVal; i <= rightLongVal; i++) {
            int part = partResolver.partition(i, leftCol.column().getType(), tbl0.cacheName());

            parts.add(new PartitionConstantNode(tbl0, part));

            if (parts.size() > maxPartsCntBetween)
                return null;
        }

        return parts.isEmpty() ? PartitionNoneNode.INSTANCE :
            parts.size() == 1 ? parts.iterator().next() : new PartitionGroupNode(parts);
    }

    /**
     * Retrieves operation type.
     *
     * @param ast Tree
     * @return Operation type.
     */
    private GridSqlOperationType retrieveOperationType(GridSqlAst ast) {
        if (!(ast instanceof GridSqlOperation))
            return null;

        return ((GridSqlOperation)ast).operationType();
    }
}
