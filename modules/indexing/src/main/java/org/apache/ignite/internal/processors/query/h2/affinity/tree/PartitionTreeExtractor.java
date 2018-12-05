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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.query.GridCacheSqlQuery;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2RowDescriptor;
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
import org.h2.table.IndexColumn;
import org.jetbrains.annotations.Nullable;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.ignite.internal.processors.query.h2.opt.GridH2KeyValueRowOnheap.DEFAULT_COLUMNS_COUNT;

/**
 * Partition tree extractor.
 */
public class PartitionTreeExtractor {
    /** Indexing. */
    private final IgniteH2Indexing idx;

    /**
     * Constructor.
     *
     * @param idx Indexing.
     */
    public PartitionTreeExtractor(IgniteH2Indexing idx) {
        this.idx = idx;
    }

    /**
     * Merge partition info from multiple queries.
     *
     * @param qrys Queries.
     * @return Partition result or {@code null} if nothing is resolved.
     */
    public PartitionResult merge(List<GridCacheSqlQuery> qrys) {
        // Check if merge is possible.
        PartitionTableDescriptor desc = null;

        for (GridCacheSqlQuery qry : qrys) {
            PartitionResult qryRes = (PartitionResult)qry.derivedPartitions2();

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
            PartitionResult qryRes = (PartitionResult)qry.derivedPartitions2();

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

        if (parts.size() == 1)
            return parts.iterator().next();
        else
            return new PartitionGroupNode(parts);
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

        if (!isAffinityKey(leftCol.getColumnId(), tbl))
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
     *
     * @param colId Column ID to check
     * @param tbl H2 Table
     * @return is affinity key or not
     */
    private static boolean isAffinityKey(int colId, GridH2Table tbl) {
        GridH2RowDescriptor desc = tbl.rowDescriptor();

        if (desc.isKeyColumn(colId))
            return true;

        IndexColumn affKeyCol = tbl.getAffinityKeyColumn();

        try {
            return
                affKeyCol != null &&
                colId >= DEFAULT_COLUMNS_COUNT &&
                desc.isColumnKeyProperty(colId - DEFAULT_COLUMNS_COUNT) &&
                colId == affKeyCol.column.getColumnId();
        }
        catch (IllegalStateException e) {
            return false;
        }
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
}
