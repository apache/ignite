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
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.cache.query.GridCacheSqlQuery;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2RowDescriptor;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlAst;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlColumn;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlConst;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlElement;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlOperation;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlOperationType;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlParameter;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlQuery;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlSelect;
import org.h2.table.Column;
import org.h2.table.IndexColumn;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import static org.apache.ignite.internal.processors.query.h2.opt.GridH2KeyValueRowOnheap.DEFAULT_COLUMNS_COUNT;

/**
 * Utility class to extract partitions from the query.
 */
public class PartitionExtractor {
    /**
     * Ensures all given queries have non-empty derived partitions and merges them.
     *
     * @param queries Collection of queries.
     * @return Derived partitions for all queries, or {@code null}.
     */
    public static PartitionInfo[] mergePartitionsFromMultipleQueries(List<GridCacheSqlQuery> queries) {
        PartitionInfo[] res = null;

        HashSet<PartitionInfo> res0 = new HashSet<>();

        for (GridCacheSqlQuery qry : queries) {
            PartitionInfo[] qryPartInfo = (PartitionInfo[])qry.derivedPartitions();

            if (qryPartInfo == null)
                return null;

            Collections.addAll(res0, qryPartInfo);
        }

        if (!res0.isEmpty()) {
            res = new PartitionInfo[res0.size()];

            int idx = 0;

            for (PartitionInfo part : res0)
                res[idx++] = part;
        }

        return res;
    }

    /**
     * Checks if given query contains expressions over key or affinity key
     * that make it possible to run it only on a small isolated
     * set of partitions.
     *
     * @param qry Query.
     * @param ctx Kernal context.
     * @return Array of partitions, or {@code null} if none identified
     */
    public static PartitionInfo[] derivePartitionsFromQuery(GridSqlQuery qry, GridKernalContext ctx)
        throws IgniteCheckedException {
        // No unions support yet.
        if (!(qry instanceof GridSqlSelect))
            return null;

        GridSqlSelect select = (GridSqlSelect)qry;

        // no joins support yet
        if (select.from() == null || select.from().size() != 1)
            return null;

        return extractPartition(select.where(), ctx);
    }

    /**
     * @param el AST element to start with.
     * @param ctx Kernal context.
     * @return Array of partition info objects, or {@code null} if none identified
     */
    private static PartitionInfo[] extractPartition(GridSqlAst el, GridKernalContext ctx)
        throws IgniteCheckedException {

        if (!(el instanceof GridSqlOperation))
            return null;

        GridSqlOperation op = (GridSqlOperation)el;

        switch (op.operationType()) {
            case EQUAL: {
                PartitionInfo partInfo = extractPartitionFromEquality(op, ctx);

                if (partInfo != null)
                    return new PartitionInfo[] { partInfo };

                return null;
            }

            case AND: {
                assert op.size() == 2;

                PartitionInfo[] partsLeft = extractPartition(op.child(0), ctx);
                PartitionInfo[] partsRight = extractPartition(op.child(1), ctx);

                if (partsLeft != null && partsRight != null)
                    return intersectPartitionInfo(partsLeft, partsRight);

                if (partsLeft != null)
                    return partsLeft;

                if (partsRight != null)
                    return partsRight;

                return null;
            }

            case OR: {
                assert op.size() == 2;

                PartitionInfo[] partsLeft = extractPartition(op.child(0), ctx);
                PartitionInfo[] partsRight = extractPartition(op.child(1), ctx);

                if (partsLeft != null && partsRight != null)
                    return mergePartitionInfo(partsLeft, partsRight);

                return null;
            }

            case IN: {
                // Operation should contain at least two children: left (column) and right (const or column).
                if (op.size() < 2)
                    return null;

                // Left operand should be column.
                GridSqlAst left = op.child();

                GridSqlColumn leftCol;

                if (left instanceof GridSqlColumn)
                    leftCol = (GridSqlColumn)left;
                else
                    return null;

                // Can work only with Ignite's tables.
                if (!(leftCol.column().getTable() instanceof GridH2Table))
                    return null;

                PartitionInfo[] res = new PartitionInfo[op.size() - 1];

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
                        return null;

                    PartitionInfo cur = getCacheQueryPartitionInfo(
                        leftCol.column(),
                        rightConst,
                        rightParam,
                        ctx
                    );

                    // Same thing as above: single unknown partition in disjunction defeats optimization.
                    if (cur == null)
                        return null;

                    res[i - 1] = cur;
                }

                return res;
            }

            default:
                return null;
        }
    }

    /**
     * Analyses the equality operation and extracts the partition if possible
     *
     * @param op AST equality operation.
     * @param ctx Kernal Context.
     * @return partition info, or {@code null} if none identified
     */
    private static PartitionInfo extractPartitionFromEquality(GridSqlOperation op, GridKernalContext ctx)
        throws IgniteCheckedException {

        assert op.operationType() == GridSqlOperationType.EQUAL;

        GridSqlElement left = op.child(0);
        GridSqlElement right = op.child(1);

        GridSqlColumn leftCol;

        if (left instanceof GridSqlColumn)
            leftCol = (GridSqlColumn)left;
        else
            return null;

        if (!(leftCol.column().getTable() instanceof GridH2Table))
            return null;

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
            return null;

        return getCacheQueryPartitionInfo(leftCol.column(), rightConst, rightParam, ctx);
    }

    /**
     * Merges two partition info arrays, removing duplicates
     *
     * @param a Partition info array.
     * @param b Partition info array.
     * @return Result.
     */
    private static PartitionInfo[] mergePartitionInfo(PartitionInfo[] a, PartitionInfo[] b) {
        assert a != null;
        assert b != null;

        if (a.length == 1 && b.length == 1) {
            if (a[0].equals(b[0]))
                return new PartitionInfo[] { a[0] };

            return new PartitionInfo[] { a[0], b[0] };
        }

        ArrayList<PartitionInfo> list = new ArrayList<>(a.length + b.length);

        Collections.addAll(list, a);

        for (PartitionInfo part: b) {
            int i = 0;

            while (i < list.size() && !list.get(i).equals(part))
                i++;

            if (i == list.size())
                list.add(part);
        }

        PartitionInfo[] result = new PartitionInfo[list.size()];

        for (int i = 0; i < list.size(); i++)
            result[i] = list.get(i);

        return result;
    }

    /**
     * Extracts the partition if possible
     * @param leftCol Column on the left side.
     * @param rightConst Constant on the right side.
     * @param rightParam Parameter on the right side.
     * @param ctx Kernal Context.
     * @return partition info, or {@code null} if none identified
     * @throws IgniteCheckedException If failed.
     */
    @Nullable private static PartitionInfo getCacheQueryPartitionInfo(
        Column leftCol,
        GridSqlConst rightConst,
        GridSqlParameter rightParam,
        GridKernalContext ctx
    ) throws IgniteCheckedException {
        assert leftCol != null;
        assert leftCol.getTable() != null;
        assert leftCol.getTable() instanceof GridH2Table;

        GridH2Table tbl = (GridH2Table)leftCol.getTable();

        if (!isAffinityKey(leftCol.getColumnId(), tbl))
            return null;

        if (rightConst != null) {
            int part = ctx.affinity().partition(tbl.cacheName(), rightConst.value().getObject());

            return new PartitionInfo(
                part,
                null,
                null,
                -1,
                -1
            );
        }
        else if (rightParam != null) {
            return new PartitionInfo(
                -1,
                tbl.cacheName(),
                tbl.getName(),
                leftCol.getType(),
                rightParam.index()
            );
        }
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
            return affKeyCol != null && colId >= DEFAULT_COLUMNS_COUNT && desc.isColumnKeyProperty(colId - DEFAULT_COLUMNS_COUNT) && colId == affKeyCol.column.getColumnId();
        }
        catch(IllegalStateException e) {
            return false;
        }
    }

    /**
     * Find parametrized partitions.
     *
     * @param parts All partitions.
     * @return Parametrized partitions.
     */
    private static ArrayList<PartitionInfo> findParameterized(PartitionInfo[] parts){
        ArrayList<PartitionInfo> res = new ArrayList<>(parts.length);

        for (PartitionInfo p : parts) {
            if (p.partition() < 0)
                res.add(p);
        }

        return res;
    }

    /**
     * Merges two partition info arrays, removing duplicates
     *
     * @param a Partition info array.
     * @param b Partition info array.
     * @return Result.
     */
    private static PartitionInfo[] intersectPartitionInfo(
        PartitionInfo[] a,
        PartitionInfo[] b) {
        assert a != null;
        assert b != null;

        if (a.length == 0 || b.length == 0)
            return new PartitionInfo[0];

        ArrayList<PartitionInfo> aWithParams = findParameterized(a);
        ArrayList<PartitionInfo> bWithParams = findParameterized(b);

        if (aWithParams.size() > 0 || bWithParams.size() > 0){
            PartitionInfo[][] holder = new PartitionInfo[2][];

            holder[0] = a;

            holder[1] = b;

            PartitionInfo[] res = new PartitionInfo[1];

            res[0] = new PartitionInfo(holder);

            return res;
        }
        // Here conjunction logic could be added to handle one-side parameterized cases to remove partitions in cases
        // like [1,2,?] AND [2,3] where partition 1 could be calculated as not actually needed.
        // NB! partition 2 should stay as it is playing role in case where parameter belongs to any other after binding
        else {
            ArrayList<PartitionInfo> list = new ArrayList<>(a.length + b.length);

            for (PartitionInfo partA : a) {
                for (PartitionInfo partB : b) {
                    if (partA.equals(partB))
                        list.add(partA);
                }
            }

            return list.toArray(new PartitionInfo[0]);
        }
    }

    /**
     * Private constructor.
     */
    private PartitionExtractor() {
        // No-op.
    }
}
