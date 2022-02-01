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

package org.apache.ignite.internal.processors.query.h2.sql;

import java.util.TreeSet;
import org.apache.ignite.IgniteException;
import org.h2.value.Value;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.query.h2.sql.GridSqlConst.TRUE;
import static org.apache.ignite.internal.processors.query.h2.sql.GridSqlFunctionType.MAX;
import static org.apache.ignite.internal.processors.query.h2.sql.GridSqlFunctionType.MIN;

/**
 * Utility methods for splitter.
 */
public class SplitterUtils {
    /**
     * Check whether AST element has aggregates.
     *
     * @param el Expression part in SELECT clause.
     * @return {@code true} If expression contains aggregates.
     */
    public static boolean hasAggregates(GridSqlAst el) {
        if (el instanceof GridSqlAggregateFunction)
            return true;

        // If in SELECT clause we have a subquery expression with aggregate,
        // we should not split it. Run the whole subquery on MAP stage.
        if (el instanceof GridSqlSubquery)
            return false;

        for (int i = 0; i < el.size(); i++) {
            if (hasAggregates(el.child(i)))
                return true;
        }

        return false;
    }

    /**
     * @param qry Select.
     * @param paramsCnt Number of parameters.
     * @param paramIdxs Parameter indexes.
     */
    public static void findParamsQuery(GridSqlQuery qry, int paramsCnt, TreeSet<Integer> paramIdxs) {
        if (qry instanceof GridSqlSelect)
            findParamsSelect((GridSqlSelect)qry, paramsCnt, paramIdxs);
        else {
            GridSqlUnion union = (GridSqlUnion)qry;

            findParamsQuery(union.left(), paramsCnt, paramIdxs);
            findParamsQuery(union.right(), paramsCnt, paramIdxs);

            findParams(qry.limit(), paramsCnt, paramIdxs);
            findParams(qry.offset(), paramsCnt, paramIdxs);
        }
    }

    /**
     * @param select Select.
     * @param paramsCnt Number of parameters.
     * @param paramIdxs Parameter indexes.
     */
    private static void findParamsSelect(GridSqlSelect select, int paramsCnt, TreeSet<Integer> paramIdxs) {
        if (paramsCnt == 0)
            return;

        for (GridSqlAst el : select.columns(false))
            findParams(el, paramsCnt, paramIdxs);

        findParams(select.from(), paramsCnt, paramIdxs);
        findParams(select.where(), paramsCnt, paramIdxs);

        // Don't search in GROUP BY and HAVING since they expected to be in select list.

        findParams(select.limit(), paramsCnt, paramIdxs);
        findParams(select.offset(), paramsCnt, paramIdxs);
    }

    /**
     * @param el Element.
     * @param paramsCnt Number of parameters.
     * @param paramIdxs Parameter indexes.
     */
    private static void findParams(@Nullable GridSqlAst el, int paramsCnt, TreeSet<Integer> paramIdxs) {
        if (el == null)
            return;

        if (el instanceof GridSqlParameter) {
            // H2 Supports queries like "select ?5" but first 4 non-existing parameters are need to be set to any value.
            // Here we will set them to NULL.
            final int idx = ((GridSqlParameter)el).index();

            if (paramsCnt <= idx)
                throw new IgniteException("Invalid number of query parameters. " +
                    "Cannot find " + idx + " parameter.");

            paramIdxs.add(idx);
        }
        else if (el instanceof GridSqlSubquery)
            findParamsQuery(((GridSqlSubquery)el).subquery(), paramsCnt, paramIdxs);
        else {
            for (int i = 0; i < el.size(); i++)
                findParams(el.child(i), paramsCnt, paramIdxs);
        }
    }

    /**
     * Lookup for distinct aggregates.
     * Note, DISTINCT make no sense for MIN and MAX aggregates, so its will be ignored.
     *
     * @param el Expression.
     * @return {@code true} If expression contains distinct aggregates.
     */
    public static boolean hasDistinctAggregates(GridSqlAst el) {
        if (el instanceof GridSqlAggregateFunction) {
            GridSqlFunctionType type = ((GridSqlAggregateFunction)el).type();

            return ((GridSqlAggregateFunction)el).distinct() && type != MIN && type != MAX;
        }

        for (int i = 0; i < el.size(); i++) {
            if (hasDistinctAggregates(el.child(i)))
                return true;
        }

        return false;
    }

    /**
     * Check whether LEFT OUTER join exist.
     *
     * @param from FROM clause.
     * @return {@code true} If contains LEFT OUTER JOIN.
     */
    public static boolean hasLeftJoin(GridSqlAst from) {
        while (from instanceof GridSqlJoin) {
            GridSqlJoin join = (GridSqlJoin)from;

            assert !(join.rightTable() instanceof GridSqlJoin);

            if (join.isLeftOuter())
                return true;

            from = join.leftTable();
        }

        return false;
    }

    /**
     * @param ast Reduce query AST.
     * @param rdcQry Reduce query string.
     */
    public static void checkNoDataTablesInReduceQuery(GridSqlAst ast, String rdcQry) {
        if (ast instanceof GridSqlTable) {
            if (((GridSqlTable)ast).dataTable() != null)
                throw new IgniteException("Failed to generate REDUCE query. Data table found: " + ast.getSQL() +
                    " \n" + rdcQry);
        }
        else {
            for (int i = 0; i < ast.size(); i++)
                checkNoDataTablesInReduceQuery(ast.child(i), rdcQry);
        }
    }

    /**
     * @param expr Expression.
     * @return {@code true} If this expression represents a constant value `TRUE`.
     */
    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    public static boolean isTrue(GridSqlAst expr) {
        return expr instanceof GridSqlConst && ((GridSqlConst)expr).value() == TRUE.value();
    }

    /**
     * @param type data type id
     * @return true if given type is fractional
     */
    public static boolean isFractionalType(int type) {
        return type == Value.DECIMAL || type == Value.FLOAT || type == Value.DOUBLE;
    }

    /**
     * @param distinct Distinct.
     * @param type Type.
     * @return Aggregate function.
     */
    public static GridSqlAggregateFunction aggregate(boolean distinct, GridSqlFunctionType type) {
        return new GridSqlAggregateFunction(distinct, type);
    }

    /**
     * @param name Column name.
     * @return Column.
     */
    public static GridSqlColumn column(String name) {
        return new GridSqlColumn(null, null, null, null, name);
    }

    /**
     * @param alias Alias.
     * @param child Child.
     * @return Alias.
     */
    public static GridSqlAlias alias(String alias, GridSqlAst child) {
        GridSqlAlias res = new GridSqlAlias(alias, child);

        res.resultType(child.resultType());

        return res;
    }

    /**
     * @param type Type.
     * @param left Left expression.
     * @param right Right expression.
     * @return Binary operator.
     */
    public static GridSqlOperation op(GridSqlOperationType type, GridSqlAst left, GridSqlAst right) {
        return new GridSqlOperation(type, left, right);
    }

    /**
     * @param ast Map query AST.
     * @return {@code true} If the given AST has partitioned tables.
     */
    public static boolean hasPartitionedTables(GridSqlAst ast) {
        if (ast instanceof GridSqlTable) {
            if (((GridSqlTable)ast).dataTable() != null)
                return ((GridSqlTable)ast).dataTable().isPartitioned();
            else
                return false;
        }

        for (int i = 0; i < ast.size(); i++) {
            if (hasPartitionedTables(ast.child(i)))
                return true;
        }

        return false;
    }

    /**
     * Check whether the given SELECT has subqueries.
     *
     * @param qry Query.
     * @return {@code True} if subqueries are found.
     */
    public static boolean hasSubQueries(GridSqlSelect qry) {
        boolean res = hasSubQueries0(qry.where()) || hasSubQueries0(qry.from());

        if (!res) {
            for (int i = 0; i < qry.columns(false).size(); i++) {
                if (hasSubQueries0(qry.column(i))) {
                    res = true;

                    break;
                }
            }
        }

        return res;
    }

    /**
     * @param ast Query AST.
     * @return {@code true} If the given AST has sub-queries.
     */
    private static boolean hasSubQueries0(GridSqlAst ast) {
        if (ast == null)
            return false;

        if (ast instanceof GridSqlSubquery)
            return true;

        for (int childIdx = 0; childIdx < ast.size(); childIdx++) {
            if (hasSubQueries0(ast.child(childIdx)))
                return true;
        }

        return false;
    }

    /**
     * Private constructor.
     */
    private SplitterUtils() {
        // No-op.
    }
}
