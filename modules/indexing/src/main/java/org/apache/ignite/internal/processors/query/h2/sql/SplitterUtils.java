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

import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
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
     * Checks whether the expression has an OUTER JOIN from replicated to partitioned.
     *
     * This is used to infer the `treatReplicatedAsPartitioned` flag
     * to eventually pass it to {@link org.apache.ignite.spi.indexing.IndexingQueryFilterImpl}.
     *
     * @param from FROM expression.
     * @return {@code true} if the expression has an OUTER JOIN from replicated to partitioned.
     */
    public static boolean hasOuterJoinReplicatedPartitioned(GridSqlAst from) {
        boolean isRightPartitioned = false;
        while (from instanceof GridSqlJoin) {
            GridSqlJoin join = (GridSqlJoin)from;

            assert !(join.rightTable() instanceof GridSqlJoin);

            isRightPartitioned = isRightPartitioned || hasPartitionedTables(join.rightTable());

            if (join.isLeftOuter()) {
                boolean isLeftPartitioned = hasPartitionedTables(join.leftTable());
                return !isLeftPartitioned && isRightPartitioned;
            }

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
     * Traverse AST while join operation isn't found. Check it if found.
     */
    public static void lookForPartitionedJoin(GridSqlAst ast, GridSqlAst upWhere, IgniteLogger log) {
        if (ast == null)
            return;

        GridSqlJoin join = null;
        GridSqlAst where = null;

        if (ast instanceof GridSqlJoin) {
            join = (GridSqlJoin) ast;
            where = upWhere;

        } else if (ast instanceof GridSqlSelect) {
            GridSqlSelect select = (GridSqlSelect) ast;

            if (select.from() instanceof GridSqlJoin) {
                join = (GridSqlJoin) select.from();
                where = select.where();
            }
        }

        // Traverse AST deeper.
        if (join == null) {
            for (int i = 0; i < ast.size(); i++)
                lookForPartitionedJoin(ast.child(i), null, log);

            return;
        }

        // Also checks WHERE condition.
        lookForPartitionedJoin(where, null, log);

        GridSqlTable leftTable = getTable(join.leftTable());

        // Left side of join is a subquery.
        if (leftTable == null) {
            lookForPartitionedJoin(join.leftTable(), where, log);
            return;
        }

        GridSqlTable rightTable = getTable(join.rightTable());

        // Right side of join is a subquery.
        if (rightTable == null) {
            lookForPartitionedJoin(join.rightTable(), where, log);
            return;
        }

        GridH2Table left = leftTable.dataTable();
        GridH2Table right = leftTable.dataTable();

        // Skip check at least of tables isn't partitioned.
        if (!(left.isPartitioned() && right.isPartitioned()))
            return;

        checkPartitionedJoin(join, where, left, right, log);
    }

    /**
     * Checks whether an AST contains valid join operation between partitioned tables.
     * Join condition should be an equality operation of affinity keys of tables. Conditions can be splitted between
     * join and where clauses. If join is invalid then warning a user about that.
     *
     * @param join The join to check.
     * @param where The where statement from previous AST, for nested joins.
     * @param left Left side of join.
     * @param right Right side of join.
     * @param log Ignite logger.
     */
    private static void checkPartitionedJoin(GridSqlJoin join, GridSqlAst where, GridH2Table left, GridH2Table right, IgniteLogger log) {
        String leftTblAls = getAlias(join.leftTable());
        String rightTblAls = getAlias(join.rightTable());

        // User explicitly specify an affinity key. Otherwise use primary key.
        boolean pkLeft = left.getExplicitAffinityKeyColumn() == null;
        boolean pkRight = right.getExplicitAffinityKeyColumn() == null;

        Set<String> leftAffKeys = affKeys(pkLeft, left);
        Set<String> rightAffKeys = affKeys(pkRight, right);

        boolean joinIsValid = checkPartitionedCondition(join.on(),
            leftTblAls, leftAffKeys, pkLeft,
            rightTblAls, rightAffKeys, pkRight);

        if (!joinIsValid && where instanceof GridSqlElement)
            joinIsValid = checkPartitionedCondition((GridSqlElement) where,
                leftTblAls, leftAffKeys, pkLeft,
                rightTblAls, rightAffKeys, pkRight);

        if (!joinIsValid) {
            log.warning(
                String.format(
                    "For join two partitioned tables join condition should be the equality operation of affinity keys." +
                        " Left side: %s; right side: %s", left.getName(), right.getName())
            );
        }
    }

    /** @return Set of possible affinity keys for this table, incl. default _KEY. */
    private static Set<String> affKeys(boolean pk, GridH2Table tbl) {
        Set<String> affKeys = new HashSet<>();

        // User explicitly specify an affinity key. Otherwise use primary key.
        if (!pk)
            affKeys.add(tbl.getAffinityKeyColumn().columnName);
        else {
            affKeys.add("_KEY");

            String keyFieldName = tbl.rowDescriptor().type().keyFieldName();

            if (keyFieldName == null)
                affKeys.addAll(tbl.rowDescriptor().type().primaryKeyFields());
            else
                affKeys.add(keyFieldName);
        }

        return affKeys;
    }

    /**
     * Valid join condition contains:
     * 1. Equality of Primary (incl. cases of complex PK) or Affinity keys;
     * 2. Additional conditions must be joint with AND operation to the affinity join condition.
     *
     * @return {@code true} if join condition contains affinity join condition, otherwise {@code false}.
     */
    private static boolean checkPartitionedCondition(GridSqlElement condition,
        String leftTbl, Set<String> leftAffKeys, boolean pkLeft,
        String rightTbl, Set<String> rightAffKeys, boolean pkRight) {

        if (!(condition instanceof GridSqlOperation))
            return false;

        GridSqlOperation op = (GridSqlOperation) condition;

        // It is may be a part of affinity condition.
        if (GridSqlOperationType.EQUAL == op.operationType())
            checkEqualityOperation(op, leftTbl, leftAffKeys, pkLeft, rightTbl, rightAffKeys, pkRight);

        // Check affinity condition is covered fully. If true then return. Otherwise go deeper.
        if (affinityCondIsCovered(leftAffKeys, rightAffKeys))
            return true;

        // If we don't cover affinity condition prior to first AND then this is not an affinity condition.
        if (GridSqlOperationType.AND != op.operationType())
            return false;

        // Go recursively to childs.
        for (int i = 0; i < op.size(); i++) {
            boolean ret = checkPartitionedCondition(op.child(i),
                leftTbl, leftAffKeys, pkLeft,
                rightTbl, rightAffKeys, pkRight);

            if (ret)
                return true;
        }

        // Join condition doesn't contain affinity condition.
        return false;
    }

    /** */
    private static boolean affinityCondIsCovered(Set<String> leftAffKeys, Set<String> rightAffKeys) {
        return leftAffKeys.isEmpty() && rightAffKeys.isEmpty();
    }

    /** */
    private static void checkEqualityOperation(GridSqlOperation equalOp,
        String leftTbl, Set<String> leftCols, boolean pkLeft,
        String rightTbl, Set<String> rightCols, boolean pkRight) {

        // TODO: Alias?
        if (!(equalOp.child(0) instanceof GridSqlColumn))
            return;

        // TODO: Alias?
        if (!(equalOp.child(1) instanceof GridSqlColumn))
            return;

        String leftCol = ((GridSqlColumn) equalOp.child(0)).columnName();
        String rightCol = ((GridSqlColumn) equalOp.child(1)).columnName();

        String leftTblAls = ((GridSqlColumn) equalOp.child(0)).tableAlias();
        String rightTblAls = ((GridSqlColumn) equalOp.child(1)).tableAlias();

        Set<String> actLeftCols;
        Set<String> actRightCols;

        if (leftTbl.equals(leftTblAls))
            actLeftCols = leftCols;
        else if (leftTbl.equals(rightTblAls))
            actLeftCols = rightCols;
        else
            return;

        if (rightTbl.equals(rightTblAls))
            actRightCols = rightCols;
        else if (rightTbl.equals(leftTblAls))
            actRightCols = leftCols;
        else
            return;

        // This is part of the affinity join condition.
        if (actLeftCols.contains(leftCol) && actRightCols.contains(rightCol)) {
            if (pkLeft && "_KEY".equals(leftCol))
                actLeftCols.clear();
            else if (pkLeft) {
                actLeftCols.remove(leftCol);
                // Only _KEY is there.
                if (actLeftCols.size() == 1)
                    actLeftCols.clear();
            }
            else
                actLeftCols.remove(leftCol);

            if (pkRight && "_KEY".equals(rightCol))
                actRightCols.clear();
            else if (pkRight) {
                actRightCols.remove(rightCol);
                // Only _KEY is there.
                if (actRightCols.size() == 1)
                    actRightCols.clear();
            }
            else
                actRightCols.remove(rightCol);
        }
    }

    /** Extract table instance from an AST element. */
    private static GridSqlTable getTable(GridSqlElement el) {
        if (el instanceof GridSqlTable)
            return (GridSqlTable) el;

        if (el instanceof GridSqlAlias && el.child() instanceof GridSqlTable)
            return el.child();

        return null;
    }

    /** Extract alias value. */
    private static String getAlias(GridSqlElement el) {
        if (el instanceof GridSqlAlias)
            return ((GridSqlAlias)el).alias();

        return null;
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
