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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.query.GridCacheSqlQuery;
import org.apache.ignite.internal.processors.cache.query.GridCacheTwoStepQuery;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2AbstractKeyValueRow;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.apache.ignite.internal.util.lang.GridTriple;
import org.apache.ignite.internal.util.lang.IgnitePair;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgnitePredicate;
import org.h2.command.Prepared;
import org.h2.command.dml.Delete;
import org.h2.command.dml.Explain;
import org.h2.command.dml.Insert;
import org.h2.command.dml.Merge;
import org.h2.command.dml.Query;
import org.h2.command.dml.Update;
import org.h2.jdbc.JdbcPreparedStatement;
import org.h2.table.Column;
import org.h2.util.IntArray;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.query.h2.opt.GridH2CollocationModel.isCollocated;
import static org.apache.ignite.internal.processors.query.h2.sql.GridSqlQueryParser.prepared;
import static org.apache.ignite.internal.processors.query.h2.sql.GridSqlQueryParser.query;
import static org.apache.ignite.internal.processors.query.h2.twostep.GridReduceQueryExecutor.toArray;

/**
 * Arbitrary SQL statement splitter (queries and DML statements).
 */
public final class GridSqlStatementSplitter {
    /**
     * Empty ctor to prevent initialization.
     */
    private GridSqlStatementSplitter() {
        // No-op.
    }

    /**
     * @param stmt Prepared statement.
     * @param params Parameters.
     * @param injectKeysFilter Whether only specific keys must be selected.
     * @param collocatedGrpBy Whether the query has collocated GROUP BY keys.
     * @param distributedJoins If distributed joins enabled.
     * @return Two step query.
     */
    public static GridCacheTwoStepQuery split(JdbcPreparedStatement stmt, Object[] params, Object[] injectKeysFilter,
        boolean collocatedGrpBy, boolean distributedJoins) throws IgniteCheckedException {
        if (params == null)
            params = GridCacheSqlQuery.EMPTY_PARAMS;

        final Prepared prepared = prepared(stmt);

        if (prepared instanceof Query || prepared instanceof Explain)
            return GridSqlQuerySplitter.split(stmt, params, collocatedGrpBy, distributedJoins);

        if (prepared instanceof Merge || prepared instanceof Update || prepared instanceof Insert ||
            prepared instanceof Delete)
            return GridSqlStatementSplitter.splitUpdateQuery(prepared, params, injectKeysFilter, collocatedGrpBy,
                distributedJoins);

        throw new UnsupportedOperationException("Query not supported [cls=" + prepared.getClass().getName() + "]");
    }

    /**
     * @param prepared Prepared SQL INSERT, MERGE, UPDATE, or DELETE statement.
     * @param params Parameters.
     * @param keysFilter Whether only specific keys must be selected.
     * @param collocatedGrpBy Collocated SELECT (for WHERE and subqueries).
     * @param distributedJoins If distributed joins enabled.    @return Two step query.
     */
    private static GridCacheTwoStepQuery splitUpdateQuery(Prepared prepared, Object[] params, Object[] keysFilter,
            boolean collocatedGrpBy, final boolean distributedJoins) throws IgniteCheckedException {
        GridSqlStatement gridStmt = new GridSqlQueryParser().parse(prepared);

        X.ensureX(gridStmt instanceof GridSqlInsert || gridStmt instanceof GridSqlMerge ||
            gridStmt instanceof GridSqlUpdate || gridStmt instanceof GridSqlDelete, "SQL update operation expected");

        if (gridStmt instanceof GridSqlUpdate)
            return splitUpdate((GridSqlUpdate)gridStmt, params, keysFilter, collocatedGrpBy, distributedJoins);

        if (gridStmt instanceof GridSqlDelete)
            return splitDelete((GridSqlDelete)gridStmt, params, keysFilter, collocatedGrpBy, distributedJoins);

        GridSqlElement target;

        GridSqlQuery sel;

        boolean hasRows;

        if (gridStmt instanceof GridSqlInsert) {
            GridSqlInsert ins = (GridSqlInsert) gridStmt;
            target = ins.into();
            hasRows = !F.isEmpty(ins.rows());
            sel = selectForInsertOrMerge(ins.rows(), ins.query());
        }
        else {
            GridSqlMerge merge = (GridSqlMerge) gridStmt;
            target = merge.into();
            hasRows = !F.isEmpty(merge.rows());
            sel = selectForInsertOrMerge(merge.rows(), merge.query());
        }

        X.ensureX(target != null, "Failed to retrieve target for SQL update operation");

        // Build resulting two step query.
        GridCacheTwoStepQuery res;

        if (!hasRows) // INSERT or MERGE from SELECT
            res = GridSqlQuerySplitter.split(sel, params, collocatedGrpBy, distributedJoins);
        else {
            res = new GridCacheTwoStepQuery(new HashSet<String>(), new HashSet<String>());

            IntArray paramIdxs = new IntArray(params.length);

            GridCacheSqlQuery rdc = new GridCacheSqlQuery(sel.getSQL(),
                findParams(sel, params, new ArrayList<>(), paramIdxs).toArray());

            rdc.parameterIndexes(toArray(paramIdxs));

            res.reduceQuery(rdc);

            // We do not have to look at each map query separately here, because if
            // the whole initial query is collocated, then all the map sub-queries
            // will be collocated as well.
            //
            // Statement left for clarity - currently update operations don't really care much about joins.
            res.distributedJoins(distributedJoins && !isCollocated(query(prepared)));
        }

        GridSqlQuerySplitter.collectAllTablesInFrom(target, res.schemas(), res.tables());

        res.initialStatement(gridStmt);

        return res;
    }

    /**
     * Create SELECT on which subsequent INSERT or MERGE will be based.
     *
     * @param rows Rows to create pseudo-SELECT upon.
     * @param subQry Subquery to use rather than rows.
     * @return Subquery or pseudo-SELECT to evaluate inserted expressions.
     */
    public static GridSqlQuery selectForInsertOrMerge(List<GridSqlElement[]> rows, GridSqlQuery subQry) {
        if (!F.isEmpty(rows)) {
            GridSqlSelect sel = new GridSqlSelect();

            for (GridSqlElement[] row : rows)
                sel.addColumn(new GridSqlArray(F.asList(row)), true);

            return sel;
        }
        else {
            assert subQry != null;

            return subQry;
        }
    }

    /** */
    @SuppressWarnings("ConstantConditions")
    private static GridCacheTwoStepQuery splitDelete(GridSqlDelete del, Object[] params, Object[] keysFilter,
                                                     boolean collocatedGrpBy, boolean distributedJoins) throws IgniteCheckedException {
        GridTriple<GridSqlElement> singleUpdate = getSingleItemFilter(del);

        if (singleUpdate != null)
            return twoStepQueryForSingleItem(del, del.from(), singleUpdate);

        int paramsCnt = F.isEmpty(params) ? 0 : params.length;
        GridSqlSelect mapQry = mapQueryForDelete(del, !F.isEmpty(keysFilter) ? paramsCnt : null);

        params = Arrays.copyOf(U.firstNotNull(params, X.EMPTY_OBJECT_ARRAY), paramsCnt + 1);
        params[paramsCnt] = keysFilter;

        GridCacheTwoStepQuery res = GridSqlQuerySplitter.split(mapQry, params, collocatedGrpBy, distributedJoins);
        res.initialStatement(del);

        return res;
    }

    /**
     * Generate SQL SELECT based on DELETE's WHERE, LIMIT, etc.
     *
     * @param del Delete statement.
     * @param keysParamIdx Index for .
     * @return SELECT statement.
     */
    public static GridSqlSelect mapQueryForDelete(GridSqlDelete del, @Nullable Integer keysParamIdx)
        throws IgniteCheckedException {
        GridSqlSelect mapQry = new GridSqlSelect();

        mapQry.from(del.from());

        Set<GridSqlTable> tbls = new HashSet<>();

        collectAllGridTablesInTarget(del.from(), tbls);

        X.ensureX(tbls.size() == 1, "Failed to determine target table for DELETE");

        GridSqlTable tbl = tbls.iterator().next();

        GridH2Table gridTbl = tbl.dataTable();

        X.ensureX(gridTbl != null, "Failed to determine target grid table for DELETE");

        Column h2KeyCol = gridTbl.getColumn(GridH2AbstractKeyValueRow.KEY_COL);

        Column h2ValCol = gridTbl.getColumn(GridH2AbstractKeyValueRow.VAL_COL);

        GridSqlColumn keyCol = new GridSqlColumn(h2KeyCol, tbl, h2KeyCol.getName(), h2KeyCol.getSQL());
        keyCol.resultType(GridSqlType.fromColumn(h2KeyCol));

        GridSqlColumn valCol = new GridSqlColumn(h2ValCol, tbl, h2ValCol.getName(), h2ValCol.getSQL());
        valCol.resultType(GridSqlType.fromColumn(h2ValCol));

        mapQry.addColumn(keyCol, true);
        mapQry.addColumn(valCol, true);

        GridSqlElement where = del.where();
        if (keysParamIdx != null)
            where = injectKeysFilterParam(where, keyCol, keysParamIdx);

        mapQry.where(where);
        mapQry.limit(del.limit());

        return mapQry;
    }

    /** */
    @SuppressWarnings("ConstantConditions")
    private static GridCacheTwoStepQuery splitUpdate(GridSqlUpdate update, Object[] params, Object[] keysFilter,
                                                     boolean collocatedGrpBy, boolean distributedJoins) throws IgniteCheckedException {
        GridTriple<GridSqlElement> singleUpdate = getSingleItemFilter(update);

        if (singleUpdate != null)
            return twoStepQueryForSingleItem(update, update.target(), singleUpdate);

        int paramsCnt = F.isEmpty(params) ? 0 : params.length;
        GridSqlSelect mapQry = mapQueryForUpdate(update, !F.isEmpty(keysFilter) ? paramsCnt: null);

        params = Arrays.copyOf(U.firstNotNull(params, X.EMPTY_OBJECT_ARRAY), paramsCnt + 1);
        params[paramsCnt] = keysFilter;

        GridCacheTwoStepQuery res = GridSqlQuerySplitter.split(mapQry, params, collocatedGrpBy, distributedJoins);
        res.initialStatement(update);

        return res;
    }


    /**
     * @param stmt Initial statement.
     * @param target Table or alias the statement points to.
     * @param singleUpdate Operation arguments.
     * @return Empty two step query that bears only initial statement and single operation arguments.
     */
    private static GridCacheTwoStepQuery twoStepQueryForSingleItem(GridSqlStatement stmt,
                                                                   GridSqlElement target, GridTriple<GridSqlElement> singleUpdate) {
        Set<String> schemas = new HashSet<>();
        Set<String> tbls = new HashSet<>();

        GridSqlQuerySplitter.collectAllTablesInFrom(target, schemas, tbls);

        // No need to do any more parsing - we know that this statement affects one item at most.
        GridCacheTwoStepQuery res = new GridCacheTwoStepQuery(schemas, tbls);

        res.initialStatement(stmt);
        res.singleUpdate(singleUpdate);

        return res;
    }

    /**
     * @param update UPDATE statement.
     * @return {@code null} if given statement directly updates {@code _val} column with a literal or param value
     * and filters by single non expression key (and, optionally,  by single non expression value).
     */
    public static GridTriple<GridSqlElement> getSingleItemFilter(GridSqlUpdate update) {
        IgnitePair<GridSqlElement> filter = findKeyValueEqulaityCondition(update.where());

        if (filter == null)
            return null;

        if (update.cols().size() != 1 ||
            !IgniteH2Indexing.VAL_FIELD_NAME.equalsIgnoreCase(update.cols().get(0).columnName()))
            return null;

        GridSqlElement set = update.set().get(update.cols().get(0).columnName());

        if (!(set instanceof GridSqlConst || set instanceof GridSqlParameter))
            return null;

        return new GridTriple<>(filter.getKey(), filter.getValue(), set);
    }

    /**
     * @param del DELETE statement.
     * @return {@code true} if given statement filters by single non expression key.
     */
    public static GridTriple<GridSqlElement> getSingleItemFilter(GridSqlDelete del) {
        IgnitePair<GridSqlElement> filter = findKeyValueEqulaityCondition(del.where());

        if (filter == null)
            return null;

        return new GridTriple<>(filter.getKey(), filter.getValue(), null);
    }

    /**
     * @param where Element to test.
     * @return Whether given element corresponds to {@code WHERE _key = ?}, and key is a literal expressed
     * in query or a query param.
     */
    private static IgnitePair<GridSqlElement> findKeyValueEqulaityCondition(GridSqlElement where) {
        if (where == null || !(where instanceof GridSqlOperation))
            return null;

        GridSqlOperation whereOp = (GridSqlOperation) where;

        // Does this WHERE limit only by _key?
        if (isKeyEqualityCondition(whereOp))
            return new IgnitePair<>(whereOp.child(1), null);

        // Or maybe it limits both by _key and _val?
        if (whereOp.operationType() != GridSqlOperationType.AND)
            return null;

        GridSqlElement left = whereOp.child(0);

        GridSqlElement right = whereOp.child(1);

        if (!(left instanceof GridSqlOperation && right instanceof GridSqlOperation))
            return null;

        GridSqlOperation leftOp = (GridSqlOperation) left;

        GridSqlOperation rightOp = (GridSqlOperation) right;

        if (isKeyEqualityCondition(leftOp)) { // _key = ? and _val = ?
            if (!isValueEqualityCondition(rightOp))
                return null;

            return new IgnitePair<>(leftOp.child(1), rightOp.child(1));
        }
        else if (isKeyEqualityCondition(rightOp)) { // _val = ? and _key = ?
            if (!isValueEqualityCondition(leftOp))
                return null;

            return new IgnitePair<>(rightOp.child(1), leftOp.child(1));
        }
        else // Neither
            return null;
    }

    /**
     * @param op Operation.
     * @param colName Column name to check.
     * @return Whether this condition is of form {@code colName} = ?
     */
    private static boolean isEqualityCondition(GridSqlOperation op, String colName) {
        if (op.operationType() != GridSqlOperationType.EQUAL)
            return false;

        GridSqlElement left = op.child(0);
        GridSqlElement right = op.child(1);

        return left instanceof GridSqlColumn &&
            colName.equalsIgnoreCase(((GridSqlColumn) left).columnName()) &&
            (right instanceof GridSqlConst || right instanceof GridSqlParameter);
    }

    /**
     * @param op Operation.
     * @return Whether this condition is of form _key = ?
     */
    private static boolean isKeyEqualityCondition(GridSqlOperation op) {
        return isEqualityCondition(op, IgniteH2Indexing.KEY_FIELD_NAME);
    }

    /**
     * @param op Operation.
     * @return Whether this condition is of form _val = ?
     */
    private static boolean isValueEqualityCondition(GridSqlOperation op) {
        return isEqualityCondition(op, IgniteH2Indexing.VAL_FIELD_NAME);
    }


    /**
     * Generate SQL SELECT based on UPDATE's WHERE, LIMIT, etc.
     *
     * @param update Update statement.
     * @param keysParamIdx Index of new param for the array of keys.
     * @return SELECT statement.
     */
    public static GridSqlSelect mapQueryForUpdate(GridSqlUpdate update, @Nullable Integer keysParamIdx)
        throws IgniteCheckedException {
        GridSqlSelect mapQry = new GridSqlSelect();

        mapQry.from(update.target());

        Set<GridSqlTable> tbls = new HashSet<>();

        collectAllGridTablesInTarget(update.target(), tbls);

        X.ensureX(tbls.size() == 1, "Failed to determine target table for UPDATE");

        GridSqlTable tbl = tbls.iterator().next();

        GridH2Table gridTbl = tbl.dataTable();

        X.ensureX(gridTbl != null, "Failed to determine target grid table for UPDATE");

        Column h2KeyCol = gridTbl.getColumn(GridH2AbstractKeyValueRow.KEY_COL);

        Column h2ValCol = gridTbl.getColumn(GridH2AbstractKeyValueRow.VAL_COL);

        GridSqlColumn keyCol = new GridSqlColumn(h2KeyCol, tbl, h2KeyCol.getName(), h2KeyCol.getSQL());
        keyCol.resultType(GridSqlType.fromColumn(h2KeyCol));

        GridSqlColumn valCol = new GridSqlColumn(h2ValCol, tbl, h2ValCol.getName(), h2ValCol.getSQL());
        valCol.resultType(GridSqlType.fromColumn(h2ValCol));

        mapQry.addColumn(keyCol, true);
        mapQry.addColumn(valCol, true);

        for (GridSqlColumn c : update.cols()) {
            String newColName = "_upd_" + c.columnName();
            // We have to use aliases to cover cases when the user
            // wants to update _val field directly (if it's a literal)
            GridSqlAlias alias = new GridSqlAlias(newColName, update.set().get(c.columnName()), true);
            alias.resultType(c.resultType());
            mapQry.addColumn(alias, true);
        }

        GridSqlElement where = update.where();
        if (keysParamIdx != null)
            where = injectKeysFilterParam(where, keyCol, keysParamIdx);

        mapQry.where(where);
        mapQry.limit(update.limit());

        return mapQry;
    }

    /**
     * Append additional condition to WHERE for it to select only specific keys.
     *
     * @param where Initial condition.
     * @param keyCol Column to base the new condition on.
     * @return New condition.
     */
    private static GridSqlElement injectKeysFilterParam(GridSqlElement where, GridSqlColumn keyCol, int paramIdx) {
        GridSqlElement e = new GridSqlOperation(GridSqlOperationType.IN, keyCol, new GridSqlParameter(paramIdx));

        if (where == null)
            return e;
        else
            return new GridSqlOperation(GridSqlOperationType.AND, where, e);
    }

    /**
     * @param stmt Statement.
     * @param params Parameters.
     * @param target Extracted parameters.
     * @param paramIdxs Parameter indexes.
     * @return Extracted parameters list.
     */
    private static List<Object> findParams(GridSqlStatement stmt, Object[] params, ArrayList<Object> target,
                                           IntArray paramIdxs) {
        if (stmt instanceof GridSqlQuery)
            return GridSqlQuerySplitter.findParams((GridSqlQuery) stmt, params, target, paramIdxs);

        if (stmt instanceof GridSqlMerge)
            return findParams((GridSqlMerge)stmt, params, target, paramIdxs);

        if (stmt instanceof GridSqlInsert)
            return findParams((GridSqlInsert)stmt, params, target, paramIdxs);

        return target;
    }

    /**
     * @param stmt Statement.
     * @param params Parameters.
     * @param target Extracted parameters.
     * @param paramIdxs Parameter indexes.
     * @return Extracted parameters list.
     */
    private static List<Object> findParams(GridSqlMerge stmt, Object[] params, ArrayList<Object> target,
                                           IntArray paramIdxs) {
        if (params.length == 0)
            return target;

        for (GridSqlElement el : stmt.columns())
            GridSqlQuerySplitter.findParams(el, params, target, paramIdxs);

        for (GridSqlElement[] row : stmt.rows())
            for (GridSqlElement el : row)
                GridSqlQuerySplitter.findParams(el, params, target, paramIdxs);

        return target;
    }

    /**
     * @param stmt Statement.
     * @param params Parameters.
     * @param target Extracted parameters.
     * @param paramIdxs Parameter indexes.
     * @return Extracted parameters list.
     */
    private static List<Object> findParams(GridSqlInsert stmt, Object[] params, ArrayList<Object> target,
                                           IntArray paramIdxs) {
        if (params.length == 0)
            return target;

        for (GridSqlElement el : stmt.columns())
            GridSqlQuerySplitter.findParams(el, params, target, paramIdxs);

        for (GridSqlElement[] row : stmt.rows())
            for (GridSqlElement el : row)
                GridSqlQuerySplitter.findParams(el, params, target, paramIdxs);

        return target;
    }

    /**
     * @param from From element.
     * @param tbls Tables.
     */
    public static void collectAllGridTablesInTarget(GridSqlElement from, final Set<GridSqlTable> tbls) {
        GridSqlQuerySplitter.findTablesInFrom(from, new IgnitePredicate<GridSqlElement>() {
            @Override public boolean apply(GridSqlElement el) {
                if (el instanceof GridSqlTable)
                    tbls.add((GridSqlTable)el);

                return false;
            }
        });
    }
}
