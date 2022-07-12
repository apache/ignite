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

package org.apache.ignite.internal.processors.query.h2.dml;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.query.GridQueryRowDescriptor;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlAlias;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlArray;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlAst;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlColumn;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlConst;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlDelete;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlElement;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlFunction;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlFunctionType;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlJoin;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlKeyword;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlOperation;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlOperationType;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlParameter;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlQuery;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlSelect;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlSubquery;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlTable;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlType;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlUnion;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlUpdate;
import org.apache.ignite.internal.util.lang.IgnitePair;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgnitePredicate;
import org.h2.command.Parser;
import org.h2.expression.Expression;
import org.h2.index.Index;
import org.h2.table.Column;
import org.h2.table.Table;
import org.h2.util.IntArray;
import org.h2.value.DataType;
import org.h2.value.Value;
import org.h2.value.ValueDate;
import org.h2.value.ValueInt;
import org.h2.value.ValueString;
import org.h2.value.ValueTime;
import org.h2.value.ValueTimestamp;
import org.jetbrains.annotations.Nullable;

/**
 * AST utils for DML
 */
public final class DmlAstUtils {
    /**
     * Empty ctor to prevent initialization.
     */
    private DmlAstUtils() {
        // No-op.
    }

    /**
     * Create SELECT on which subsequent INSERT or MERGE will be based.
     *
     * @param cols Columns to insert values into.
     * @param rows Rows to create pseudo-SELECT upon.
     * @param subQry Subquery to use rather than rows.
     * @return Subquery or pseudo-SELECT to evaluate inserted expressions, or {@code null} no query needs to be run.
     */
    public static GridSqlQuery selectForInsertOrMerge(GridSqlColumn[] cols, List<GridSqlElement[]> rows,
        GridSqlQuery subQry) {
        if (!F.isEmpty(rows)) {
            assert !F.isEmpty(cols);

            GridSqlSelect sel = new GridSqlSelect();

            GridSqlFunction from = new GridSqlFunction(GridSqlFunctionType.TABLE);

            sel.from(from);

            GridSqlArray[] args = new GridSqlArray[cols.length];

            for (int i = 0; i < cols.length; i++) {
                GridSqlArray arr = new GridSqlArray(rows.size());

                String colName = cols[i].columnName();

                GridSqlAlias alias = new GridSqlAlias(colName, arr);

                alias.resultType(cols[i].resultType());

                from.addChild(alias);

                args[i] = arr;

                GridSqlColumn newCol = new GridSqlColumn(null, from, null, "TABLE", colName);

                newCol.resultType(cols[i].resultType());

                sel.addColumn(newCol, true);
            }

            for (GridSqlElement[] row : rows) {
                assert cols.length == row.length;

                for (int i = 0; i < row.length; i++)
                    args[i].addChild(row[i]);
            }

            return sel;
        }
        else {
            assert subQry != null;

            return subQry;
        }
    }

    /**
     * Generate SQL SELECT based on DELETE's WHERE, LIMIT, etc.
     *
     * @param del Delete statement.
     * @return SELECT statement.
     */
    public static GridSqlSelect selectForDelete(GridSqlDelete del) {
        GridSqlSelect mapQry = new GridSqlSelect();

        mapQry.from(del.from());

        Set<GridSqlTable> tbls = new HashSet<>();

        collectAllGridTablesInTarget(del.from(), tbls);

        assert tbls.size() == 1 : "Failed to determine target table for DELETE";

        GridSqlTable tbl = tbls.iterator().next();

        GridH2Table gridTbl = tbl.dataTable();

        assert gridTbl != null : "Failed to determine target grid table for DELETE";

        Column h2KeyCol = gridTbl.getColumn(QueryUtils.KEY_COL);

        Column h2ValCol = gridTbl.getColumn(QueryUtils.VAL_COL);

        GridSqlColumn keyCol = new GridSqlColumn(h2KeyCol, tbl, h2KeyCol.getName());
        keyCol.resultType(GridSqlType.fromColumn(h2KeyCol));

        GridSqlColumn valCol = new GridSqlColumn(h2ValCol, tbl, h2ValCol.getName());
        valCol.resultType(GridSqlType.fromColumn(h2ValCol));

        mapQry.addColumn(keyCol, true);
        mapQry.addColumn(valCol, true);

        GridSqlElement where = del.where();

        mapQry.where(where);
        mapQry.limit(del.limit());

        return mapQry;
    }

    /**
     * @param update UPDATE statement.
     * @return {@code null} if given statement directly updates {@code _val} column with a literal or param value
     * and filters by single non expression key (and, optionally,  by single non expression value).
     */
    public static FastUpdate getFastUpdateArgs(GridSqlUpdate update) {
        IgnitePair<GridSqlElement> filter = findKeyValueEqualityCondition(update.where());

        if (filter == null)
            return null;

        if (update.cols().size() != 1)
            return null;

        Table tbl = update.cols().get(0).column().getTable();
        if (!(tbl instanceof GridH2Table))
            return null;

        GridQueryRowDescriptor desc = ((GridH2Table)tbl).rowDescriptor();
        if (!desc.isValueColumn(update.cols().get(0).column().getColumnId()))
            return null;

        GridSqlElement set = update.set().get(update.cols().get(0).columnName());

        if (!(set instanceof GridSqlConst || set instanceof GridSqlParameter))
            return null;

        return FastUpdate.create(filter.getKey(), filter.getValue(), set);
    }

    /**
     * @param del DELETE statement.
     * @return {@code true} if given statement filters by single non expression key.
     */
    public static FastUpdate getFastDeleteArgs(GridSqlDelete del) {
        IgnitePair<GridSqlElement> filter = findKeyValueEqualityCondition(del.where());

        if (filter == null)
            return null;

        return FastUpdate.create(filter.getKey(), filter.getValue(), null);
    }

    /**
     * @param where Element to test.
     * @return Whether given element corresponds to {@code WHERE _key = ?}, and key is a literal expressed
     * in query or a query param.
     */
    @SuppressWarnings("RedundantCast")
    private static IgnitePair<GridSqlElement> findKeyValueEqualityCondition(GridSqlElement where) {
        if (!(where instanceof GridSqlOperation))
            return null;

        GridSqlOperation whereOp = (GridSqlOperation)where;

        // Does this WHERE limit only by _key?
        if (isKeyEqualityCondition(whereOp))
            return new IgnitePair<>((GridSqlElement)whereOp.child(1), null);

        // Or maybe it limits both by _key and _val?
        if (whereOp.operationType() != GridSqlOperationType.AND)
            return null;

        GridSqlElement left = whereOp.child(0);

        GridSqlElement right = whereOp.child(1);

        if (!(left instanceof GridSqlOperation && right instanceof GridSqlOperation))
            return null;

        GridSqlOperation leftOp = (GridSqlOperation)left;

        GridSqlOperation rightOp = (GridSqlOperation)right;

        if (isKeyEqualityCondition(leftOp)) { // _key = ? and _val = ?
            if (!isValueEqualityCondition(rightOp))
                return null;

            return new IgnitePair<>((GridSqlElement)leftOp.child(1), (GridSqlElement)rightOp.child(1));
        }
        else if (isKeyEqualityCondition(rightOp)) { // _val = ? and _key = ?
            if (!isValueEqualityCondition(leftOp))
                return null;

            return new IgnitePair<>((GridSqlElement)rightOp.child(1), (GridSqlElement)leftOp.child(1));
        }
        else // Neither
            return null;
    }

    /**
     * @param op Operation.
     * @param key true - check for key equality condition,
     *            otherwise check for value equality condition
     * @return Whether this condition is of form {@code colName} = ?
     */
    private static boolean isEqualityCondition(GridSqlOperation op, boolean key) {
        if (op.operationType() != GridSqlOperationType.EQUAL)
            return false;

        GridSqlElement left = op.child(0);
        GridSqlElement right = op.child(1);

        if (!(left instanceof GridSqlColumn))
            return false;

        GridSqlColumn column = (GridSqlColumn)left;
        if (!(column.column().getTable() instanceof GridH2Table))
            return false;

        GridQueryRowDescriptor desc = ((GridH2Table)column.column().getTable()).rowDescriptor();

        return (key ? desc.isKeyColumn(column.column().getColumnId()) :
                       desc.isValueColumn(column.column().getColumnId())) &&
                (right instanceof GridSqlConst || right instanceof GridSqlParameter);
    }

    /**
     * @param op Operation.
     * @return Whether this condition is of form _key = ?
     */
    private static boolean isKeyEqualityCondition(GridSqlOperation op) {
        return isEqualityCondition(op, true);
    }

    /**
     * @param op Operation.
     * @return Whether this condition is of form _val = ?
     */
    private static boolean isValueEqualityCondition(GridSqlOperation op) {
        return isEqualityCondition(op, false);
    }

    /**
     * Generate SQL SELECT based on UPDATE's WHERE, LIMIT, etc.
     *
     * @param update Update statement.
     * @return SELECT statement.
     */
    public static GridSqlSelect selectForUpdate(GridSqlUpdate update) {
        GridSqlSelect mapQry = new GridSqlSelect();

        mapQry.from(update.target());

        Set<GridSqlTable> tbls = new HashSet<>();

        collectAllGridTablesInTarget(update.target(), tbls);

        assert tbls.size() == 1 : "Failed to determine target table for UPDATE";

        GridSqlTable tbl = tbls.iterator().next();

        GridH2Table gridTbl = tbl.dataTable();

        assert gridTbl != null : "Failed to determine target grid table for UPDATE";

        Column h2KeyCol = gridTbl.getColumn(QueryUtils.KEY_COL);

        Column h2ValCol = gridTbl.getColumn(QueryUtils.VAL_COL);

        GridSqlColumn keyCol = new GridSqlColumn(h2KeyCol, tbl, h2KeyCol.getName());
        keyCol.resultType(GridSqlType.fromColumn(h2KeyCol));

        GridSqlColumn valCol = new GridSqlColumn(h2ValCol, tbl, h2ValCol.getName());
        valCol.resultType(GridSqlType.fromColumn(h2ValCol));

        mapQry.addColumn(keyCol, true);
        mapQry.addColumn(valCol, true);

        for (GridSqlColumn c : update.cols()) {
            String newColName = Parser.quoteIdentifier("_upd_" + c.columnName());

            // We have to use aliases to cover cases when the user
            // wants to update _val field directly (if it's a literal)
            GridSqlAlias alias = new GridSqlAlias(newColName, elementOrDefault(update.set().get(c.columnName()), c), true);
            alias.resultType(c.resultType());
            mapQry.addColumn(alias, true);
        }

        GridSqlElement where = update.where();

        // On no MVCC mode we cannot use lazy mode when UPDATE query contains index with updated columns
        // and that index may be chosen to scan by WHERE condition
        // because in this case any rows update may be updated several times.
        // e.g. in the cases below we cannot use lazy mode:
        //
        // 1. CREATE INDEX idx on test(val)
        //    UPDATE test SET val = val + 1 WHERE val >= ?
        //
        // 2. CREATE INDEX idx on test(val0, val1)
        //    UPDATE test SET val1 = val1 + 1 WHERE val0 >= ?
        mapQry.canBeLazy(!isIndexWithUpdateColumnsMayBeUsed(
            gridTbl,
            update.cols().stream()
                .map(GridSqlColumn::column)
                .collect(Collectors.toSet()),
            extractColumns(gridTbl, where)));

        mapQry.where(where);
        mapQry.limit(update.limit());

        return mapQry;
    }

    /**
     * @return Set columns of the specified table that are used in expression.
     */
    private static Set<Column> extractColumns(GridH2Table tbl, GridSqlAst expr) {
        if (expr == null)
            return Collections.emptySet();

        if (expr instanceof GridSqlColumn && ((GridSqlColumn)expr).column().getTable().equals(tbl))
            return Collections.singleton(((GridSqlColumn)expr).column());

        HashSet<Column> set = new HashSet<>();

        for (int i = 0; i < expr.size(); ++i)
            set.addAll(extractColumns(tbl, expr.child(i)));

        return set;
    }

    /**
     * @return {@code true} if the index contains update columns may be potentially used for scan.
     */
    private static boolean isIndexWithUpdateColumnsMayBeUsed(
        GridH2Table tbl,
        Set<Column> updateCols,
        Set<Column> whereCols) {
        if (F.isEmpty(whereCols))
            return false;

        if (updateCols.size() == 1 && whereCols.size() == 1
            && tbl.rowDescriptor().isValueColumn(F.first(updateCols).getColumnId())
            && tbl.rowDescriptor().isValueColumn(F.first(whereCols).getColumnId()))
            return true;

        for (Index idx : tbl.getIndexes()) {
            if (idx.equals(tbl.getPrimaryKey()) || whereCols.contains(idx.getColumns()[0])) {
                for (Column idxCol : idx.getColumns()) {
                    if (updateCols.contains(idxCol))
                        return true;
                }
            }
        }

        return false;
    }

    /**
     * Do what we can to compute default value for this column (mimics H2 behavior).
     * @see Table#getDefaultValue
     * @see Column#validateConvertUpdateSequence
     * @param el SQL element.
     * @param col Column.
     * @return {@link GridSqlConst#NULL}, if {@code el} is null, or {@code el} if
     * it's not {@link GridSqlKeyword#DEFAULT}, or computed default value.
     */
    private static GridSqlElement elementOrDefault(GridSqlElement el, GridSqlColumn col) {
        if (el == null)
            return GridSqlConst.NULL;

        if (el != GridSqlKeyword.DEFAULT)
            return el;

        Column h2Col = col.column();

        Expression dfltExpr = h2Col.getDefaultExpression();

        Value dfltVal;

        try {
            dfltVal = dfltExpr != null ? dfltExpr.getValue(null) : null;
        }
        catch (Exception ignored) {
            throw new IgniteSQLException("Failed to evaluate default value for a column " + col.columnName());
        }

        if (dfltVal != null)
            return new GridSqlConst(dfltVal);

        int type = h2Col.getType();

        DataType dt = DataType.getDataType(type);

        if (dt.decimal)
            dfltVal = ValueInt.get(0).convertTo(type);
        else if (dt.type == Value.TIMESTAMP)
            dfltVal = ValueTimestamp.fromMillis(U.currentTimeMillis());
        else if (dt.type == Value.TIME)
            dfltVal = ValueTime.fromNanos(0);
        else if (dt.type == Value.DATE)
            dfltVal = ValueDate.fromMillis(U.currentTimeMillis());
        else
            dfltVal = ValueString.get("").convertTo(type);

        return new GridSqlConst(dfltVal);
    }

    /**
     * @param qry Select.
     * @param params Parameters.
     * @param target Extracted parameters.
     * @param paramIdxs Parameter indexes.
     * @return Extracted parameters list.
     */
    @SuppressWarnings("unused")
    private static List<Object> findParams(GridSqlQuery qry, Object[] params, ArrayList<Object> target,
        IntArray paramIdxs) {
        if (qry instanceof GridSqlSelect)
            return findParams((GridSqlSelect)qry, params, target, paramIdxs);

        GridSqlUnion union = (GridSqlUnion)qry;

        findParams(union.left(), params, target, paramIdxs);
        findParams(union.right(), params, target, paramIdxs);

        findParams((GridSqlElement)qry.limit(), params, target, paramIdxs);
        findParams((GridSqlElement)qry.offset(), params, target, paramIdxs);

        return target;
    }

    /**
     * @param qry Select.
     * @param params Parameters.
     * @param target Extracted parameters.
     * @param paramIdxs Parameter indexes.
     * @return Extracted parameters list.
     */
    private static List<Object> findParams(GridSqlSelect qry, Object[] params, ArrayList<Object> target,
        IntArray paramIdxs) {
        if (params.length == 0)
            return target;

        for (GridSqlAst el : qry.columns(false))
            findParams((GridSqlElement)el, params, target, paramIdxs);

        findParams((GridSqlElement)qry.from(), params, target, paramIdxs);
        findParams((GridSqlElement)qry.where(), params, target, paramIdxs);

        // Don't search in GROUP BY and HAVING since they expected to be in select list.

        findParams((GridSqlElement)qry.limit(), params, target, paramIdxs);
        findParams((GridSqlElement)qry.offset(), params, target, paramIdxs);

        return target;
    }

    /**
     * @param el Element.
     * @param params Parameters.
     * @param target Extracted parameters.
     * @param paramIdxs Parameter indexes.
     */
    private static void findParams(@Nullable GridSqlElement el, Object[] params, ArrayList<Object> target,
        IntArray paramIdxs) {
        if (el == null)
            return;

        if (el instanceof GridSqlParameter) {
            // H2 Supports queries like "select ?5" but first 4 non-existing parameters are need to be set to any value.
            // Here we will set them to NULL.
            final int idx = ((GridSqlParameter)el).index();

            while (target.size() < idx)
                target.add(null);

            if (params.length <= idx)
                throw new IgniteException("Invalid number of query parameters. " +
                    "Cannot find " + idx + " parameter.");

            Object param = params[idx];

            if (idx == target.size())
                target.add(param);
            else
                target.set(idx, param);

            paramIdxs.add(idx);
        }
        else if (el instanceof GridSqlSubquery)
            findParams(((GridSqlSubquery)el).subquery(), params, target, paramIdxs);
        else
            for (int i = 0; i < el.size(); i++)
                findParams((GridSqlElement)el.child(i), params, target, paramIdxs);
    }

    /**
     * Processes all the tables and subqueries using the given closure.
     *
     * @param from FROM element.
     * @param c Closure each found table and subquery will be passed to. If returns {@code true} the we need to stop.
     * @return {@code true} If we have found.
     */
    @SuppressWarnings({"RedundantCast", "RedundantIfStatement"})
    private static boolean findTablesInFrom(GridSqlElement from, IgnitePredicate<GridSqlElement> c) {
        if (from == null)
            return false;

        if (from instanceof GridSqlTable || from instanceof GridSqlSubquery)
            return c.apply(from);

        if (from instanceof GridSqlJoin) {
            // Left and right.
            if (findTablesInFrom((GridSqlElement)from.child(0), c))
                return true;

            if (findTablesInFrom((GridSqlElement)from.child(1), c))
                return true;

            // We don't process ON condition because it is not a joining part of from here.
            return false;
        }
        else if (from instanceof GridSqlAlias)
            return findTablesInFrom((GridSqlElement)from.child(), c);
        else if (from instanceof GridSqlFunction)
            return false;

        throw new IllegalStateException(from.getClass().getName() + " : " + from.getSQL());
    }

    /**
     * @param from From element.
     * @param tbls Tables.
     */
    public static void collectAllGridTablesInTarget(GridSqlElement from, final Set<GridSqlTable> tbls) {
        findTablesInFrom(from, new IgnitePredicate<GridSqlElement>() {
            @Override public boolean apply(GridSqlElement el) {
                if (el instanceof GridSqlTable)
                    tbls.add((GridSqlTable)el);

                return false;
            }
        });
    }

    /**
     * @param target Expression to extract the table from.
     * @return Back end table for this element.
     */
    public static GridSqlTable gridTableForElement(GridSqlElement target) {
        Set<GridSqlTable> tbls = new HashSet<>();

        collectAllGridTablesInTarget(target, tbls);

        if (tbls.size() != 1)
            throw new IgniteSQLException("Failed to determine target table", IgniteQueryErrorCode.TABLE_NOT_FOUND);

        return tbls.iterator().next();
    }
}
