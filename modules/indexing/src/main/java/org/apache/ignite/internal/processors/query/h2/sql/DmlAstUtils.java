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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.processors.query.h2.dml.FastUpdateArguments;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2AbstractKeyValueRow;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2RowDescriptor;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.apache.ignite.internal.util.lang.IgnitePair;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgnitePredicate;
import org.h2.expression.Expression;
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
import org.h2.value.ValueTimestampUtc;
import org.jetbrains.annotations.Nullable;

import org.apache.ignite.internal.processors.query.h2.dml.FastUpdateArgument;

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
     * @param desc Row descriptor.
     * @return Subquery or pseudo-SELECT to evaluate inserted expressions.
     */
    public static GridSqlQuery selectForInsertOrMerge(GridSqlColumn[] cols, List<GridSqlElement[]> rows,
        GridSqlQuery subQry, GridH2RowDescriptor desc) {
        if (!F.isEmpty(rows)) {
            assert !F.isEmpty(cols);

            GridSqlSelect sel = new GridSqlSelect();

            GridSqlFunction from = new GridSqlFunction(GridSqlFunctionType.TABLE);

            sel.from(from);

            GridSqlArray[] args = new GridSqlArray[cols.length];

            for (int i = 0; i < cols.length; i++) {
                GridSqlArray arr = new GridSqlArray(rows.size());

                String colName = IgniteH2Indexing.escapeName(cols[i].columnName(), desc.quoteAllIdentifiers());

                GridSqlAlias alias = new GridSqlAlias(colName, arr);

                alias.resultType(cols[i].resultType());

                from.addChild(alias);

                args[i] = arr;

                GridSqlColumn newCol = new GridSqlColumn(null, from, colName, "TABLE." + colName);

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
     * @param keysParamIdx Index for .
     * @return SELECT statement.
     */
    public static GridSqlSelect selectForDelete(GridSqlDelete del, @Nullable Integer keysParamIdx) {
        GridSqlSelect mapQry = new GridSqlSelect();

        mapQry.from(del.from());

        Set<GridSqlTable> tbls = new HashSet<>();

        collectAllGridTablesInTarget(del.from(), tbls);

        assert tbls.size() == 1 : "Failed to determine target table for DELETE";

        GridSqlTable tbl = tbls.iterator().next();

        GridH2Table gridTbl = tbl.dataTable();

        assert gridTbl != null : "Failed to determine target grid table for DELETE";

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

    /**
     * @param update UPDATE statement.
     * @return {@code null} if given statement directly updates {@code _val} column with a literal or param value
     * and filters by single non expression key (and, optionally,  by single non expression value).
     */
    public static FastUpdateArguments getFastUpdateArgs(GridSqlUpdate update) {
        IgnitePair<GridSqlElement> filter = findKeyValueEqualityCondition(update.where());

        if (filter == null)
            return null;

        if (update.cols().size() != 1 ||
            !IgniteH2Indexing.VAL_FIELD_NAME.equalsIgnoreCase(update.cols().get(0).columnName()))
            return null;

        GridSqlElement set = update.set().get(update.cols().get(0).columnName());

        if (!(set instanceof GridSqlConst || set instanceof GridSqlParameter))
            return null;

        return new FastUpdateArguments(operandForElement(filter.getKey()), operandForElement(filter.getValue()),
            operandForElement(set));
    }

    /**
     * Create operand based on exact type of SQL element.
     *
     * @param el element.
     * @return Operand.
     */
    private static FastUpdateArgument operandForElement(GridSqlElement el) {
        assert el == null ^ (el instanceof GridSqlConst || el instanceof GridSqlParameter);

        if (el == null)
            return FastUpdateArguments.NULL_ARGUMENT;

        if (el instanceof GridSqlConst)
            return new ValueArgument(((GridSqlConst)el).value().getObject());
        else
            return new ParamArgument(((GridSqlParameter)el).index());
    }

    /**
     * @param del DELETE statement.
     * @return {@code true} if given statement filters by single non expression key.
     */
    public static FastUpdateArguments getFastDeleteArgs(GridSqlDelete del) {
        IgnitePair<GridSqlElement> filter = findKeyValueEqualityCondition(del.where());

        if (filter == null)
            return null;

        return new FastUpdateArguments(operandForElement(filter.getKey()), operandForElement(filter.getValue()),
            FastUpdateArguments.NULL_ARGUMENT);
    }

    /**
     * @param where Element to test.
     * @return Whether given element corresponds to {@code WHERE _key = ?}, and key is a literal expressed
     * in query or a query param.
     */
    private static IgnitePair<GridSqlElement> findKeyValueEqualityCondition(GridSqlElement where) {
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
            colName.equals(((GridSqlColumn) left).columnName()) &&
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
    public static GridSqlSelect selectForUpdate(GridSqlUpdate update, @Nullable Integer keysParamIdx) {
        GridSqlSelect mapQry = new GridSqlSelect();

        mapQry.from(update.target());

        Set<GridSqlTable> tbls = new HashSet<>();

        collectAllGridTablesInTarget(update.target(), tbls);

        assert tbls.size() == 1 : "Failed to determine target table for UPDATE";

        GridSqlTable tbl = tbls.iterator().next();

        GridH2Table gridTbl = tbl.dataTable();

        assert gridTbl != null : "Failed to determine target grid table for UPDATE";

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
            GridSqlAlias alias = new GridSqlAlias(newColName, elementOrDefault(update.set().get(c.columnName()), c), true);
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
        else if (dt.type == Value.TIMESTAMP_UTC)
            dfltVal = ValueTimestampUtc.fromMillis(U.currentTimeMillis());
        else if (dt.type == Value.TIME)
            dfltVal = ValueTime.fromNanos(0);
        else if (dt.type == Value.DATE)
            dfltVal = ValueDate.fromMillis(U.currentTimeMillis());
        else
            dfltVal = ValueString.get("").convertTo(type);

        return new GridSqlConst(dfltVal);
    }

    /**
     * Append additional condition to WHERE for it to select only specific keys.
     *
     * @param where Initial condition.
     * @param keyCol Column to base the new condition on.
     * @return New condition.
     */
    private static GridSqlElement injectKeysFilterParam(GridSqlElement where, GridSqlColumn keyCol, int paramIdx) {
        // Yes, we need a subquery for "WHERE _key IN ?" to work with param being an array without dirty query rewriting.
        GridSqlSelect sel = new GridSqlSelect();

        GridSqlFunction from = new GridSqlFunction(GridSqlFunctionType.TABLE);

        sel.from(from);

        GridSqlColumn col = new GridSqlColumn(null, from, "_IGNITE_ERR_KEYS", "TABLE._IGNITE_ERR_KEYS");

        sel.addColumn(col, true);

        GridSqlAlias alias = new GridSqlAlias("_IGNITE_ERR_KEYS", new GridSqlParameter(paramIdx));

        alias.resultType(keyCol.resultType());

        from.addChild(alias);

        GridSqlElement e = new GridSqlOperation(GridSqlOperationType.IN, keyCol, new GridSqlSubquery(sel));

        if (where == null)
            return e;
        else
            return new GridSqlOperation(GridSqlOperationType.AND, where, e);
    }

    /**
     * @param qry Select.
     * @param params Parameters.
     * @param target Extracted parameters.
     * @param paramIdxs Parameter indexes.
     * @return Extracted parameters list.
     */
    private static List<Object> findParams(GridSqlQuery qry, Object[] params, ArrayList<Object> target,
                                           IntArray paramIdxs) {
        if (qry instanceof GridSqlSelect)
            return findParams((GridSqlSelect)qry, params, target, paramIdxs);

        GridSqlUnion union = (GridSqlUnion)qry;

        findParams(union.left(), params, target, paramIdxs);
        findParams(union.right(), params, target, paramIdxs);

        findParams(qry.limit(), params, target, paramIdxs);
        findParams(qry.offset(), params, target, paramIdxs);

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

        for (GridSqlElement el : qry.columns(false))
            findParams(el, params, target, paramIdxs);

        findParams(qry.from(), params, target, paramIdxs);
        findParams(qry.where(), params, target, paramIdxs);

        // Don't search in GROUP BY and HAVING since they expected to be in select list.

        findParams(qry.limit(), params, target, paramIdxs);
        findParams(qry.offset(), params, target, paramIdxs);

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
            findParams(((GridSqlSubquery)el).select(), params, target, paramIdxs);
        else
            for (GridSqlElement child : el)
                findParams(child, params, target, paramIdxs);
    }

    /**
     * Processes all the tables and subqueries using the given closure.
     *
     * @param from FROM element.
     * @param c Closure each found table and subquery will be passed to. If returns {@code true} the we need to stop.
     * @return {@code true} If we have found.
     */
    private static boolean findTablesInFrom(GridSqlElement from, IgnitePredicate<GridSqlElement> c) {
        if (from == null)
            return false;

        if (from instanceof GridSqlTable || from instanceof GridSqlSubquery)
            return c.apply(from);

        if (from instanceof GridSqlJoin) {
            // Left and right.
            if (findTablesInFrom(from.child(0), c))
                return true;

            if (findTablesInFrom(from.child(1), c))
                return true;

            // We don't process ON condition because it is not a joining part of from here.
            return false;
        }
        else if (from instanceof GridSqlAlias)
            return findTablesInFrom(from.child(), c);
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

    /** Simple constant value based operand. */
    private final static class ValueArgument implements FastUpdateArgument {
        /** Value to return. */
        private final Object val;

        /** */
        private ValueArgument(Object val) {
            this.val = val;
        }

        /** {@inheritDoc} */
        @Override public Object apply(Object[] arg) throws IgniteCheckedException {
            return val;
        }
    }

    /** Simple constant value based operand. */
    private final static class ParamArgument implements FastUpdateArgument {
        /** Value to return. */
        private final int paramIdx;

        /** */
        private ParamArgument(int paramIdx) {
            assert paramIdx >= 0;

            this.paramIdx = paramIdx;
        }

        /** {@inheritDoc} */
        @Override public Object apply(Object[] arg) throws IgniteCheckedException {
            assert arg.length > paramIdx;

            return arg[paramIdx];
        }
    }
}
