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

import org.apache.ignite.*;
import org.h2.command.dml.*;
import org.h2.engine.*;
import org.h2.expression.*;
import org.h2.jdbc.*;
import org.h2.result.*;
import org.h2.table.*;
import org.jetbrains.annotations.*;

import java.lang.reflect.*;
import java.sql.*;
import java.util.*;
import java.util.Set;

import static org.apache.ignite.internal.processors.query.h2.sql.GridSqlOperationType.*;

/**
 * H2 Query parser.
 */
@SuppressWarnings("TypeMayBeWeakened")
public class GridSqlQueryParser {
    /** */
    private static final GridSqlOperationType[] OPERATION_OP_TYPES = new GridSqlOperationType[]{CONCAT, PLUS, MINUS, MULTIPLY, DIVIDE, null, MODULUS};

    /** */
    private static final GridSqlOperationType[] COMPARISON_TYPES = new GridSqlOperationType[]{EQUAL, BIGGER_EQUAL, BIGGER, SMALLER_EQUAL,
        SMALLER, NOT_EQUAL, IS_NULL, IS_NOT_NULL,
        null, null, null, SPATIAL_INTERSECTS /* 11 */, null, null, null, null, EQUAL_NULL_SAFE /* 16 */, null, null, null, null,
        NOT_EQUAL_NULL_SAFE /* 21 */};

    /** */
    private static final Getter<Select, Expression> CONDITION = getter(Select.class, "condition");

    /** */
    private static final Getter<Select, int[]> GROUP_INDEXES = getter(Select.class, "groupIndex");

    /** */
    private static final Getter<Operation, Integer> OPERATION_TYPE = getter(Operation.class, "opType");

    /** */
    private static final Getter<Operation, Expression> OPERATION_LEFT = getter(Operation.class, "left");

    /** */
    private static final Getter<Operation, Expression> OPERATION_RIGHT = getter(Operation.class, "right");

    /** */
    private static final Getter<Comparison, Integer> COMPARISON_TYPE = getter(Comparison.class, "compareType");

    /** */
    private static final Getter<Comparison, Expression> COMPARISON_LEFT = getter(Comparison.class, "left");

    /** */
    private static final Getter<Comparison, Expression> COMPARISON_RIGHT = getter(Comparison.class, "right");

    /** */
    private static final Getter<ConditionAndOr, Integer> ANDOR_TYPE = getter(ConditionAndOr.class, "andOrType");

    /** */
    private static final Getter<ConditionAndOr, Expression> ANDOR_LEFT = getter(ConditionAndOr.class, "left");

    /** */
    private static final Getter<ConditionAndOr, Expression> ANDOR_RIGHT = getter(ConditionAndOr.class, "right");

    /** */
    private static final Getter<TableView, Query> VIEW_QUERY = getter(TableView.class, "viewQuery");

    /** */
    private static final Getter<TableFilter, String> ALIAS = getter(TableFilter.class, "alias");

    /** */
    private static final Getter<Select, Integer> HAVING_INDEX = getter(Select.class, "havingIndex");

    /** */
    private static final Getter<ConditionIn, Expression> LEFT_CI = getter(ConditionIn.class, "left");

    /** */
    private static final Getter<ConditionIn, List<Expression>> VALUE_LIST_CI = getter(ConditionIn.class, "valueList");

    /** */
    private static final Getter<ConditionInConstantSet, Expression> LEFT_CICS =
        getter(ConditionInConstantSet.class, "left");

    /** */
    private static final Getter<ConditionInConstantSet, List<Expression>> VALUE_LIST_CICS =
        getter(ConditionInConstantSet.class, "valueList");

    /** */
    private static final Getter<ConditionInSelect, Expression> LEFT_CIS = getter(ConditionInSelect.class, "left");

    /** */
    private static final Getter<ConditionInSelect, Boolean> ALL = getter(ConditionInSelect.class, "all");

    /** */
    private static final Getter<ConditionInSelect, Integer> COMPARE_TYPE = getter(ConditionInSelect.class,
        "compareType");

    /** */
    private static final Getter<ConditionInSelect, Query> QUERY = getter(ConditionInSelect.class, "query");

    /** */
    private static final Getter<CompareLike, Expression> LEFT = getter(CompareLike.class, "left");

    /** */
    private static final Getter<CompareLike, Expression> RIGHT = getter(CompareLike.class, "right");

    /** */
    private static final Getter<CompareLike, Expression> ESCAPE = getter(CompareLike.class, "escape");

    /** */
    private static final Getter<CompareLike, Boolean> REGEXP_CL = getter(CompareLike.class, "regexp");

    /** */
    private static final Getter<Aggregate, Boolean> DISTINCT = getter(Aggregate.class, "distinct");

    /** */
    private static final Getter<Aggregate, Integer> TYPE = getter(Aggregate.class, "type");

    /** */
    private static final Getter<Aggregate, Expression> ON = getter(Aggregate.class, "on");

    /** */
    private static final Getter<RangeTable, Expression> RANGE_MIN = getter(RangeTable.class, "min");

    /** */
    private static final Getter<RangeTable, Expression> RANGE_MAX = getter(RangeTable.class, "max");

    /** */
    private final IdentityHashMap<Object, Object> h2ObjToGridObj = new IdentityHashMap<>();

    /**
     * @param conn Connection.
     * @param select Select query.
     * @return Parsed select query.
     */
    public static GridSqlSelect parse(Connection conn, String select) {
        Session ses = (Session)((JdbcConnection)conn).getSession();

        return new GridSqlQueryParser().parse((Select)ses.prepare(select));
    }

    /**
     * @param filter Filter.
     */
    private GridSqlElement parse(TableFilter filter) {
        GridSqlElement res = (GridSqlElement)h2ObjToGridObj.get(filter);

        if (res == null) {
            Table tbl = filter.getTable();

            if (tbl instanceof TableBase)
                res = new GridSqlTable(tbl.getSchema().getName(), tbl.getName());
            else if (tbl instanceof TableView) {
                Query qry = VIEW_QUERY.get((TableView)tbl);

                assert0(qry instanceof Select, qry);

                res = new GridSqlSubquery(parse((Select)qry));
            }
            else if (tbl instanceof RangeTable) {
                res = new GridSqlFunction("SYSTEM_RANGE");

                res.addChild(parseExpression(RANGE_MIN.get((RangeTable)tbl)));
                res.addChild(parseExpression(RANGE_MAX.get((RangeTable)tbl)));
            }
            else
                throw new IgniteException("Unsupported query: " + filter);

            String alias = ALIAS.get(filter);

            if (alias != null)
                res = new GridSqlAlias(alias, res, false);

            h2ObjToGridObj.put(filter, res);
        }

        return res;
    }

    /**
     * @param select Select.
     */
    public GridSqlSelect parse(Select select) {
        GridSqlSelect res = (GridSqlSelect)h2ObjToGridObj.get(select);

        if (res != null)
            return res;

        res = new GridSqlSelect();

        h2ObjToGridObj.put(select, res);

        res.distinct(select.isDistinct());

        Expression where = CONDITION.get(select);
        res.where(parseExpression(where));

        Set<TableFilter> allFilters = new HashSet<>(select.getTopFilters());

        GridSqlElement from = null;

        TableFilter filter = select.getTopTableFilter();
        do {
            assert0(filter != null, select);
            assert0(!filter.isJoinOuter(), select); // TODO 3
            assert0(!filter.isJoinOuterIndirect(), select);
            assert0(filter.getNestedJoin() == null, select);
            assert0(filter.getJoinCondition() == null, select); // TODO 1
            assert0(filter.getFilterCondition() == null, select); // TODO 2

            GridSqlElement gridFilter = parse(filter);

            from = from == null ? gridFilter : new GridSqlJoin(from, gridFilter);

            allFilters.remove(filter);

            filter = filter.getJoin();
        }
        while (filter != null);

        res.from(from);

        assert allFilters.isEmpty();

        ArrayList<Expression> expressions = select.getExpressions();

        for (Expression exp : expressions)
            res.addExpression(parseExpression(exp));

        int[] grpIdx = GROUP_INDEXES.get(select);

        if (grpIdx != null) {
            res.groupColumns(grpIdx);

            for (int idx : grpIdx)
                res.addGroupExpression(parseExpression(expressions.get(idx)));
        }

        int havingIdx = HAVING_INDEX.get(select);

        if (havingIdx >= 0) {
            res.havingColumn(havingIdx);

            res.having(parseExpression(expressions.get(havingIdx)));
        }

        for (int i = 0; i < select.getColumnCount(); i++)
            res.addSelectExpression(parseExpression(expressions.get(i)));

        SortOrder sortOrder = select.getSortOrder();

        if (sortOrder != null) {
            int[] indexes = sortOrder.getQueryColumnIndexes();
            int[] sortTypes = sortOrder.getSortTypes();

            for (int i = 0; i < indexes.length; i++) {
                int colIdx = indexes[i];
                int type = sortTypes[i];

                res.addSort(parseExpression(expressions.get(colIdx)), new GridSqlSortColumn(colIdx,
                    (type & SortOrder.DESCENDING) == 0,
                    (type & SortOrder.NULLS_FIRST) != 0,
                    (type & SortOrder.NULLS_LAST) != 0));
            }
        }

        res.limit(parseExpression(select.getLimit()));
        res.offset(parseExpression(select.getOffset()));

        return res;
    }

    /**
     * @param expression Expression.
     */
    private GridSqlElement parseExpression(@Nullable Expression expression) {
        if (expression == null)
            return null;

        GridSqlElement res = (GridSqlElement)h2ObjToGridObj.get(expression);

        if (res == null) {
            res = parseExpression0(expression);

            h2ObjToGridObj.put(expression, res);
        }

        return res;
    }

    /**
     * @param expression Expression.
     */
    private GridSqlElement parseExpression0(Expression expression) {
        if (expression instanceof ExpressionColumn) {
            TableFilter tblFilter = ((ExpressionColumn)expression).getTableFilter();

            GridSqlElement gridTblFilter = parse(tblFilter);

            return new GridSqlColumn(gridTblFilter, expression.getColumnName(), expression.getSQL());
        }

        if (expression instanceof Alias)
            return new GridSqlAlias(expression.getAlias(), parseExpression(expression.getNonAliasExpression()), true);

        if (expression instanceof ValueExpression)
            return new GridSqlConst(expression.getValue(null));

        if (expression instanceof Operation) {
            Operation operation = (Operation)expression;

            Integer type = OPERATION_TYPE.get(operation);

            if (type == Operation.NEGATE) {
                assert OPERATION_RIGHT.get(operation) == null;

                return new GridSqlOperation(GridSqlOperationType.NEGATE, parseExpression(OPERATION_LEFT.get(operation)));
            }

            return new GridSqlOperation(OPERATION_OP_TYPES[type],
                parseExpression(OPERATION_LEFT.get(operation)),
                parseExpression(OPERATION_RIGHT.get(operation)));
        }

        if (expression instanceof Comparison) {
            Comparison cmp = (Comparison)expression;

            GridSqlOperationType opType = COMPARISON_TYPES[COMPARISON_TYPE.get(cmp)];

            assert opType != null : COMPARISON_TYPE.get(cmp);

            GridSqlElement left = parseExpression(COMPARISON_LEFT.get(cmp));

            if (opType.childrenCount() == 1)
                return new GridSqlOperation(opType, left);

            GridSqlElement right = parseExpression(COMPARISON_RIGHT.get(cmp));

            return new GridSqlOperation(opType, left, right);
        }

        if (expression instanceof ConditionNot)
            return new GridSqlOperation(NOT, parseExpression(expression.getNotIfPossible(null)));

        if (expression instanceof ConditionAndOr) {
            ConditionAndOr andOr = (ConditionAndOr)expression;

            int type = ANDOR_TYPE.get(andOr);

            assert type == ConditionAndOr.AND || type == ConditionAndOr.OR;

            return new GridSqlOperation(type == ConditionAndOr.AND ? AND : OR,
                parseExpression(ANDOR_LEFT.get(andOr)), parseExpression(ANDOR_RIGHT.get(andOr)));
        }

        if (expression instanceof Subquery) {
            Query qry = ((Subquery)expression).getQuery();

            assert0(qry instanceof Select, expression);

            return new GridSqlSubquery(parse((Select) qry));
        }

        if (expression instanceof ConditionIn) {
            GridSqlOperation res = new GridSqlOperation(IN);

            res.addChild(parseExpression(LEFT_CI.get((ConditionIn) expression)));

            List<Expression> vals = VALUE_LIST_CI.get((ConditionIn)expression);

            for (Expression val : vals)
                res.addChild(parseExpression(val));

            return res;
        }

        if (expression instanceof ConditionInConstantSet) {
            GridSqlOperation res = new GridSqlOperation(IN);

            res.addChild(parseExpression(LEFT_CICS.get((ConditionInConstantSet) expression)));

            List<Expression> vals = VALUE_LIST_CICS.get((ConditionInConstantSet)expression);

            for (Expression val : vals)
                res.addChild(parseExpression(val));

            return res;
        }

        if (expression instanceof ConditionInSelect) {
            GridSqlOperation res = new GridSqlOperation(IN);

            boolean all = ALL.get((ConditionInSelect)expression);
            int compareType = COMPARE_TYPE.get((ConditionInSelect)expression);

            assert0(!all, expression);
            assert0(compareType == Comparison.EQUAL, expression);

            res.addChild(parseExpression(LEFT_CIS.get((ConditionInSelect) expression)));

            Query qry = QUERY.get((ConditionInSelect)expression);

            assert0(qry instanceof Select, qry);

            res.addChild(new GridSqlSubquery(parse((Select) qry)));

            return res;
        }

        if (expression instanceof CompareLike) {
            assert0(ESCAPE.get((CompareLike)expression) == null, expression);

            boolean regexp = REGEXP_CL.get((CompareLike)expression);

            return new GridSqlOperation(regexp ? REGEXP : LIKE, parseExpression(LEFT.get((CompareLike) expression)),
                parseExpression(RIGHT.get((CompareLike) expression)));
        }

        if (expression instanceof Function) {
            Function f = (Function)expression;

            GridSqlFunction res = new GridSqlFunction(f.getName());

            for (Expression arg : f.getArgs())
                res.addChild(parseExpression(arg));

            if (f.getFunctionType() == Function.CAST || f.getFunctionType() == Function.CONVERT)
                res.setCastType(new Column(null, f.getType(), f.getPrecision(), f.getScale(), f.getDisplaySize())
                    .getCreateSQL());

            return res;
        }

        if (expression instanceof JavaFunction) {
            JavaFunction f = (JavaFunction)expression;

            GridSqlFunction res = new GridSqlFunction(f.getName());

            for (Expression arg : f.getArgs())
                res.addChild(parseExpression(arg));

            return res;
        }

        if (expression instanceof Parameter)
            return new GridSqlParameter(((Parameter)expression).getIndex());

        if (expression instanceof Aggregate) {
            GridSqlAggregateFunction res = new GridSqlAggregateFunction(DISTINCT.get((Aggregate)expression),
                TYPE.get((Aggregate)expression));

            Expression on = ON.get((Aggregate)expression);

            if (on != null)
                res.addChild(parseExpression(on));

            return res;
        }

        throw new IgniteException("Unsupported expression: " + expression + " [type=" +
            expression.getClass().getSimpleName() + ']');
    }

    /**
     * @param cond Condition.
     * @param o Object.
     */
    private static void assert0(boolean cond, Object o) {
        if (!cond)
            throw new IgniteException("Unsupported query: " + o);
    }

    /**
     * @param cls Class.
     * @param fldName Fld name.
     */
    private static <T, R> Getter<T, R> getter(Class<T> cls, String fldName) {
        Field field;

        try {
            field = cls.getDeclaredField(fldName);
        }
        catch (NoSuchFieldException e) {
            throw new RuntimeException(e);
        }

        field.setAccessible(true);

        return new Getter<>(field);
    }

    /**
     * Field getter.
     */
    @SuppressWarnings("unchecked")
    private static class Getter<T, R> {
        /** */
        private final Field fld;

        /**
         * @param fld Fld.
         */
        private Getter(Field fld) {
            this.fld = fld;
        }

        /**
         * @param obj Object.
         * @return Result.
         */
        public R get(T obj) {
            try {
                return (R)fld.get(obj);
            }
            catch (IllegalAccessException e) {
                throw new IgniteException(e);
            }
        }
    }
}
