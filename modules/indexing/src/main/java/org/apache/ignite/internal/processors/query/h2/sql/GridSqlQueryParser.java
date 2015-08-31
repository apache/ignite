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

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;
import org.apache.ignite.IgniteException;
import org.h2.command.Command;
import org.h2.command.Prepared;
import org.h2.command.dml.Explain;
import org.h2.command.dml.Query;
import org.h2.command.dml.Select;
import org.h2.command.dml.SelectUnion;
import org.h2.engine.FunctionAlias;
import org.h2.expression.Aggregate;
import org.h2.expression.Alias;
import org.h2.expression.CompareLike;
import org.h2.expression.Comparison;
import org.h2.expression.ConditionAndOr;
import org.h2.expression.ConditionIn;
import org.h2.expression.ConditionInConstantSet;
import org.h2.expression.ConditionInSelect;
import org.h2.expression.ConditionNot;
import org.h2.expression.Expression;
import org.h2.expression.ExpressionColumn;
import org.h2.expression.Function;
import org.h2.expression.JavaFunction;
import org.h2.expression.Operation;
import org.h2.expression.Parameter;
import org.h2.expression.Subquery;
import org.h2.expression.ValueExpression;
import org.h2.jdbc.JdbcPreparedStatement;
import org.h2.result.SortOrder;
import org.h2.table.Column;
import org.h2.table.FunctionTable;
import org.h2.table.RangeTable;
import org.h2.table.Table;
import org.h2.table.TableBase;
import org.h2.table.TableFilter;
import org.h2.table.TableView;
import org.h2.value.Value;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.query.h2.sql.GridSqlOperationType.AND;
import static org.apache.ignite.internal.processors.query.h2.sql.GridSqlOperationType.BIGGER;
import static org.apache.ignite.internal.processors.query.h2.sql.GridSqlOperationType.BIGGER_EQUAL;
import static org.apache.ignite.internal.processors.query.h2.sql.GridSqlOperationType.CONCAT;
import static org.apache.ignite.internal.processors.query.h2.sql.GridSqlOperationType.DIVIDE;
import static org.apache.ignite.internal.processors.query.h2.sql.GridSqlOperationType.EQUAL;
import static org.apache.ignite.internal.processors.query.h2.sql.GridSqlOperationType.EQUAL_NULL_SAFE;
import static org.apache.ignite.internal.processors.query.h2.sql.GridSqlOperationType.IN;
import static org.apache.ignite.internal.processors.query.h2.sql.GridSqlOperationType.IS_NOT_NULL;
import static org.apache.ignite.internal.processors.query.h2.sql.GridSqlOperationType.IS_NULL;
import static org.apache.ignite.internal.processors.query.h2.sql.GridSqlOperationType.LIKE;
import static org.apache.ignite.internal.processors.query.h2.sql.GridSqlOperationType.MINUS;
import static org.apache.ignite.internal.processors.query.h2.sql.GridSqlOperationType.MODULUS;
import static org.apache.ignite.internal.processors.query.h2.sql.GridSqlOperationType.MULTIPLY;
import static org.apache.ignite.internal.processors.query.h2.sql.GridSqlOperationType.NOT;
import static org.apache.ignite.internal.processors.query.h2.sql.GridSqlOperationType.NOT_EQUAL;
import static org.apache.ignite.internal.processors.query.h2.sql.GridSqlOperationType.NOT_EQUAL_NULL_SAFE;
import static org.apache.ignite.internal.processors.query.h2.sql.GridSqlOperationType.OR;
import static org.apache.ignite.internal.processors.query.h2.sql.GridSqlOperationType.PLUS;
import static org.apache.ignite.internal.processors.query.h2.sql.GridSqlOperationType.REGEXP;
import static org.apache.ignite.internal.processors.query.h2.sql.GridSqlOperationType.SMALLER;
import static org.apache.ignite.internal.processors.query.h2.sql.GridSqlOperationType.SMALLER_EQUAL;
import static org.apache.ignite.internal.processors.query.h2.sql.GridSqlOperationType.SPATIAL_INTERSECTS;

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
    private static final Getter<FunctionTable, Expression> FUNC_EXPR = getter(FunctionTable.class, "functionExpr");

    /** */
    private static final Getter<JavaFunction, FunctionAlias> FUNC_ALIAS = getter(JavaFunction.class, "functionAlias");

    /** */
    private static final Getter<JdbcPreparedStatement,Command> COMMAND = getter(JdbcPreparedStatement.class, "command");

    /** */
    private static final Getter<SelectUnion, SortOrder> UNION_SORT = getter(SelectUnion.class, "sort");

    /** */
    private static final Getter<Explain, Prepared> EXPLAIN_COMMAND = getter(Explain.class, "command");

    /** */
    private static volatile Getter<Command,Prepared> prepared;

    /** */
    private final IdentityHashMap<Object, Object> h2ObjToGridObj = new IdentityHashMap<>();

    /**
     * @param stmt Prepared statement.
     * @return Parsed select.
     */
    public static GridSqlQuery parse(JdbcPreparedStatement stmt) {
        Command cmd = COMMAND.get(stmt);

        Getter<Command,Prepared> p = prepared;

        if (p == null) {
            Class<? extends Command> cls = cmd.getClass();

            assert "CommandContainer".equals(cls.getSimpleName());

            prepared = p = getter(cls, "prepared");
        }

        Prepared statement = p.get(cmd);

        return new GridSqlQueryParser().parse(statement);
    }

    /**
     * @param filter Filter.
     */
    private GridSqlElement parseTable(TableFilter filter) {
        GridSqlElement res = (GridSqlElement)h2ObjToGridObj.get(filter);

        if (res == null) {
            Table tbl = filter.getTable();

            if (tbl instanceof TableBase)
                res = new GridSqlTable(tbl.getSchema().getName(), tbl.getName());
            else if (tbl instanceof TableView) {
                Query qry = VIEW_QUERY.get((TableView)tbl);

                res = new GridSqlSubquery(parse(qry));
            }
            else if (tbl instanceof FunctionTable)
                res = parseExpression(FUNC_EXPR.get((FunctionTable)tbl), false);
            else if (tbl instanceof RangeTable) {
                res = new GridSqlFunction(GridSqlFunctionType.SYSTEM_RANGE);

                res.addChild(parseExpression(RANGE_MIN.get((RangeTable)tbl), false));
                res.addChild(parseExpression(RANGE_MAX.get((RangeTable)tbl), false));
            }
            else
                assert0(false, filter.getSelect().getSQL());

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
        res.where(parseExpression(where, false));

        Set<TableFilter> allFilters = new HashSet<>(select.getTopFilters());

        GridSqlElement from = null;

        TableFilter filter = select.getTopTableFilter();

        do {
            assert0(filter != null, select);
            assert0(filter.getNestedJoin() == null, select);

            GridSqlElement gridFilter = parseTable(filter);

            from = from == null ? gridFilter : new GridSqlJoin(from, gridFilter, filter.isJoinOuter(),
                parseExpression(filter.getJoinCondition(), false));

            allFilters.remove(filter);

            filter = filter.getJoin();
        }
        while (filter != null);

        res.from(from);

        assert allFilters.isEmpty();

        ArrayList<Expression> expressions = select.getExpressions();

        for (int i = 0; i < expressions.size(); i++)
            res.addColumn(parseExpression(expressions.get(i), true), i < select.getColumnCount());

        int[] grpIdx = GROUP_INDEXES.get(select);

        if (grpIdx != null)
            res.groupColumns(grpIdx);

        int havingIdx = HAVING_INDEX.get(select);

        if (havingIdx >= 0)
            res.havingColumn(havingIdx);

        processSortOrder(select.getSortOrder(), res);

        res.limit(parseExpression(select.getLimit(), false));
        res.offset(parseExpression(select.getOffset(), false));

        return res;
    }

    /**
     * @param sortOrder Sort order.
     * @param qry Query.
     */
    private void processSortOrder(SortOrder sortOrder, GridSqlQuery qry) {
        if (sortOrder == null)
            return;

        int[] indexes = sortOrder.getQueryColumnIndexes();
        int[] sortTypes = sortOrder.getSortTypes();

        for (int i = 0; i < indexes.length; i++) {
            int colIdx = indexes[i];
            int type = sortTypes[i];

            qry.addSort(new GridSqlSortColumn(colIdx,
                (type & SortOrder.DESCENDING) == 0,
                (type & SortOrder.NULLS_FIRST) != 0,
                (type & SortOrder.NULLS_LAST) != 0));
        }
    }

    /**
     * @param qry Select.
     */
    public GridSqlQuery parse(Prepared qry) {
        if (qry instanceof Select)
            return parse((Select)qry);

        if (qry instanceof SelectUnion)
            return parse((SelectUnion)qry);

        if (qry instanceof Explain)
            return parse(EXPLAIN_COMMAND.get((Explain)qry)).explain(true);

        throw new UnsupportedOperationException("Unknown query type: " + qry);
    }

    /**
     * @param union Select.
     */
    public GridSqlUnion parse(SelectUnion union) {
        GridSqlUnion res = (GridSqlUnion)h2ObjToGridObj.get(union);

        if (res != null)
            return res;

        res = new GridSqlUnion();

        res.right(parse(union.getRight()));
        res.left(parse(union.getLeft()));

        res.unionType(union.getUnionType());

        res.limit(parseExpression(union.getLimit(), false));
        res.offset(parseExpression(union.getOffset(), false));

        processSortOrder(UNION_SORT.get(union), res);

        h2ObjToGridObj.put(union, res);

        return res;
    }

    /**
     * @param expression Expression.
     * @param calcTypes Calculate types for all the expressions.
     * @return Parsed expression.
     */
    private GridSqlElement parseExpression(@Nullable Expression expression, boolean calcTypes) {
        if (expression == null)
            return null;

        GridSqlElement res = (GridSqlElement)h2ObjToGridObj.get(expression);

        if (res == null) {
            res = parseExpression0(expression, calcTypes);

            if (calcTypes) {
                GridSqlType type = GridSqlType.UNKNOWN;

                if (expression.getType() != Value.UNKNOWN) {
                    Column c = new Column(null, expression.getType(), expression.getPrecision(), expression.getScale(),
                        expression.getDisplaySize());

                    type = new GridSqlType(c.getType(), c.getScale(), c.getPrecision(), c.getDisplaySize(), c.getCreateSQL());
                }

                res.resultType(type);
            }

            h2ObjToGridObj.put(expression, res);
        }

        return res;
    }

    /**
     * @param expression Expression.
     * @param calcTypes Calculate types for all the expressions.
     * @return Parsed expression.
     */
    private GridSqlElement parseExpression0(Expression expression, boolean calcTypes) {
        if (expression instanceof ExpressionColumn) {
            TableFilter tblFilter = ((ExpressionColumn)expression).getTableFilter();

            GridSqlElement gridTblFilter = parseTable(tblFilter);

            return new GridSqlColumn(gridTblFilter, expression.getColumnName(), expression.getSQL());
        }

        if (expression instanceof Alias)
            return new GridSqlAlias(expression.getAlias(),
                parseExpression(expression.getNonAliasExpression(), calcTypes), true);

        if (expression instanceof ValueExpression)
            return new GridSqlConst(expression.getValue(null));

        if (expression instanceof Operation) {
            Operation operation = (Operation)expression;

            Integer type = OPERATION_TYPE.get(operation);

            if (type == Operation.NEGATE) {
                assert OPERATION_RIGHT.get(operation) == null;

                return new GridSqlOperation(GridSqlOperationType.NEGATE,
                    parseExpression(OPERATION_LEFT.get(operation), calcTypes));
            }

            return new GridSqlOperation(OPERATION_OP_TYPES[type],
                parseExpression(OPERATION_LEFT.get(operation), calcTypes),
                parseExpression(OPERATION_RIGHT.get(operation), calcTypes));
        }

        if (expression instanceof Comparison) {
            Comparison cmp = (Comparison)expression;

            GridSqlOperationType opType = COMPARISON_TYPES[COMPARISON_TYPE.get(cmp)];

            assert opType != null : COMPARISON_TYPE.get(cmp);

            GridSqlElement left = parseExpression(COMPARISON_LEFT.get(cmp), calcTypes);

            if (opType.childrenCount() == 1)
                return new GridSqlOperation(opType, left);

            GridSqlElement right = parseExpression(COMPARISON_RIGHT.get(cmp), calcTypes);

            return new GridSqlOperation(opType, left, right);
        }

        if (expression instanceof ConditionNot)
            return new GridSqlOperation(NOT, parseExpression(expression.getNotIfPossible(null), calcTypes));

        if (expression instanceof ConditionAndOr) {
            ConditionAndOr andOr = (ConditionAndOr)expression;

            int type = ANDOR_TYPE.get(andOr);

            assert type == ConditionAndOr.AND || type == ConditionAndOr.OR;

            return new GridSqlOperation(type == ConditionAndOr.AND ? AND : OR,
                parseExpression(ANDOR_LEFT.get(andOr), calcTypes), parseExpression(ANDOR_RIGHT.get(andOr), calcTypes));
        }

        if (expression instanceof Subquery) {
            Query qry = ((Subquery)expression).getQuery();

            assert0(qry instanceof Select, expression);

            return new GridSqlSubquery(parse((Select)qry));
        }

        if (expression instanceof ConditionIn) {
            GridSqlOperation res = new GridSqlOperation(IN);

            res.addChild(parseExpression(LEFT_CI.get((ConditionIn)expression), calcTypes));

            List<Expression> vals = VALUE_LIST_CI.get((ConditionIn)expression);

            for (Expression val : vals)
                res.addChild(parseExpression(val, calcTypes));

            return res;
        }

        if (expression instanceof ConditionInConstantSet) {
            GridSqlOperation res = new GridSqlOperation(IN);

            res.addChild(parseExpression(LEFT_CICS.get((ConditionInConstantSet)expression), calcTypes));

            List<Expression> vals = VALUE_LIST_CICS.get((ConditionInConstantSet)expression);

            for (Expression val : vals)
                res.addChild(parseExpression(val, calcTypes));

            return res;
        }

        if (expression instanceof ConditionInSelect) {
            GridSqlOperation res = new GridSqlOperation(IN);

            boolean all = ALL.get((ConditionInSelect)expression);
            int compareType = COMPARE_TYPE.get((ConditionInSelect)expression);

            assert0(!all, expression);
            assert0(compareType == Comparison.EQUAL, expression);

            res.addChild(parseExpression(LEFT_CIS.get((ConditionInSelect) expression), calcTypes));

            Query qry = QUERY.get((ConditionInSelect)expression);

            assert0(qry instanceof Select, qry);

            res.addChild(new GridSqlSubquery(parse((Select) qry)));

            return res;
        }

        if (expression instanceof CompareLike) {
            assert0(ESCAPE.get((CompareLike)expression) == null, expression);

            boolean regexp = REGEXP_CL.get((CompareLike)expression);

            return new GridSqlOperation(regexp ? REGEXP : LIKE,
                parseExpression(LEFT.get((CompareLike)expression), calcTypes),
                parseExpression(RIGHT.get((CompareLike)expression), calcTypes));
        }

        if (expression instanceof Function) {
            Function f = (Function)expression;

            GridSqlFunction res = new GridSqlFunction(null, f.getName());

            if (f.getArgs() != null) {
                for (Expression arg : f.getArgs()) {
                    if (arg == null) {
                        if (f.getFunctionType() != Function.CASE)
                            throw new IllegalStateException("Function type with null arg: " + f.getFunctionType());

                        res.addChild(GridSqlPlaceholder.EMPTY);
                    }
                    else
                        res.addChild(parseExpression(arg, calcTypes));
                }
            }

            if (f.getFunctionType() == Function.CAST || f.getFunctionType() == Function.CONVERT) {
                Column c = new Column(null, f.getType(), f.getPrecision(), f.getScale(), f.getDisplaySize());

                res.resultType(new GridSqlType(c.getType(), c.getScale(), c.getPrecision(),
                    c.getDisplaySize(), c.getCreateSQL()));
            }

            return res;
        }

        if (expression instanceof JavaFunction) {
            JavaFunction f = (JavaFunction)expression;

            FunctionAlias alias = FUNC_ALIAS.get(f);

            GridSqlFunction res = new GridSqlFunction(alias.getSchema().getName(), f.getName());

            if (f.getArgs() != null) {
                for (Expression arg : f.getArgs())
                    res.addChild(parseExpression(arg, calcTypes));
            }

            return res;
        }

        if (expression instanceof Parameter)
            return new GridSqlParameter(((Parameter)expression).getIndex());

        if (expression instanceof Aggregate) {
            GridSqlAggregateFunction res = new GridSqlAggregateFunction(DISTINCT.get((Aggregate)expression),
                TYPE.get((Aggregate)expression));

            Expression on = ON.get((Aggregate)expression);

            if (on != null)
                res.addChild(parseExpression(on, calcTypes));

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
    private static <T, R> Getter<T, R> getter(Class<? extends T> cls, String fldName) {
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