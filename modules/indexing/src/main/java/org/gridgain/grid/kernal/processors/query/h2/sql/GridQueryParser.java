/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.query.h2.sql;

import org.apache.ignite.*;
import org.gridgain.grid.kernal.processors.query.h2.sql.GridQueryUtils.*;
import org.h2.command.dml.*;
import org.h2.expression.*;
import org.h2.result.*;
import org.h2.table.*;
import org.jetbrains.annotations.*;

import java.lang.reflect.*;
import java.util.*;
import java.util.Set;

import static org.gridgain.grid.kernal.processors.query.h2.sql.GridOperationType.*;
import static org.gridgain.grid.kernal.processors.query.h2.sql.GridQueryUtils.*;

/**
 * H2 Query parser.
 */
@SuppressWarnings("TypeMayBeWeakened")
public class GridQueryParser {
    /** */
    private static final GridOperationType[] OPERATION_OP_TYPES = new GridOperationType[]{CONCAT, PLUS, MINUS, MULTIPLY, DIVIDE, null, MODULUS};

    /** */
    private static final GridOperationType[] COMPARISON_TYPES = new GridOperationType[]{EQUAL, BIGGER_EQUAL, BIGGER, SMALLER_EQUAL,
        SMALLER, NOT_EQUAL, IS_NULL, IS_NOT_NULL,
        null, null, null, SPATIAL_INTERSECTS /* 11 */, null, null, null, null, EQUAL_NULL_SAFE /* 16 */, null, null, null, null,
        NOT_EQUAL_NULL_SAFE /* 21 */};

    /** */
    private static final Getter<Select, Expression> CONDITION = getter(Select.class, "condition");

    /** */
    private static final Field GROUP_INDEXES = GridQueryUtils.getField(Select.class, "groupIndex");

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
    private final IdentityHashMap<Object, Object> h2ObjToGridObj = new IdentityHashMap<>();

    /**
     * @param filter Filter.
     */
    private GridSqlElement toGridTableFilter(TableFilter filter) {
        GridSqlElement res = (GridSqlElement)h2ObjToGridObj.get(filter);

        if (res == null) {
            Table tbl = filter.getTable();

            if (tbl instanceof TableBase)
                res = new GridTable(tbl.getSchema().getName(), tbl.getName());
            else if (tbl instanceof TableView) {
                Query qry = GridQueryUtils.getFieldValue(tbl, "viewQuery");

                assert0(qry instanceof Select, qry);

                res = new GridSubquery(toGridSelect((Select)qry));
            }
            else
                throw new IgniteException("Unsupported query: " + filter);

            String alias = GridQueryUtils.getFieldValue(filter, "alias");

            if (alias != null)
                res = new GridAlias(alias, res, false);

            h2ObjToGridObj.put(filter, res);
        }

        return res;
    }

    /**
     * @param select Select.
     */
    public GridSelect toGridSelect(Select select) {
        GridSelect res = (GridSelect)h2ObjToGridObj.get(select);

        if (res != null)
            return res;

        res = new GridSelect();

        h2ObjToGridObj.put(select, res);

        res.distinct(select.isDistinct());

        Expression where = CONDITION.get(select);
        res.where(toGridExpression(where));

        Set<TableFilter> allFilers = new HashSet<>(select.getTopFilters());

        GridSqlElement from = null;

        TableFilter filter = select.getTopTableFilter();
        do {
            assert0(filter != null, select);
            assert0(!filter.isJoinOuter(), select);
            assert0(filter.getNestedJoin() == null, select);
            assert0(filter.getJoinCondition() == null, select);
            assert0(filter.getFilterCondition() == null, select);

            GridSqlElement gridFilter = toGridTableFilter(filter);

            from = from == null ? gridFilter : new GridJoin(from, gridFilter);

            allFilers.remove(filter);

            filter = filter.getJoin();
        }
        while (filter != null);

        res.from(from);

        assert allFilers.isEmpty();

        ArrayList<Expression> expressions = select.getExpressions();

        int[] grpIdx = GridQueryUtils.getFieldValue(GROUP_INDEXES, select);

        if (grpIdx != null) {
            for (int idx : grpIdx)
                res.addGroupExpression(toGridExpression(expressions.get(idx)));
        }

        assert0(select.getHaving() == null, select);

        int havingIdx = GridQueryUtils.getFieldValue(select, "havingIndex");

        if (havingIdx >= 0)
            res.having(toGridExpression(expressions.get(havingIdx)));

        for (int i = 0; i < select.getColumnCount(); i++)
            res.addSelectExpression(toGridExpression(expressions.get(i)));

        SortOrder sortOrder = select.getSortOrder();

        if (sortOrder != null) {
            int[] indexes = sortOrder.getQueryColumnIndexes();
            int[] sortTypes = sortOrder.getSortTypes();

            for (int i = 0; i < indexes.length; i++)
                res.addSort(toGridExpression(expressions.get(indexes[i])), sortTypes[i]);
        }

        return res;
    }

    /**
     * @param expression Expression.
     */
    private GridSqlElement toGridExpression(@Nullable Expression expression) {
        if (expression == null)
            return null;

        GridSqlElement res = (GridSqlElement)h2ObjToGridObj.get(expression);

        if (res == null) {
            res = toGridExpression0(expression);

            h2ObjToGridObj.put(expression, res);
        }

        return res;
    }

    /**
     * @param expression Expression.
     */
    @NotNull private GridSqlElement toGridExpression0(@NotNull Expression expression) {
        if (expression instanceof ExpressionColumn) {
            TableFilter tblFilter = ((ExpressionColumn)expression).getTableFilter();

            GridSqlElement gridTblFilter = toGridTableFilter(tblFilter);

            return new GridSqlColumn(gridTblFilter, expression.getColumnName(), expression.getSQL());
        }

        if (expression instanceof Alias)
            return new GridAlias(expression.getAlias(), toGridExpression(expression.getNonAliasExpression()), true);

        if (expression instanceof ValueExpression)
            return new GridValueExpression(expression.getValue(null));

        if (expression instanceof Operation) {
            Operation operation = (Operation)expression;

            Integer type = OPERATION_TYPE.get(operation);

            if (type == Operation.NEGATE) {
                assert OPERATION_RIGHT.get(operation) == null;

                return new GridOperation(GridOperationType.NEGATE, toGridExpression(OPERATION_LEFT.get(operation)));
            }

            return new GridOperation(OPERATION_OP_TYPES[type],
                toGridExpression(OPERATION_LEFT.get(operation)),
                toGridExpression(OPERATION_RIGHT.get(operation)));
        }

        if (expression instanceof Comparison) {
            Comparison cmp = (Comparison)expression;

            GridOperationType opType = COMPARISON_TYPES[COMPARISON_TYPE.get(cmp)];

            assert opType != null : COMPARISON_TYPE.get(cmp);

            GridSqlElement left = toGridExpression(COMPARISON_LEFT.get(cmp));

            if (opType.childrenCount() == 1)
                return new GridOperation(opType, left);

            GridSqlElement right = toGridExpression(COMPARISON_RIGHT.get(cmp));

            return new GridOperation(opType, left, right);
        }

        if (expression instanceof ConditionNot)
            return new GridOperation(NOT, toGridExpression(expression.getNotIfPossible(null)));

        if (expression instanceof ConditionAndOr) {
            ConditionAndOr andOr = (ConditionAndOr)expression;

            int type = ANDOR_TYPE.get(andOr);

            assert type == ConditionAndOr.AND || type == ConditionAndOr.OR;

            return new GridOperation(type == ConditionAndOr.AND ? AND : OR,
                toGridExpression(ANDOR_LEFT.get(andOr)), toGridExpression(ANDOR_RIGHT.get(andOr)));
        }

        if (expression instanceof Subquery) {
            Query qry = ((Subquery)expression).getQuery();

            assert0(qry instanceof Select, expression);

            return new GridSubquery(toGridSelect((Select)qry));
        }

        if (expression instanceof ConditionIn) {
            GridOperation res = new GridOperation(IN);

            res.addChild(toGridExpression((Expression)GridQueryUtils.getFieldValue(expression, "left")));

            List<Expression> vals = GridQueryUtils.getFieldValue(expression, "valueList");

            for (Expression val : vals)
                res.addChild(toGridExpression(val));

            return res;
        }

        if (expression instanceof ConditionInConstantSet) {
            GridOperation res = new GridOperation(IN);

            res.addChild(toGridExpression((Expression)GridQueryUtils.getFieldValue(expression, "left")));

            List<Expression> vals = GridQueryUtils.getFieldValue(expression, "valueList");

            for (Expression val : vals)
                res.addChild(toGridExpression(val));

            return res;
        }

        if (expression instanceof ConditionInSelect) {
            GridOperation res = new GridOperation(IN);

            boolean all = GridQueryUtils.getFieldValue(expression, "all");
            int compareType = GridQueryUtils.getFieldValue(expression, "compareType");

            assert0(!all, expression);
            assert0(compareType == Comparison.EQUAL, expression);

            res.addChild(toGridExpression((Expression)GridQueryUtils.getFieldValue(expression, "left")));

            Query qry = GridQueryUtils.getFieldValue(expression, "query");

            assert0(qry instanceof Select, qry);

            res.addChild(new GridSubquery(toGridSelect((Select)qry)));

            return res;
        }

        if (expression instanceof CompareLike) {
            assert0(GridQueryUtils.getFieldValue(expression, "escape") == null, expression);

            boolean regexp = GridQueryUtils.getFieldValue(expression, "regexp");

            return new GridOperation(regexp ? REGEXP : LIKE,
                toGridExpression((Expression)GridQueryUtils.getFieldValue(expression, "left")),
                toGridExpression((Expression)GridQueryUtils.getFieldValue(expression, "right")));
        }

        if (expression instanceof Function) {
            Function function = (Function)expression;

            GridSqlFunction res = new GridSqlFunction(function.getName());

            for (Expression arg : function.getArgs())
                res.addChild(toGridExpression(arg));

            return res;
        }

        if (expression instanceof Parameter)
            return new GridSqlParameter(((Parameter)expression).getIndex());

        if (expression instanceof Aggregate) {
            GridAggregateFunction res = new GridAggregateFunction(GridQueryUtils.<Boolean>getFieldValue(expression,
                "distinct"), GridQueryUtils.<Integer>getFieldValue(expression, "type"));

            Expression on = GridQueryUtils.getFieldValue(expression, "on");

            if (on != null)
                res.addChild(toGridExpression(on));

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
}
