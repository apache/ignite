/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression.condition;

import java.util.ArrayList;
import org.h2.engine.Database;
import org.h2.engine.Session;
import org.h2.expression.Expression;
import org.h2.expression.ExpressionColumn;
import org.h2.expression.ExpressionVisitor;
import org.h2.expression.Parameter;
import org.h2.expression.ValueExpression;
import org.h2.expression.function.Function;
import org.h2.expression.function.TableFunction;
import org.h2.index.IndexCondition;
import org.h2.result.ResultInterface;
import org.h2.table.ColumnResolver;
import org.h2.table.TableFilter;
import org.h2.value.Value;
import org.h2.value.ValueBoolean;
import org.h2.value.ValueNull;

/**
 * An 'in' condition with a list of values, as in WHERE NAME IN(...)
 */
public class ConditionIn extends Condition {

    private final Database database;
    private Expression left;
    private final ArrayList<Expression> valueList;

    /**
     * Create a new IN(..) condition.
     *
     * @param database the database
     * @param left the expression before IN
     * @param values the value list (at least one element)
     */
    public ConditionIn(Database database, Expression left,
            ArrayList<Expression> values) {
        this.database = database;
        this.left = left;
        this.valueList = values;
    }

    @Override
    public Value getValue(Session session) {
        Value l = left.getValue(session);
        if (l.containsNull()) {
            return ValueNull.INSTANCE;
        }
        int size = valueList.size();
        if (size == 1) {
            Expression e = valueList.get(0);
            if (e instanceof TableFunction) {
                return ConditionInParameter.getValue(database, l, e.getValue(session));
            }
        }
        boolean hasNull = false;
        for (int i = 0; i < size; i++) {
            Expression e = valueList.get(i);
            Value r = e.getValue(session);
            Value cmp = Comparison.compare(database, l, r, Comparison.EQUAL);
            if (cmp == ValueNull.INSTANCE) {
                hasNull = true;
            } else if (cmp == ValueBoolean.TRUE) {
                return cmp;
            }
        }
        if (hasNull) {
            return ValueNull.INSTANCE;
        }
        return ValueBoolean.FALSE;
    }

    @Override
    public void mapColumns(ColumnResolver resolver, int level, int state) {
        left.mapColumns(resolver, level, state);
        for (Expression e : valueList) {
            e.mapColumns(resolver, level, state);
        }
    }

    @Override
    public Expression optimize(Session session) {
        left = left.optimize(session);
        boolean constant = left.isConstant();
        if (constant && left == ValueExpression.getNull()) {
            return left;
        }
        int size = valueList.size();
        if (size == 1) {
            Expression right = valueList.get(0);
            if (right instanceof TableFunction) {
                TableFunction tf = (TableFunction) right;
                if (tf.getFunctionType() == Function.UNNEST) {
                    Expression[] args = tf.getArgs();
                    if (args.length == 1) {
                        Expression arg = args[0];
                        if (arg instanceof Parameter) {
                            return new ConditionInParameter(database, left, (Parameter) arg);
                        }
                    }
                }
                if (tf.isConstant()) {
                    boolean allValuesNull = true;
                    ResultInterface ri = right.getValue(session).getResult();
                    ArrayList<Expression> list = new ArrayList<>(ri.getRowCount());
                    while (ri.next()) {
                        Value v = ri.currentRow()[0];
                        if (!v.containsNull()) {
                            allValuesNull = false;
                        }
                        list.add(ValueExpression.get(v));
                    }
                    return optimize2(session, constant, true, allValuesNull, list);
                }
                return this;
            }
        }
        boolean allValuesConstant = true;
        boolean allValuesNull = true;
        for (int i = 0; i < size; i++) {
            Expression e = valueList.get(i);
            e = e.optimize(session);
            if (e.isConstant() && !e.getValue(session).containsNull()) {
                allValuesNull = false;
            }
            if (allValuesConstant && !e.isConstant()) {
                allValuesConstant = false;
            }
            if (left instanceof ExpressionColumn && e instanceof Parameter) {
                ((Parameter) e)
                        .setColumn(((ExpressionColumn) left).getColumn());
            }
            valueList.set(i, e);
        }
        return optimize2(session, constant, allValuesConstant, allValuesNull, valueList);
    }

    private Expression optimize2(Session session, boolean constant, boolean allValuesConstant, boolean allValuesNull,
            ArrayList<Expression> values) {
        if (constant && allValuesConstant) {
            return ValueExpression.get(getValue(session));
        }
        if (values.size() == 1) {
            return new Comparison(session, Comparison.EQUAL, left, values.get(0)).optimize(session);
        }
        if (allValuesConstant && !allValuesNull) {
            int leftType = left.getType().getValueType();
            if (leftType == Value.UNKNOWN) {
                return this;
            }
            if (leftType == Value.ENUM && !(left instanceof ExpressionColumn)) {
                return this;
            }
            Expression expr = new ConditionInConstantSet(session, left, values);
            expr = expr.optimize(session);
            return expr;
        }
        return this;
    }

    @Override
    public void createIndexConditions(Session session, TableFilter filter) {
        if (!(left instanceof ExpressionColumn)) {
            return;
        }
        ExpressionColumn l = (ExpressionColumn) left;
        if (filter != l.getTableFilter()) {
            return;
        }
        if (session.getDatabase().getSettings().optimizeInList) {
            ExpressionVisitor visitor = ExpressionVisitor.getNotFromResolverVisitor(filter);
            for (Expression e : valueList) {
                if (!e.isEverything(visitor)) {
                    return;
                }
            }
            filter.addIndexCondition(IndexCondition.getInList(l, valueList));
        }
    }

    @Override
    public void setEvaluatable(TableFilter tableFilter, boolean b) {
        left.setEvaluatable(tableFilter, b);
        for (Expression e : valueList) {
            e.setEvaluatable(tableFilter, b);
        }
    }

    @Override
    public StringBuilder getSQL(StringBuilder builder, boolean alwaysQuote) {
        builder.append('(');
        left.getSQL(builder, alwaysQuote).append(" IN(");
        writeExpressions(builder, valueList, alwaysQuote);
        return builder.append("))");
    }

    @Override
    public void updateAggregate(Session session, int stage) {
        left.updateAggregate(session, stage);
        for (Expression e : valueList) {
            e.updateAggregate(session, stage);
        }
    }

    @Override
    public boolean isEverything(ExpressionVisitor visitor) {
        if (!left.isEverything(visitor)) {
            return false;
        }
        return areAllValues(visitor);
    }

    private boolean areAllValues(ExpressionVisitor visitor) {
        for (Expression e : valueList) {
            if (!e.isEverything(visitor)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int getCost() {
        int cost = left.getCost();
        for (Expression e : valueList) {
            cost += e.getCost();
        }
        return cost;
    }

    /**
     * Add an additional element if possible. Example: given two conditions
     * A IN(1, 2) OR A=3, the constant 3 is added: A IN(1, 2, 3).
     *
     * @param other the second condition
     * @return null if the condition was not added, or the new condition
     */
    Expression getAdditional(Comparison other) {
        Expression add = other.getIfEquals(left);
        if (add != null) {
            valueList.add(add);
            return this;
        }
        return null;
    }

    @Override
    public int getSubexpressionCount() {
        return 1 + valueList.size();
    }

    @Override
    public Expression getSubexpression(int index) {
        if (index == 0) {
            return left;
        } else if (index > 0 && index <= valueList.size()) {
            return valueList.get(index - 1);
        }
        throw new IndexOutOfBoundsException();
    }

}
