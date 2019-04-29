/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression;

import org.h2.engine.Session;
import org.h2.table.Column;
import org.h2.table.ColumnResolver;
import org.h2.table.TableFilter;
import org.h2.value.TypeInfo;
import org.h2.value.Value;
import org.h2.value.ValueArray;
import org.h2.value.ValueRow;

/**
 * A list of expressions, as in (ID, NAME).
 * The result of this expression is a row or an array.
 */
public class ExpressionList extends Expression {

    private final Expression[] list;
    private final boolean isArray;

    public ExpressionList(Expression[] list, boolean isArray) {
        this.list = list;
        this.isArray = isArray;
    }

    @Override
    public Value getValue(Session session) {
        Value[] v = new Value[list.length];
        for (int i = 0; i < list.length; i++) {
            v[i] = list[i].getValue(session);
        }
        return isArray ? ValueArray.get(v) : ValueRow.get(v);
    }

    @Override
    public TypeInfo getType() {
        return isArray ? TypeInfo.TYPE_ARRAY : TypeInfo.TYPE_ROW;
    }

    @Override
    public void mapColumns(ColumnResolver resolver, int level, int state) {
        for (Expression e : list) {
            e.mapColumns(resolver, level, state);
        }
    }

    @Override
    public Expression optimize(Session session) {
        boolean allConst = true;
        for (int i = 0; i < list.length; i++) {
            Expression e = list[i].optimize(session);
            if (!e.isConstant()) {
                allConst = false;
            }
            list[i] = e;
        }
        if (allConst) {
            return ValueExpression.get(getValue(session));
        }
        return this;
    }

    @Override
    public void setEvaluatable(TableFilter tableFilter, boolean b) {
        for (Expression e : list) {
            e.setEvaluatable(tableFilter, b);
        }
    }

    @Override
    public StringBuilder getSQL(StringBuilder builder, boolean alwaysQuote) {
        builder.append(isArray ? "ARRAY [" : "ROW (");
        writeExpressions(builder, list, alwaysQuote);
        return builder.append(isArray ? ']' : ')');
    }

    @Override
    public void updateAggregate(Session session, int stage) {
        for (Expression e : list) {
            e.updateAggregate(session, stage);
        }
    }

    @Override
    public boolean isEverything(ExpressionVisitor visitor) {
        for (Expression e : list) {
            if (!e.isEverything(visitor)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int getCost() {
        int cost = 1;
        for (Expression e : list) {
            cost += e.getCost();
        }
        return cost;
    }

    @Override
    public Expression[] getExpressionColumns(Session session) {
        ExpressionColumn[] expr = new ExpressionColumn[list.length];
        for (int i = 0; i < list.length; i++) {
            Expression e = list[i];
            Column col = new Column("C" + (i + 1), e.getType());
            expr[i] = new ExpressionColumn(session.getDatabase(), col);
        }
        return expr;
    }

    @Override
    public boolean isConstant() {
        for (Expression e : list) {
            if (!e.isConstant()) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int getSubexpressionCount() {
        return list.length;
    }

    @Override
    public Expression getSubexpression(int index) {
        return list[index];
    }

}
