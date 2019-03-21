/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression;

import org.h2.engine.Session;
import org.h2.table.ColumnResolver;
import org.h2.table.TableFilter;
import org.h2.value.TypeInfo;
import org.h2.value.Value;
import org.h2.value.ValueNull;

/**
 * Unary operation. Only negation operation is currently supported.
 */
public class UnaryOperation extends Expression {

    private Expression arg;
    private TypeInfo type;

    public UnaryOperation(Expression arg) {
        this.arg = arg;
    }

    @Override
    public StringBuilder getSQL(StringBuilder builder, boolean alwaysQuote) {
        // don't remove the space, otherwise it might end up some thing like
        // --1 which is a line remark
        builder.append("(- ");
        return arg.getSQL(builder, alwaysQuote).append(')');
    }

    @Override
    public Value getValue(Session session) {
        Value a = arg.getValue(session).convertTo(type, session.getDatabase().getMode(), null);
        return a == ValueNull.INSTANCE ? a : a.negate();
    }

    @Override
    public void mapColumns(ColumnResolver resolver, int level, int state) {
        arg.mapColumns(resolver, level, state);
    }

    @Override
    public Expression optimize(Session session) {
        arg = arg.optimize(session);
        type = arg.getType();
        if (type.getValueType() == Value.UNKNOWN) {
            type = TypeInfo.TYPE_DECIMAL_DEFAULT;
        } else if (type.getValueType() == Value.ENUM) {
            type = TypeInfo.TYPE_INT;
        }
        if (arg.isConstant()) {
            return ValueExpression.get(getValue(session));
        }
        return this;
    }

    @Override
    public void setEvaluatable(TableFilter tableFilter, boolean b) {
        arg.setEvaluatable(tableFilter, b);
    }

    @Override
    public TypeInfo getType() {
        return type;
    }

    @Override
    public void updateAggregate(Session session, int stage) {
        arg.updateAggregate(session, stage);
    }

    @Override
    public boolean isEverything(ExpressionVisitor visitor) {
        return arg.isEverything(visitor);
    }

    @Override
    public int getCost() {
        return arg.getCost() + 1;
    }

    @Override
    public int getSubexpressionCount() {
        return 1;
    }

    @Override
    public Expression getSubexpression(int index) {
        if (index == 0) {
            return arg;
        }
        throw new IndexOutOfBoundsException();
    }

}
