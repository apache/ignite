/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression;

import org.h2.command.Parser;
import org.h2.engine.Session;
import org.h2.message.DbException;
import org.h2.table.ColumnResolver;
import org.h2.table.TableFilter;
import org.h2.value.Value;

/**
 * A user-defined variable, for example: @ID.
 */
public class Variable extends Expression {

    private final String name;
    private Value lastValue;

    public Variable(Session session, String name) {
        this.name = name;
        lastValue = session.getVariable(name);
    }

    @Override
    public int getCost() {
        return 0;
    }

    @Override
    public int getDisplaySize() {
        return lastValue.getDisplaySize();
    }

    @Override
    public long getPrecision() {
        return lastValue.getPrecision();
    }

    @Override
    public String getSQL() {
        return "@" + Parser.quoteIdentifier(name);
    }

    @Override
    public int getScale() {
        return lastValue.getScale();
    }

    @Override
    public int getType() {
        return lastValue.getType();
    }

    @Override
    public Value getValue(Session session) {
        lastValue = session.getVariable(name);
        return lastValue;
    }

    @Override
    public boolean isEverything(ExpressionVisitor visitor) {
        switch (visitor.getType()) {
        case ExpressionVisitor.EVALUATABLE:
            // the value will be evaluated at execute time
        case ExpressionVisitor.SET_MAX_DATA_MODIFICATION_ID:
            // it is checked independently if the value is the same as the last
            // time
        case ExpressionVisitor.OPTIMIZABLE_MIN_MAX_COUNT_ALL:
        case ExpressionVisitor.READONLY:
        case ExpressionVisitor.INDEPENDENT:
        case ExpressionVisitor.NOT_FROM_RESOLVER:
        case ExpressionVisitor.QUERY_COMPARABLE:
        case ExpressionVisitor.GET_DEPENDENCIES:
        case ExpressionVisitor.GET_COLUMNS:
            return true;
        case ExpressionVisitor.DETERMINISTIC:
            return false;
        default:
            throw DbException.throwInternalError("type="+visitor.getType());
        }
    }

    @Override
    public void mapColumns(ColumnResolver resolver, int level) {
        // nothing to do
    }

    @Override
    public Expression optimize(Session session) {
        return this;
    }

    @Override
    public void setEvaluatable(TableFilter tableFilter, boolean value) {
        // nothing to do
    }

    @Override
    public void updateAggregate(Session session) {
        // nothing to do
    }

    public String getName() {
        return name;
    }

}
