/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression;

import org.h2.engine.Session;
import org.h2.message.DbException;
import org.h2.schema.Sequence;
import org.h2.table.ColumnResolver;
import org.h2.table.TableFilter;
import org.h2.value.Value;
import org.h2.value.ValueInt;
import org.h2.value.ValueLong;

/**
 * Wraps a sequence when used in a statement.
 */
public class SequenceValue extends Expression {

    private final Sequence sequence;

    public SequenceValue(Sequence sequence) {
        this.sequence = sequence;
    }

    @Override
    public Value getValue(Session session) {
        long value = sequence.getNext(session);
        session.setLastIdentity(ValueLong.get(value));
        return ValueLong.get(value);
    }

    @Override
    public int getType() {
        return Value.LONG;
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
    public void setEvaluatable(TableFilter tableFilter, boolean b) {
        // nothing to do
    }

    @Override
    public int getScale() {
        return 0;
    }

    @Override
    public long getPrecision() {
        return ValueInt.PRECISION;
    }

    @Override
    public int getDisplaySize() {
        return ValueInt.DISPLAY_SIZE;
    }

    @Override
    public String getSQL() {
        return "(NEXT VALUE FOR " + sequence.getSQL() +")";
    }

    @Override
    public void updateAggregate(Session session) {
        // nothing to do
    }

    @Override
    public boolean isEverything(ExpressionVisitor visitor) {
        switch (visitor.getType()) {
        case ExpressionVisitor.EVALUATABLE:
        case ExpressionVisitor.OPTIMIZABLE_MIN_MAX_COUNT_ALL:
        case ExpressionVisitor.NOT_FROM_RESOLVER:
        case ExpressionVisitor.GET_COLUMNS:
            return true;
        case ExpressionVisitor.DETERMINISTIC:
        case ExpressionVisitor.READONLY:
        case ExpressionVisitor.INDEPENDENT:
        case ExpressionVisitor.QUERY_COMPARABLE:
            return false;
        case ExpressionVisitor.SET_MAX_DATA_MODIFICATION_ID:
            visitor.addDataModificationId(sequence.getModificationId());
            return true;
        case ExpressionVisitor.GET_DEPENDENCIES:
            visitor.addDependency(sequence);
            return true;
        default:
            throw DbException.throwInternalError("type="+visitor.getType());
        }
    }

    @Override
    public int getCost() {
        return 1;
    }

}
