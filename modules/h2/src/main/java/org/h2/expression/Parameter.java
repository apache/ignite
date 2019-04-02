/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression;

import org.h2.api.ErrorCode;
import org.h2.engine.Session;
import org.h2.expression.condition.Comparison;
import org.h2.message.DbException;
import org.h2.table.Column;
import org.h2.table.ColumnResolver;
import org.h2.table.TableFilter;
import org.h2.value.TypeInfo;
import org.h2.value.Value;
import org.h2.value.ValueBoolean;
import org.h2.value.ValueNull;
import org.h2.value.ValueString;

/**
 * A parameter of a prepared statement.
 */
public class Parameter extends Expression implements ParameterInterface {

    private Value value;
    private Column column;
    private final int index;

    public Parameter(int index) {
        this.index = index;
    }

    @Override
    public StringBuilder getSQL(StringBuilder builder, boolean alwaysQuote) {
        return builder.append('?').append(index + 1);
    }

    @Override
    public void setValue(Value v, boolean closeOld) {
        // don't need to close the old value as temporary files are anyway
        // removed
        this.value = v;
    }

    public void setValue(Value v) {
        this.value = v;
    }

    @Override
    public Value getParamValue() {
        if (value == null) {
            // to allow parameters in function tables
            return ValueNull.INSTANCE;
        }
        return value;
    }

    @Override
    public Value getValue(Session session) {
        return getParamValue();
    }

    @Override
    public TypeInfo getType() {
        if (value != null) {
            return value.getType();
        }
        if (column != null) {
            return column.getType();
        }
        return TypeInfo.TYPE_UNKNOWN;
    }

    @Override
    public int getValueType() {
        if (value != null) {
            return value.getValueType();
        }
        if (column != null) {
            return column.getType().getValueType();
        }
        return Value.UNKNOWN;
    }

    @Override
    public void mapColumns(ColumnResolver resolver, int level, int state) {
        // can't map
    }

    @Override
    public void checkSet() {
        if (value == null) {
            throw DbException.get(ErrorCode.PARAMETER_NOT_SET_1, "#" + (index + 1));
        }
    }

    @Override
    public Expression optimize(Session session) {
        if (session.getDatabase().getMode().treatEmptyStringsAsNull) {
            if (value instanceof ValueString && value.getString().isEmpty()) {
                value = ValueNull.INSTANCE;
            }
        }
        return this;
    }

    @Override
    public boolean isConstant() {
        return false;
    }

    @Override
    public boolean isValueSet() {
        return value != null;
    }

    @Override
    public void setEvaluatable(TableFilter tableFilter, boolean b) {
        // not bound
    }

    @Override
    public int getScale() {
        if (value != null) {
            return value.getType().getScale();
        }
        if (column != null) {
            return column.getType().getScale();
        }
        return 0;
    }

    @Override
    public long getPrecision() {
        if (value != null) {
            return value.getType().getPrecision();
        }
        if (column != null) {
            return column.getType().getPrecision();
        }
        return 0;
    }

    @Override
    public void updateAggregate(Session session, int stage) {
        // nothing to do
    }

    @Override
    public boolean isEverything(ExpressionVisitor visitor) {
        switch (visitor.getType()) {
        case ExpressionVisitor.EVALUATABLE:
            // the parameter _will_be_ evaluatable at execute time
        case ExpressionVisitor.SET_MAX_DATA_MODIFICATION_ID:
            // it is checked independently if the value is the same as the last
            // time
        case ExpressionVisitor.NOT_FROM_RESOLVER:
        case ExpressionVisitor.QUERY_COMPARABLE:
        case ExpressionVisitor.GET_DEPENDENCIES:
        case ExpressionVisitor.OPTIMIZABLE_AGGREGATE:
        case ExpressionVisitor.DETERMINISTIC:
        case ExpressionVisitor.READONLY:
        case ExpressionVisitor.GET_COLUMNS1:
        case ExpressionVisitor.GET_COLUMNS2:
            return true;
        case ExpressionVisitor.INDEPENDENT:
            return value != null;
        default:
            throw DbException.throwInternalError("type="+visitor.getType());
        }
    }

    @Override
    public int getCost() {
        return 0;
    }

    @Override
    public Expression getNotIfPossible(Session session) {
        return new Comparison(session, Comparison.EQUAL, this,
                ValueExpression.get(ValueBoolean.FALSE));
    }

    public void setColumn(Column column) {
        this.column = column;
    }

    public int getIndex() {
        return index;
    }

}
