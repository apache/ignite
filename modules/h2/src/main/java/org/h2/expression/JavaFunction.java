/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression;

import org.h2.command.Parser;
import org.h2.engine.Constants;
import org.h2.engine.FunctionAlias;
import org.h2.engine.Session;
import org.h2.table.ColumnResolver;
import org.h2.table.TableFilter;
import org.h2.util.StatementBuilder;
import org.h2.value.DataType;
import org.h2.value.Value;
import org.h2.value.ValueArray;
import org.h2.value.ValueNull;
import org.h2.value.ValueResultSet;

/**
 * This class wraps a user-defined function.
 */
public class JavaFunction extends Expression implements FunctionCall {

    private final FunctionAlias functionAlias;
    private final FunctionAlias.JavaMethod javaMethod;
    private final Expression[] args;

    public JavaFunction(FunctionAlias functionAlias, Expression[] args) {
        this.functionAlias = functionAlias;
        this.javaMethod = functionAlias.findJavaMethod(args);
        this.args = args;
    }

    @Override
    public Value getValue(Session session) {
        return javaMethod.getValue(session, args, false);
    }

    @Override
    public int getType() {
        return javaMethod.getDataType();
    }

    @Override
    public void mapColumns(ColumnResolver resolver, int level) {
        for (Expression e : args) {
            e.mapColumns(resolver, level);
        }
    }

    @Override
    public Expression optimize(Session session) {
        boolean allConst = isDeterministic();
        for (int i = 0, len = args.length; i < len; i++) {
            Expression e = args[i].optimize(session);
            args[i] = e;
            allConst &= e.isConstant();
        }
        if (allConst) {
            return ValueExpression.get(getValue(session));
        }
        return this;
    }

    @Override
    public void setEvaluatable(TableFilter tableFilter, boolean b) {
        for (Expression e : args) {
            if (e != null) {
                e.setEvaluatable(tableFilter, b);
            }
        }
    }

    @Override
    public int getScale() {
        return DataType.getDataType(getType()).defaultScale;
    }

    @Override
    public long getPrecision() {
        return Integer.MAX_VALUE;
    }

    @Override
    public int getDisplaySize() {
        return Integer.MAX_VALUE;
    }

    @Override
    public String getSQL() {
        StatementBuilder buff = new StatementBuilder();
        // TODO always append the schema once FUNCTIONS_IN_SCHEMA is enabled
        if (functionAlias.getDatabase().getSettings().functionsInSchema ||
                !functionAlias.getSchema().getName().equals(Constants.SCHEMA_MAIN)) {
            buff.append(
                    Parser.quoteIdentifier(functionAlias.getSchema().getName()))
                    .append('.');
        }
        buff.append(Parser.quoteIdentifier(functionAlias.getName())).append('(');
        for (Expression e : args) {
            buff.appendExceptFirst(", ");
            buff.append(e.getSQL());
        }
        return buff.append(')').toString();
    }

    @Override
    public void updateAggregate(Session session) {
        for (Expression e : args) {
            if (e != null) {
                e.updateAggregate(session);
            }
        }
    }

    @Override
    public String getName() {
        return functionAlias.getName();
    }

    @Override
    public ValueResultSet getValueForColumnList(Session session,
            Expression[] argList) {
        Value v = javaMethod.getValue(session, argList, true);
        return v == ValueNull.INSTANCE ? null : (ValueResultSet) v;
    }

    @Override
    public Expression[] getArgs() {
        return args;
    }

    @Override
    public boolean isEverything(ExpressionVisitor visitor) {
        switch (visitor.getType()) {
        case ExpressionVisitor.DETERMINISTIC:
            if (!isDeterministic()) {
                return false;
            }
            // only if all parameters are deterministic as well
            break;
        case ExpressionVisitor.GET_DEPENDENCIES:
            visitor.addDependency(functionAlias);
            break;
        default:
        }
        for (Expression e : args) {
            if (e != null && !e.isEverything(visitor)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int getCost() {
        int cost = javaMethod.hasConnectionParam() ? 25 : 5;
        for (Expression e : args) {
            cost += e.getCost();
        }
        return cost;
    }

    @Override
    public boolean isDeterministic() {
        return functionAlias.isDeterministic();
    }

    @Override
    public Expression[] getExpressionColumns(Session session) {
        switch (getType()) {
        case Value.RESULT_SET:
            ValueResultSet rs = getValueForColumnList(session, getArgs());
            return getExpressionColumns(session, rs.getResultSet());
        case Value.ARRAY:
            return getExpressionColumns(session, (ValueArray) getValue(session));
        }
        return super.getExpressionColumns(session);
    }

    @Override
    public boolean isBufferResultSetToLocalTemp() {
        return functionAlias.isBufferResultSetToLocalTemp();
    }

}
