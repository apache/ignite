/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression;

import java.util.ArrayList;
import org.h2.api.ErrorCode;
import org.h2.command.dml.Query;
import org.h2.engine.Session;
import org.h2.message.DbException;
import org.h2.result.ResultInterface;
import org.h2.table.ColumnResolver;
import org.h2.table.TableFilter;
import org.h2.value.Value;
import org.h2.value.ValueArray;
import org.h2.value.ValueNull;

/**
 * A query returning a single value.
 * Subqueries are used inside other statements.
 */
public class Subquery extends Expression {

    private final Query query;
    private Expression expression;

    public Subquery(Query query) {
        this.query = query;
    }

    @Override
    public Value getValue(Session session) {
        query.setSession(session);
        try (ResultInterface result = query.query(2)) {
            Value v;
            if (!result.next()) {
                v = ValueNull.INSTANCE;
            } else {
                Value[] values = result.currentRow();
                if (result.getVisibleColumnCount() == 1) {
                    v = values[0];
                } else {
                    v = ValueArray.get(values);
                }
                if (result.hasNext()) {
                    throw DbException.get(ErrorCode.SCALAR_SUBQUERY_CONTAINS_MORE_THAN_ONE_ROW);
                }
            }
            return v;
        }
    }

    @Override
    public int getType() {
        return getExpression().getType();
    }

    @Override
    public void mapColumns(ColumnResolver resolver, int level) {
        query.mapColumns(resolver, level + 1);
    }

    @Override
    public Expression optimize(Session session) {
        session.optimizeQueryExpression(query);
        return this;
    }

    @Override
    public void setEvaluatable(TableFilter tableFilter, boolean b) {
        query.setEvaluatable(tableFilter, b);
    }

    @Override
    public int getScale() {
        return getExpression().getScale();
    }

    @Override
    public long getPrecision() {
        return getExpression().getPrecision();
    }

    @Override
    public int getDisplaySize() {
        return getExpression().getDisplaySize();
    }

    @Override
    public String getSQL() {
        return "(" + query.getPlanSQL() + ")";
    }

    @Override
    public void updateAggregate(Session session) {
        query.updateAggregate(session);
    }

    private Expression getExpression() {
        if (expression == null) {
            ArrayList<Expression> expressions = query.getExpressions();
            int columnCount = query.getColumnCount();
            if (columnCount == 1) {
                expression = expressions.get(0);
            } else {
                Expression[] list = new Expression[columnCount];
                for (int i = 0; i < columnCount; i++) {
                    list[i] = expressions.get(i);
                }
                expression = new ExpressionList(list);
            }
        }
        return expression;
    }

    @Override
    public boolean isEverything(ExpressionVisitor visitor) {
        return query.isEverything(visitor);
    }

    public Query getQuery() {
        return query;
    }

    @Override
    public int getCost() {
        return query.getCostAsExpression();
    }

    @Override
    public Expression[] getExpressionColumns(Session session) {
        return getExpression().getExpressionColumns(session);
    }
}
