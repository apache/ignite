/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression;

import org.h2.command.Parser;
import org.h2.engine.Session;
import org.h2.table.ColumnResolver;
import org.h2.table.TableFilter;
import org.h2.value.Value;

/**
 * A column alias as in SELECT 'Hello' AS NAME ...
 */
public class Alias extends Expression {

    private final String alias;
    private Expression expr;
    private final boolean aliasColumnName;

    public Alias(Expression expression, String alias, boolean aliasColumnName) {
        this.expr = expression;
        this.alias = alias;
        this.aliasColumnName = aliasColumnName;
    }

    @Override
    public Expression getNonAliasExpression() {
        return expr;
    }

    @Override
    public Value getValue(Session session) {
        return expr.getValue(session);
    }

    @Override
    public int getType() {
        return expr.getType();
    }

    @Override
    public void mapColumns(ColumnResolver resolver, int level) {
        expr.mapColumns(resolver, level);
    }

    @Override
    public Expression optimize(Session session) {
        expr = expr.optimize(session);
        return this;
    }

    @Override
    public void setEvaluatable(TableFilter tableFilter, boolean b) {
        expr.setEvaluatable(tableFilter, b);
    }

    @Override
    public int getScale() {
        return expr.getScale();
    }

    @Override
    public long getPrecision() {
        return expr.getPrecision();
    }

    @Override
    public int getDisplaySize() {
        return expr.getDisplaySize();
    }

    @Override
    public boolean isAutoIncrement() {
        return expr.isAutoIncrement();
    }

    @Override
    public String getSQL() {
        return expr.getSQL() + " AS " + Parser.quoteIdentifier(alias);
    }

    @Override
    public void updateAggregate(Session session) {
        expr.updateAggregate(session);
    }

    @Override
    public String getAlias() {
        return alias;
    }

    @Override
    public int getNullable() {
        return expr.getNullable();
    }

    @Override
    public boolean isEverything(ExpressionVisitor visitor) {
        return expr.isEverything(visitor);
    }

    @Override
    public int getCost() {
        return expr.getCost();
    }

    @Override
    public String getTableName() {
        if (aliasColumnName) {
            return super.getTableName();
        }
        return expr.getTableName();
    }

    @Override
    public String getColumnName() {
        if (!(expr instanceof ExpressionColumn) || aliasColumnName) {
            return super.getColumnName();
        }
        return expr.getColumnName();
    }

}
