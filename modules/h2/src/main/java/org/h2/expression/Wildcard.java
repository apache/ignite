/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression;

import org.h2.api.ErrorCode;
import org.h2.engine.Session;
import org.h2.message.DbException;
import org.h2.table.ColumnResolver;
import org.h2.table.TableFilter;
import org.h2.util.StringUtils;
import org.h2.value.Value;

/**
 * A wildcard expression as in SELECT * FROM TEST.
 * This object is only used temporarily during the parsing phase, and later
 * replaced by column expressions.
 */
public class Wildcard extends Expression {
    private final String schema;
    private final String table;

    public Wildcard(String schema, String table) {
        this.schema = schema;
        this.table = table;
    }

    @Override
    public boolean isWildcard() {
        return true;
    }

    @Override
    public Value getValue(Session session) {
        throw DbException.throwInternalError(toString());
    }

    @Override
    public int getType() {
        throw DbException.throwInternalError(toString());
    }

    @Override
    public void mapColumns(ColumnResolver resolver, int level) {
        throw DbException.get(ErrorCode.SYNTAX_ERROR_1, table);
    }

    @Override
    public Expression optimize(Session session) {
        throw DbException.get(ErrorCode.SYNTAX_ERROR_1, table);
    }

    @Override
    public void setEvaluatable(TableFilter tableFilter, boolean b) {
        DbException.throwInternalError(toString());
    }

    @Override
    public int getScale() {
        throw DbException.throwInternalError(toString());
    }

    @Override
    public long getPrecision() {
        throw DbException.throwInternalError(toString());
    }

    @Override
    public int getDisplaySize() {
        throw DbException.throwInternalError(toString());
    }

    @Override
    public String getTableAlias() {
        return table;
    }

    @Override
    public String getSchemaName() {
        return schema;
    }

    @Override
    public String getSQL() {
        if (table == null) {
            return "*";
        }
        return StringUtils.quoteIdentifier(table) + ".*";
    }

    @Override
    public void updateAggregate(Session session) {
        DbException.throwInternalError(toString());
    }

    @Override
    public boolean isEverything(ExpressionVisitor visitor) {
        if (visitor.getType() == ExpressionVisitor.QUERY_COMPARABLE) {
            return true;
        }
        throw DbException.throwInternalError("" + visitor.getType());
    }

    @Override
    public int getCost() {
        throw DbException.throwInternalError(toString());
    }

}
