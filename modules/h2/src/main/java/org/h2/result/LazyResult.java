/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.result;

import org.h2.engine.SessionInterface;
import org.h2.expression.Expression;
import org.h2.message.DbException;
import org.h2.value.Value;

/**
 * Lazy execution support for queries.
 *
 * @author Sergi Vladykin
 */
public abstract class LazyResult implements ResultInterface {

    private final Expression[] expressions;
    private int rowId = -1;
    private Value[] currentRow;
    private Value[] nextRow;
    private boolean closed;
    private boolean afterLast;
    private int limit;

    public LazyResult(Expression[] expressions) {
        this.expressions = expressions;
    }

    public void setLimit(int limit) {
        this.limit = limit;
    }

    @Override
    public boolean isLazy() {
        return true;
    }

    @Override
    public void reset() {
        if (closed) {
            throw DbException.throwInternalError();
        }
        rowId = -1;
        afterLast = false;
        currentRow = null;
        nextRow = null;
    }

    @Override
    public Value[] currentRow() {
        return currentRow;
    }

    @Override
    public boolean next() {
        if (hasNext()) {
            rowId++;
            currentRow = nextRow;
            nextRow = null;
            return true;
        }
        if (!afterLast) {
            rowId++;
            currentRow = null;
            afterLast = true;
        }
        return false;
    }

    @Override
    public boolean hasNext() {
        if (closed || afterLast) {
            return false;
        }
        if (nextRow == null && (limit <= 0 || rowId + 1 < limit)) {
            nextRow = fetchNextRow();
        }
        return nextRow != null;
    }

    /**
     * Fetch next row or null if none available.
     *
     * @return next row or null
     */
    protected abstract Value[] fetchNextRow();

    @Override
    public boolean isAfterLast() {
        return afterLast;
    }

    @Override
    public int getRowId() {
        return rowId;
    }

    @Override
    public int getRowCount() {
        throw DbException.getUnsupportedException("Row count is unknown for lazy result.");
    }

    @Override
    public boolean needToClose() {
        return true;
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    @Override
    public void close() {
        closed = true;
    }

    @Override
    public String getAlias(int i) {
        return expressions[i].getAlias();
    }

    @Override
    public String getSchemaName(int i) {
        return expressions[i].getSchemaName();
    }

    @Override
    public String getTableName(int i) {
        return expressions[i].getTableName();
    }

    @Override
    public String getColumnName(int i) {
        return expressions[i].getColumnName();
    }

    @Override
    public int getColumnType(int i) {
        return expressions[i].getType();
    }

    @Override
    public long getColumnPrecision(int i) {
        return expressions[i].getPrecision();
    }

    @Override
    public int getColumnScale(int i) {
        return expressions[i].getScale();
    }

    @Override
    public int getDisplaySize(int i) {
        return expressions[i].getDisplaySize();
    }

    @Override
    public boolean isAutoIncrement(int i) {
        return expressions[i].isAutoIncrement();
    }

    @Override
    public int getNullable(int i) {
        return expressions[i].getNullable();
    }

    @Override
    public void setFetchSize(int fetchSize) {
        // ignore
    }

    @Override
    public int getFetchSize() {
        // We always fetch rows one by one.
        return 1;
    }

    @Override
    public ResultInterface createShallowCopy(SessionInterface targetSession) {
        // Copying is impossible with lazy result.
        return null;
    }

    @Override
    public boolean containsDistinct(Value[] values) {
        // We have to make sure that we do not allow lazy
        // evaluation when this call is needed:
        // WHERE x IN (SELECT ...).
        throw DbException.throwInternalError();
    }
}
