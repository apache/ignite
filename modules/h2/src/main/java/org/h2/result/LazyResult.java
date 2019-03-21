/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.result;

import org.h2.engine.SessionInterface;
import org.h2.expression.Expression;
import org.h2.message.DbException;
import org.h2.value.TypeInfo;
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

    /**
     * Go to the next row and skip it.
     *
     * @return true if a row exists
     */
    public boolean skip() {
        if (closed || afterLast) {
            return false;
        }
        currentRow = null;
        if (nextRow != null) {
            nextRow = null;
            return true;
        }
        if (skipNextRow()) {
            return true;
        }
        afterLast = true;
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

    /**
     * Skip next row.
     *
     * @return true if next row was available
     */
    protected boolean skipNextRow() {
        return fetchNextRow() != null;
    }

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
    public TypeInfo getColumnType(int i) {
        return expressions[i].getType();
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

}
