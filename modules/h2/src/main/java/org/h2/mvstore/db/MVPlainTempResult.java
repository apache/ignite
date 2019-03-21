/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore.db;

import org.h2.engine.Database;
import org.h2.expression.Expression;
import org.h2.message.DbException;
import org.h2.mvstore.Cursor;
import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVMap.Builder;
import org.h2.result.ResultExternal;
import org.h2.value.Value;
import org.h2.value.ValueRow;

/**
 * Plain temporary result.
 */
class MVPlainTempResult extends MVTempResult {

    /**
     * Map with identities of rows as keys rows as values.
     */
    private final MVMap<Long, ValueRow> map;

    /**
     * Counter for the identities of rows. A separate counter is used instead of
     * {@link #rowCount} because rows due to presence of {@link #removeRow(Value[])}
     * method to ensure that each row will have an own identity.
     */
    private long counter;

    /**
     * Cursor for the {@link #next()} method.
     */
    private Cursor<Long, ValueRow> cursor;

    /**
     * Creates a shallow copy of the result.
     *
     * @param parent
     *                   parent result
     */
    private MVPlainTempResult(MVPlainTempResult parent) {
        super(parent);
        this.map = parent.map;
    }

    /**
     * Creates a new plain temporary result.
     *
     * @param database
     *            database
     * @param expressions
     *            column expressions
     * @param visibleColumnCount
     *            count of visible columns
     */
    MVPlainTempResult(Database database, Expression[] expressions, int visibleColumnCount) {
        super(database, expressions, visibleColumnCount);
        ValueDataType valueType = new ValueDataType(database, new int[expressions.length]);
        Builder<Long, ValueRow> builder = new MVMap.Builder<Long, ValueRow>()
                                                .valueType(valueType).singleWriter();
        map = store.openMap("tmp", builder);
    }

    @Override
    public int addRow(Value[] values) {
        assert parent == null;
        map.append(counter++, ValueRow.get(values));
        return ++rowCount;
    }

    @Override
    public boolean contains(Value[] values) {
        throw DbException.getUnsupportedException("contains()");
    }

    @Override
    public synchronized ResultExternal createShallowCopy() {
        if (parent != null) {
            return parent.createShallowCopy();
        }
        if (closed) {
            return null;
        }
        childCount++;
        return new MVPlainTempResult(this);
    }

    @Override
    public Value[] next() {
        if (cursor == null) {
            cursor = map.cursor(null);
        }
        if (!cursor.hasNext()) {
            return null;
        }
        cursor.next();
        Value[] currentRow = cursor.getValue().getList();
        if (hasEnum) {
            fixEnum(currentRow);
        }
        return currentRow;
    }

    @Override
    public int removeRow(Value[] values) {
        throw DbException.getUnsupportedException("removeRow()");
    }

    @Override
    public void reset() {
        cursor = null;
    }

}
